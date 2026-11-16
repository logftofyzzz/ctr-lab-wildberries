#!/usr/bin/env python3
"""Local Wildberries main-image rotation and advertising CTR tracker."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import mimetypes
import os
import socket
import struct
import sqlite3
import ssl
import subprocess
import threading
import time
import tempfile
import uuid
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from shutil import which
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from kie_imagegen import (
    VARIANTS,
    _upload_source_file,
    generate_variants as generate_kie_variants,
    get_credit_balance,
    regenerate_variant,
)
from tls_config import TLS_CONTEXT


ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"
DATA_DIR = Path(os.environ.get("CTR_LAB_DATA_DIR", str(ROOT / "data"))).expanduser().resolve()
DB_PATH = DATA_DIR / "experiments.sqlite3"
GENERATED_DIR = DATA_DIR / "generated"
ORIGINALS_DIR = DATA_DIR / "originals"
CREDENTIALS_PATH = DATA_DIR / "credentials.json"
TZ = ZoneInfo("Europe/Moscow")
CONTENT_BASE = "https://content-api.wildberries.ru"
ADS_BASE = "https://advert-api.wildberries.ru"
POLL_SECONDS = 10
MAX_IMAGE_BYTES = 32 * 1024 * 1024
EXPERIMENT_LOCK = threading.RLock()
GENERATION_LOCK = threading.Lock()
GENERATING_NM_IDS: set[int] = set()
CREDENTIALS_LOCK = threading.RLock()
PRODUCT_CATALOG_LOCK = threading.Lock()
PROMOTION_STATS_LOCK = threading.Lock()
PROMOTION_STATS_LAST_REQUEST = 0.0
CREDENTIALS: dict[str, str] = {}
PRODUCT_CATALOG_CACHE: list[dict] | None = None
PRODUCT_CATALOG_CACHE_AT = 0.0
PRODUCT_CATALOG_TTL_SECONDS = 300


class PhotoPropagationPending(RuntimeError):
    """WB accepted an uploaded image but has not exposed it on the card yet."""


def get_token(name: str) -> str:
    aliases = {
        "wb_content_token": "WB_CONTENT_TOKEN",
        "wb_promotion_token": "WB_PROMOTION_TOKEN",
        "kie_api_key": "KIE_API_KEY",
    }
    with CREDENTIALS_LOCK:
        configured = CREDENTIALS.get(name, "")
    if configured:
        return configured
    if name == "wb_promotion_token":
        return os.environ.get("WB_PROMOTION_TOKEN", "").strip() or os.environ.get("WB_ADS_TOKEN", "").strip()
    return os.environ.get(aliases.get(name, ""), "").strip()


def load_credentials() -> None:
    global CREDENTIALS
    try:
        values = json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
        if isinstance(values, dict):
            CREDENTIALS = {key: str(value) for key, value in values.items() if value}
    except (OSError, ValueError):
        CREDENTIALS = {}


def credential_status() -> dict:
    return {
        "content_connected": bool(get_token("wb_content_token")),
        "promotion_connected": bool(get_token("wb_promotion_token")),
        "kie_connected": bool(get_token("kie_api_key")),
    }


def save_credentials(values: dict) -> dict:
    global PRODUCT_CATALOG_CACHE, PRODUCT_CATALOG_CACHE_AT
    allowed = {"wb_content_token", "wb_promotion_token", "openai_api_key", "kie_api_key"}
    clear = set(values.get("clear", [])) & allowed
    with CREDENTIALS_LOCK:
        for key in clear:
            CREDENTIALS.pop(key, None)
        for key in allowed:
            value = str(values.get(key, "")).strip()
            if value:
                CREDENTIALS[key] = value
        if CREDENTIALS:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(DATA_DIR, 0o700)
            except OSError:
                pass
            temp_path = CREDENTIALS_PATH.with_suffix(".tmp")
            temp_path.write_text(json.dumps(CREDENTIALS), encoding="utf-8")
            try:
                os.chmod(temp_path, 0o600)
            except OSError:
                pass
            temp_path.replace(CREDENTIALS_PATH)
        else:
            CREDENTIALS_PATH.unlink(missing_ok=True)
    with PRODUCT_CATALOG_LOCK:
        PRODUCT_CATALOG_CACHE = None
        PRODUCT_CATALOG_CACHE_AT = 0.0
    return credential_status()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nm_id INTEGER NOT NULL UNIQUE,
                title TEXT NOT NULL,
                photos_json TEXT NOT NULL,
                cover_path TEXT,
                cover_state TEXT NOT NULL DEFAULT 'missing',
                cover_source_url TEXT,
                cover_hash TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS experiments (
                id TEXT PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id),
                advert_id INTEGER NOT NULL,
                duration_days INTEGER NOT NULL,
                target_views INTEGER NOT NULL DEFAULT 1000,
                original_photos_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL,
                scheduled_start TEXT,
                current_variant_index INTEGER NOT NULL DEFAULT 0,
                current_stage_id INTEGER,
                error TEXT,
                next_photo_number INTEGER NOT NULL DEFAULT 0,
                initial_baseline_views INTEGER NOT NULL DEFAULT 0,
                initial_baseline_clicks INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS variants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                label TEXT NOT NULL,
                image_url TEXT NOT NULL,
                local_path TEXT,
                UNIQUE(experiment_id, position)
            );
            CREATE TABLE IF NOT EXISTS pending_uploads (
                experiment_id TEXT PRIMARY KEY REFERENCES experiments(id) ON DELETE CASCADE,
                variant_index INTEGER NOT NULL,
                photo_number INTEGER NOT NULL,
                existing_photos_json TEXT NOT NULL,
                upload_sent INTEGER NOT NULL DEFAULT 0,
                target_url TEXT,
                save_sent INTEGER NOT NULL DEFAULT 0,
                upload_attempts INTEGER NOT NULL DEFAULT 0,
                last_upload_at TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS stages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
                variant_index INTEGER NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                switch_at TEXT NOT NULL,
                collect_after TEXT,
                views INTEGER,
                clicks INTEGER,
                ctr REAL,
                progress_views INTEGER NOT NULL DEFAULT 0,
                progress_clicks INTEGER NOT NULL DEFAULT 0,
                baseline_views INTEGER NOT NULL DEFAULT 0,
                baseline_clicks INTEGER NOT NULL DEFAULT 0,
                baseline_ready INTEGER NOT NULL DEFAULT 1,
                last_checked_date TEXT,
                progress_error TEXT,
                target_reached INTEGER NOT NULL DEFAULT 0,
                collection_state TEXT NOT NULL DEFAULT 'pending',
                collection_error TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS stages_experiment_idx ON stages(experiment_id, variant_index);
            CREATE INDEX IF NOT EXISTS stages_collection_idx ON stages(collection_state, collect_after);
            CREATE TABLE IF NOT EXISTS generated_images (
                id TEXT PRIMARY KEY,
                nm_id INTEGER NOT NULL,
                filename TEXT NOT NULL UNIQUE,
                local_path TEXT NOT NULL,
                label TEXT NOT NULL,
                prompt TEXT NOT NULL,
                cover_hash TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        migrations = {
            "products": {
                "cover_path": "TEXT",
                "cover_state": "TEXT NOT NULL DEFAULT 'missing'",
                "cover_source_url": "TEXT",
                "cover_hash": "TEXT",
            },
            "experiments": {
                "original_photos_json": "TEXT NOT NULL DEFAULT '[]'",
                "target_views": "INTEGER NOT NULL DEFAULT 1000",
                "selected_variant_index": "INTEGER",
                "next_photo_number": "INTEGER NOT NULL DEFAULT 0",
            "initial_baseline_views": "INTEGER NOT NULL DEFAULT 0",
            "initial_baseline_clicks": "INTEGER NOT NULL DEFAULT 0",
            },
            "variants": {"local_path": "TEXT"},
            "generated_images": {"cover_hash": "TEXT"},
            "pending_uploads": {
                "target_url": "TEXT",
                "save_sent": "INTEGER NOT NULL DEFAULT 0",
                "upload_attempts": "INTEGER NOT NULL DEFAULT 0",
                "last_upload_at": "TEXT",
                "previous_cover_hash": "TEXT",
                "direct_replace_sent": "INTEGER NOT NULL DEFAULT 0",
                "direct_replace_attempts": "INTEGER NOT NULL DEFAULT 0",
                "url_save_attempts": "INTEGER NOT NULL DEFAULT 0",
                "check_failures": "INTEGER NOT NULL DEFAULT 0",
                "next_check_at": "TEXT",
            },
            "stages": {
                "progress_views": "INTEGER NOT NULL DEFAULT 0",
                "progress_clicks": "INTEGER NOT NULL DEFAULT 0",
                "baseline_views": "INTEGER NOT NULL DEFAULT 0",
                "baseline_clicks": "INTEGER NOT NULL DEFAULT 0",
                # Existing stages keep their recorded baseline. New stages are
                # explicitly created unready and establish it after WB confirms
                # the image currently being measured.
                "baseline_ready": "INTEGER NOT NULL DEFAULT 1",
                "last_checked_date": "TEXT",
                "last_checked_at": "TEXT",
                "progress_error": "TEXT",
                "target_reached": "INTEGER NOT NULL DEFAULT 0",
            },
        }
        for table, additions in migrations.items():
            columns = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
            for column, definition in additions.items():
                if column not in columns:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        db.execute(
            """UPDATE pending_uploads
               SET upload_attempts=1, last_upload_at=COALESCE(last_upload_at, created_at)
               WHERE upload_sent=1 AND upload_attempts=0"""
        )


def recover_accepted_upload_failures() -> None:
    """Resume transitions that the previous version failed while waiting for WB."""
    with connect_db() as db:
        failed = db.execute(
            """SELECT e.id, e.current_variant_index, e.current_stage_id, e.original_photos_json,
                      e.next_photo_number, p.photos_json, p.nm_id, pu.variant_index AS pending_variant_index
               FROM experiments e JOIN products p ON p.id=e.product_id
               JOIN pending_uploads pu ON pu.experiment_id=e.id
               WHERE e.status='error' AND pu.upload_sent=1 AND (
                   e.error LIKE 'WB принял файл, но карточка ещё не вернула его ссылку.%'
                   OR e.error='Wildberries ещё обрабатывает загруженное фото.'
                   OR e.error='WB принял порядок фото, но ещё не подтвердил главное изображение.'
                   OR e.error LIKE 'Wildberries вернул HTTP 408%'
                   OR e.error LIKE 'Wildberries вернул HTTP 425%'
                   OR e.error LIKE 'Wildberries вернул HTTP 429%'
                   OR e.error LIKE 'Wildberries вернул HTTP 5%'
                   OR e.error LIKE 'Не удалось связаться с Wildberries:%'
                   OR e.error LIKE '%Connection reset%'
                   OR e.error LIKE '%Connection aborted%'
                   OR e.error LIKE '%timed out%'
                   OR e.error LIKE '%timeout%'
               )"""
        ).fetchall()
        for experiment in failed:
            if (
                experiment["current_stage_id"] is None
                and int(experiment["current_variant_index"]) == 0
                and int(experiment["pending_variant_index"]) == 0
            ):
                db.execute(
                    "UPDATE experiments SET status='scheduled', scheduled_start=?, error=NULL, updated_at=? WHERE id=?",
                    (stamp(), stamp(), experiment["id"]),
                )
                db.execute(
                    "UPDATE pending_uploads SET next_check_at=NULL, check_failures=0 WHERE experiment_id=?",
                    (experiment["id"],),
                )
                log_event(
                    db,
                    experiment["id"],
                    "info",
                    "Возобновляю запуск: WB уже принял контрольное фото. Жду публикации без повторной загрузки.",
                )
                continue
            stage = db.execute(
                "SELECT target_reached FROM stages WHERE id=? AND experiment_id=?",
                (experiment["current_stage_id"], experiment["id"]),
            ).fetchone()
            next_index = int(experiment["current_variant_index"]) + 1
            variant = db.execute(
                "SELECT local_path FROM variants WHERE experiment_id=? AND position=?",
                (experiment["id"], next_index),
            ).fetchone()
            if not stage or not stage["target_reached"] or not variant or not variant["local_path"]:
                continue
            original_photos = json.loads(experiment["original_photos_json"] or experiment["photos_json"] or "[]")
            if not original_photos:
                continue
            photo_number = int(experiment["next_photo_number"] or (len(original_photos) + 1))
            db.execute(
                """INSERT OR IGNORE INTO pending_uploads(
                       experiment_id, variant_index, photo_number, existing_photos_json, upload_sent, created_at
                   ) VALUES (?, ?, ?, ?, 1, ?)""",
                (experiment["id"], next_index, photo_number, json.dumps(original_photos), stamp()),
            )
            db.execute(
                "UPDATE experiments SET next_photo_number=MAX(next_photo_number, ?) WHERE id=?",
                (photo_number + 1, experiment["id"]),
            )
            db.execute(
                "UPDATE experiments SET status='active', error=NULL, updated_at=? WHERE id=?",
                (stamp(), experiment["id"]),
            )
            db.execute(
                "UPDATE pending_uploads SET next_check_at=NULL, check_failures=0 WHERE experiment_id=?",
                (experiment["id"],),
            )
            log_event(
                db,
                experiment["id"],
                "info",
                "Возобновляю тест: WB принял фото. Жду публикации изображения в карточке.",
            )


def utc_now() -> datetime:
    return datetime.now().astimezone()


def stamp(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="seconds")


def public_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        if exc.code >= 500:
            return f"Сервер WB временно недоступен (HTTP {exc.code})"
        if exc.code == 429:
            return "WB ограничил частоту запросов (HTTP 429)"
        details = exc.read().decode("utf-8", "replace")[:300]
        if details.lstrip().startswith("<"):
            details = ""
        return f"WB вернул HTTP {exc.code}: {details or exc.reason}"
    if isinstance(exc, URLError):
        if is_certificate_verification_error(exc):
            return "Не удалось проверить TLS-сертификат WB. Проверьте системные сертификаты."
        reason = str(exc.reason)
        if "handshake operation timed out" in reason.lower():
            return "Истёк таймаут TLS-соединения с WB"
        if "timed out" in reason.lower():
            return "Истёк таймаут ответа WB"
        if "connection reset" in reason.lower():
            return "WB оборвал соединение"
        return f"Не удалось связаться с Wildberries: {exc.reason}"
    if isinstance(exc, ConnectionError):
        return "WB оборвал соединение"
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "Истёк таймаут ответа WB"
    return str(exc)


def summarize_wb_events(rows: list[sqlite3.Row]) -> list[dict]:
    """Collapse recurring transient upload failures into one readable dashboard note."""
    categories: dict[str, int] = {}
    retry_rows: list[sqlite3.Row] = []
    visible: list[dict] = []
    for row in rows:
        message = str(row["message"] or "")
        lowered = message.lower()
        is_network_failure = any(
            marker in lowered for marker in (
                "connection reset", "connection aborted", "handshake operation timed out",
                "read operation timed out", "timed out", "timeout", "http 429", "http 5",
            )
        )
        is_transient_photo_event = is_network_failure or (
            row["level"] == "warning"
            and "фото" in lowered
            and ("wb" in lowered or "wildberries" in lowered)
        )
        if not is_transient_photo_event:
            visible.append(dict(row))
            continue

        retry_rows.append(row)
        if "503" in lowered or "http 5" in lowered or "сервер wb временно недоступен" in lowered:
            category = "ответы WB 5xx"
        elif "429" in lowered or "лимит" in lowered:
            category = "ограничение частоты запросов"
        elif "tls" in lowered or "ssl" in lowered or "handshake" in lowered:
            category = "таймауты TLS"
        elif "timeout" in lowered or "timed out" in lowered or "таймаут" in lowered:
            category = "таймауты ответа"
        elif "connection reset" in lowered or "connection aborted" in lowered or "оборвал" in lowered:
            category = "обрывы соединения"
        else:
            category = "временные сетевые ошибки"
        categories[category] = categories.get(category, 0) + 1

    if retry_rows:
        category_text = ", ".join(
            f"{name} — {count}" for name, count in sorted(categories.items(), key=lambda item: item[0])
        )
        visible.insert(0, {
            "level": "warning",
            "message": (
                f"Сводка связи с WB: {len(retry_rows)} временных сбоев ({category_text}). "
                "Повторные запросы идут с паузами; CTR не собирается до подтверждения фото."
            ),
            "created_at": retry_rows[0]["created_at"],
        })
    return visible[:8]


def is_certificate_verification_error(exc: Exception) -> bool:
    reason = exc.reason if isinstance(exc, URLError) else exc
    return isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(reason)


def is_retryable_wb_failure(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in (408, 425, 429) or 500 <= exc.code < 600
    if is_certificate_verification_error(exc):
        return False
    return isinstance(exc, (URLError, TimeoutError, socket.timeout, ConnectionError))


def has_pending_wb_upload(experiment_id: str) -> bool:
    with connect_db() as db:
        row = db.execute(
            "SELECT 1 FROM pending_uploads WHERE experiment_id=? LIMIT 1",
            (experiment_id,),
        ).fetchone()
    return row is not None


def pending_upload_check_due(experiment_id: str, now: datetime | None = None) -> bool:
    with connect_db() as db:
        row = db.execute(
            "SELECT next_check_at FROM pending_uploads WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
    if not row or not row["next_check_at"]:
        return True
    try:
        return datetime.fromisoformat(row["next_check_at"]) <= (now or datetime.now().astimezone())
    except (TypeError, ValueError):
        return True


def defer_pending_upload_check(experiment_id: str) -> None:
    """Back off WB status checks after an accepted photo or a transient network error."""
    with connect_db() as db:
        row = db.execute(
            "SELECT check_failures FROM pending_uploads WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
        if not row:
            return
        failures = int(row["check_failures"] or 0) + 1
        # Status checks are read-only; keep a short backoff so a delayed WB
        # publication is noticed promptly without re-uploading the image.
        seconds = min(120, 15 * (2 ** min(failures - 1, 3)))
        retry_at = stamp(datetime.now().astimezone() + timedelta(seconds=seconds))
        db.execute(
            "UPDATE pending_uploads SET check_failures=?, next_check_at=? WHERE experiment_id=?",
            (failures, retry_at, experiment_id),
        )


def log_wb_retry_once(experiment_id: str, exc: Exception) -> None:
    with connect_db() as db:
        pending = db.execute(
            "SELECT upload_sent FROM pending_uploads WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
        if not pending:
            return
        if int(pending["upload_sent"]):
            suffix = "Проверю позже; уже принятое фото повторно не загружаю."
        else:
            suffix = "Повторю операцию после паузы."
        category = public_error(exc)
        message = f"Сбой WB: {category}. {suffix}"
        already_logged = db.execute(
            "SELECT 1 FROM events WHERE experiment_id=? AND message=? LIMIT 1",
            (experiment_id, message),
        ).fetchone()
        if not already_logged:
            log_event(db, experiment_id, "warning", message)


def log_pending_upload_delay_once(experiment_id: str) -> None:
    """Tell the seller when WB has kept an accepted photo unavailable for 15 min."""
    with connect_db() as db:
        pending = db.execute(
            "SELECT upload_sent, url_save_attempts, created_at, last_upload_at FROM pending_uploads WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
        if not pending or not int(pending["upload_sent"]):
            return
        try:
            anchor = pending["last_upload_at"] or pending["created_at"]
            age = datetime.now().astimezone() - datetime.fromisoformat(anchor)
        except (TypeError, ValueError):
            return
        if age < timedelta(minutes=15):
            return
        if int(pending["url_save_attempts"] or 0) >= 2:
            message = (
                "WB ещё не применил фото после двух передач файла. Тест не собирает показы; "
                "проверку продолжу с увеличенным интервалом."
            )
        else:
            message = (
                "WB не подтвердил фото. Тест пока не собирает показы; повторную передачу того же файла "
                "выполню после паузы с учётом лимита WB."
            )
        already_logged = db.execute(
            "SELECT 1 FROM events WHERE experiment_id=? AND message=? LIMIT 1",
            (experiment_id, message),
        ).fetchone()
        if not already_logged:
            log_event(db, experiment_id, "warning", message)


def wb_request(base: str, path: str, token: str, payload: dict) -> dict:
    request = Request(
        base + path,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "CTR-Lab-WB/0.1",
        },
        method="POST",
    )
    with urlopen(request, timeout=40, context=TLS_CONTEXT) as response:
        body = response.read()
    return json.loads(body.decode("utf-8")) if body else {}


def wb_get(base: str, path: str, token: str, params: dict) -> dict:
    request = Request(
        base + path + "?" + urlencode(params),
        headers={"Authorization": token, "Accept": "application/json", "User-Agent": "CTR-Lab-WB/0.2"},
        method="GET",
    )
    with urlopen(request, timeout=40, context=TLS_CONTEXT) as response:
        body = response.read()
    return json.loads(body.decode("utf-8")) if body else {}


def wb_upload_image(nm_id: int, photo_number: int, image_path: str) -> dict:
    token = get_token("wb_content_token")
    if not token:
        raise RuntimeError("Сохраните токен WB с доступом к Контенту.")
    path = Path(image_path)
    image_data = path.read_bytes()
    mime = mimetypes.guess_type(path.name)[0] or "image/png"
    boundary = "----CTRLabWB" + uuid.uuid4().hex
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"uploadfile\"; filename=\"{path.name}\"\r\n"
        f"Content-Type: {mime}\r\n\r\n"
    ).encode("utf-8") + image_data + f"\r\n--{boundary}--\r\n".encode("ascii")
    request = Request(
        CONTENT_BASE + "/content/v3/media/file",
        data=body,
        headers={
            "Authorization": token,
            "X-Nm-Id": str(nm_id),
            "X-Photo-Number": str(photo_number),
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
            "User-Agent": "CTR-Lab-WB/0.2",
        },
        method="POST",
    )
    with urlopen(request, timeout=60, context=TLS_CONTEXT) as response:
        result = json.loads(response.read().decode("utf-8"))
    if result.get("error"):
        raise RuntimeError(result.get("errorText") or "WB не принял сгенерированное фото.")
    return result


def fetch_card(nm_id: int) -> dict:
    token = get_token("wb_content_token")
    if not token:
        raise RuntimeError("Сохраните токен WB с доступом к Контенту.")
    cursor = {"limit": 100}
    seen: set[tuple[str, int]] = set()
    while True:
        payload = {"settings": {"cursor": cursor, "filter": {"withPhoto": -1}}}
        result = wb_request(CONTENT_BASE, "/content/v2/get/cards/list", token, payload)
        for card in result.get("cards", []):
            if int(card.get("nmID", -1)) == nm_id:
                photos = [
                    photo.get("big") or photo.get("c516x688") or photo.get("c246x328")
                    for photo in card.get("photos", [])
                ]
                photos = [url for url in photos if isinstance(url, str) and url.startswith("http")]
                if not photos:
                    raise RuntimeError("У карточки нет доступных фотографий в ответе API.")
                return {"nm_id": nm_id, "title": card.get("title") or f"Товар {nm_id}", "photos": photos}

        page_cursor = result.get("cursor") or {}
        total = int(page_cursor.get("total", 0) or 0)
        updated_at = page_cursor.get("updatedAt")
        next_nm_id = page_cursor.get("nmID")
        if not result.get("cards") or total < int(cursor.get("limit", 100)) or updated_at is None or next_nm_id is None:
            break
        marker = (str(updated_at), int(next_nm_id))
        if marker in seen:
            break
        seen.add(marker)
        cursor = {"limit": 100, "updatedAt": updated_at, "nmID": int(next_nm_id)}
        time.sleep(0.65)
    raise RuntimeError(f"Не нашёл карточку Wildberries с nmID {nm_id}.")


def fetch_product_catalog(force_refresh: bool = False) -> list[dict]:
    """Load the seller's product cards from WB Content API with short local caching."""
    global PRODUCT_CATALOG_CACHE, PRODUCT_CATALOG_CACHE_AT
    token = get_token("wb_content_token")
    if not token:
        raise RuntimeError("Сохраните токен WB с доступом к Контенту, чтобы загрузить ваши карточки.")
    with PRODUCT_CATALOG_LOCK:
        now = time.monotonic()
        if (
            not force_refresh
            and PRODUCT_CATALOG_CACHE is not None
            and now - PRODUCT_CATALOG_CACHE_AT < PRODUCT_CATALOG_TTL_SECONDS
        ):
            return PRODUCT_CATALOG_CACHE

        cursor: dict = {"limit": 100}
        seen: set[tuple[str, int]] = set()
        products: list[dict] = []
        while True:
            payload = {"settings": {"cursor": cursor, "filter": {"withPhoto": -1}}}
            result = wb_request(CONTENT_BASE, "/content/v2/get/cards/list", token, payload)
            cards = result.get("cards") or []
            for card in cards:
                try:
                    nm_id = int(card.get("nmID", 0))
                except (TypeError, ValueError):
                    continue
                if nm_id <= 0:
                    continue
                photos = card.get("photos") or []
                first_photo = next(
                    (
                        photo.get("c516x688") or photo.get("big") or photo.get("c246x328")
                        for photo in photos
                        if isinstance(photo, dict)
                        and any(photo.get(key) for key in ("c516x688", "big", "c246x328"))
                    ),
                    None,
                )
                products.append({
                    "nm_id": nm_id,
                    "title": str(card.get("title") or f"Товар {nm_id}"),
                    "vendor_code": str(card.get("vendorCode") or ""),
                    "brand": str(card.get("brand") or ""),
                    "subject": str(card.get("subjectName") or ""),
                    "photo_url": first_photo if isinstance(first_photo, str) else None,
                })

            page_cursor = result.get("cursor") or {}
            total = int(page_cursor.get("total", 0) or 0)
            updated_at = page_cursor.get("updatedAt")
            next_nm_id = page_cursor.get("nmID")
            if not cards or total < int(cursor.get("limit", 100)) or updated_at is None or next_nm_id is None:
                break
            marker = (str(updated_at), int(next_nm_id))
            if marker in seen:
                break
            seen.add(marker)
            cursor = {"limit": 100, "updatedAt": updated_at, "nmID": int(next_nm_id)}
            time.sleep(0.65)

        products.sort(key=lambda product: (product["title"].casefold(), product["nm_id"]))
        PRODUCT_CATALOG_CACHE = products
        PRODUCT_CATALOG_CACHE_AT = time.monotonic()
        return products


def download_wb_image(source_url: str) -> tuple[bytes, str]:
    parsed = urlparse(source_url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not (host == "wbbasket.ru" or host.endswith(".wbbasket.ru")):
        raise RuntimeError("Wildberries вернул неожиданный адрес главного фото.")
    request = Request(source_url, headers={"Accept": "image/*", "User-Agent": "CTR-Lab-WB/0.3"})
    try:
        with urlopen(request, timeout=45, context=TLS_CONTEXT) as response:
            content_type = response.headers.get_content_type()
            image_data = response.read(MAX_IMAGE_BYTES + 1)
    except (HTTPError, URLError) as exc:
        raise RuntimeError("Не удалось сохранить копию главного фото с Wildberries.") from exc
    if not content_type.startswith("image/") or len(image_data) < 16 or len(image_data) > MAX_IMAGE_BYTES:
        raise RuntimeError("Wildberries вернул файл фото в неожиданном формате или размере.")
    extension = Path(parsed.path).suffix.lower()
    if extension not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
        extension = mimetypes.guess_extension(content_type) or ".img"
    return image_data, extension


def perceptual_image_hash(image_data: bytes, extension: str = ".img") -> int | None:
    """Build a 32x32 grayscale average hash, normalizing WB/Kie re-encoding."""
    sips = which("sips")
    if sips:
        suffix = extension.lower() if extension.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"} else ".img"
        try:
            with tempfile.TemporaryDirectory(prefix="ctr-image-hash-") as temp_dir:
                source = Path(temp_dir) / f"source{suffix}"
                bitmap = Path(temp_dir) / "small.bmp"
                source.write_bytes(image_data)
                subprocess.run(
                    [sips, "-s", "format", "bmp", "-z", "32", "32", str(source), "--out", str(bitmap)],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=15,
                )
                bmp = bitmap.read_bytes()
            if bmp[:2] != b"BM":
                return None
            pixel_offset = struct.unpack_from("<I", bmp, 10)[0]
            width, signed_height = struct.unpack_from("<ii", bmp, 18)
            bits_per_pixel = struct.unpack_from("<H", bmp, 28)[0]
            compression = struct.unpack_from("<I", bmp, 30)[0]
            if width != 32 or abs(signed_height) != 32 or bits_per_pixel not in (24, 32):
                return None
            if compression not in (0, 3):
                return None
            masks = None
            if bits_per_pixel == 32 and compression == 3:
                masks = struct.unpack_from("<IIII", bmp, 54)
            stride = ((width * bits_per_pixel + 31) // 32) * 4

            def channel(pixel: int, mask: int, default: int) -> int:
                if not mask:
                    return default
                shift = (mask & -mask).bit_length() - 1
                maximum = mask >> shift
                return ((pixel & mask) >> shift) * 255 // maximum

            luminance: list[int] = []
            for y in range(32):
                source_y = y if signed_height < 0 else 31 - y
                row = bmp[pixel_offset + source_y * stride : pixel_offset + (source_y + 1) * stride]
                for x in range(32):
                    start = x * (bits_per_pixel // 8)
                    pixel = row[start : start + bits_per_pixel // 8]
                    if bits_per_pixel == 24:
                        blue, green, red = pixel
                        alpha = 255
                    elif masks:
                        packed = int.from_bytes(pixel, "little")
                        red = channel(packed, masks[0], 0)
                        green = channel(packed, masks[1], 0)
                        blue = channel(packed, masks[2], 0)
                        alpha = channel(packed, masks[3], 255)
                    else:
                        blue, green, red, alpha = pixel
                    red = (red * alpha + 255 * (255 - alpha)) // 255
                    green = (green * alpha + 255 * (255 - alpha)) // 255
                    blue = (blue * alpha + 255 * (255 - alpha)) // 255
                    luminance.append((299 * red + 587 * green + 114 * blue) // 1000)
            average = sum(luminance) / len(luminance)
            return sum((value > average) << index for index, value in enumerate(luminance))
        except (OSError, ValueError, struct.error, subprocess.SubprocessError):
            return None

    # Pillow is optional for non-macOS installations; without either decoder we
    # keep the conservative byte-for-byte comparison in images_are_similar().
    try:
        from io import BytesIO
        from PIL import Image

        with Image.open(BytesIO(image_data)) as image:
            pixels = list(image.convert("RGB").resize((32, 32)).convert("L").getdata())
        average = sum(pixels) / len(pixels)
        return sum((value > average) << index for index, value in enumerate(pixels))
    except (ImportError, OSError, ValueError):
        return None


def images_are_similar(
    left: bytes,
    right: bytes,
    left_ext: str = ".img",
    right_ext: str = ".img",
    alternatives: list[tuple[bytes, str]] | None = None,
) -> bool:
    """Match artwork after WB resizing/re-encoding, disambiguating other variants."""
    if hashlib.sha256(left).digest() == hashlib.sha256(right).digest():
        return True
    left_hash = perceptual_image_hash(left, left_ext)
    right_hash = perceptual_image_hash(right, right_ext)
    if left_hash is None or right_hash is None:
        return False
    distance = (left_hash ^ right_hash).bit_count()
    if distance <= 64:
        return True

    # WB can alter framing and rendering enough to move an average hash farther
    # than a byte-preserving resize. A wider match is safe only when the expected
    # image is clearly closer than every other variant in this experiment.
    if distance > 180 or not alternatives:
        return False
    alternative_distances = []
    for candidate, candidate_ext in alternatives:
        candidate_hash = perceptual_image_hash(candidate, candidate_ext)
        if candidate_hash is not None:
            alternative_distances.append((left_hash ^ candidate_hash).bit_count())
    return bool(alternative_distances) and distance + 32 <= min(alternative_distances)


def save_cover_copy(nm_id: int, source_url: str, state: str) -> dict:
    image_data, extension = download_wb_image(source_url)
    ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{nm_id}-{uuid.uuid4().hex}{extension}"
    image_path = ORIGINALS_DIR / filename
    temporary_path = image_path.with_suffix(image_path.suffix + ".tmp")
    temporary_path.write_bytes(image_data)
    try:
        os.chmod(temporary_path, 0o600)
    except OSError:
        pass
    temporary_path.replace(image_path)
    try:
        os.chmod(image_path, 0o600)
    except OSError:
        pass
    return {
        "path": str(image_path.resolve()),
        "filename": filename,
        "image_url": f"/originals/{filename}",
        "state": state,
        "cover_hash": hashlib.sha256(image_data).hexdigest(),
    }


def local_cover_path(value: str | None) -> Path | None:
    if not value:
        return None
    path = Path(value).resolve()
    if ORIGINALS_DIR.resolve() not in path.parents or not path.is_file():
        return None
    return path


def ensure_product_cover(nm_id: int, card: dict, accept_current: bool = False) -> dict:
    with connect_db() as db:
        product = db.execute("SELECT * FROM products WHERE nm_id=?", (nm_id,)).fetchone()
        history = db.execute(
            "SELECT COUNT(*) FROM experiments e JOIN products p ON p.id=e.product_id WHERE p.nm_id=?",
            (nm_id,),
        ).fetchone()[0]
    if product:
        saved_path = local_cover_path(product["cover_path"])
        if saved_path and product["cover_state"] in ("captured", "confirmed_current", "rebased_current"):
            with connect_db() as db:
                db.execute(
                    "UPDATE products SET title=?, photos_json=? WHERE nm_id=?",
                    (card["title"], json.dumps(card["photos"]), nm_id),
                )
            return {
                "available": True,
                "path": str(saved_path),
                "image_url": f"/originals/{saved_path.name}",
                "state": product["cover_state"],
                "cover_hash": product["cover_hash"],
            }
        if history and not accept_current:
            with connect_db() as db:
                db.execute(
                    "UPDATE products SET title=?, photos_json=?, cover_state='missing' WHERE nm_id=?",
                    (card["title"], json.dumps(card["photos"]), nm_id),
                )
            return {"available": False, "path": None, "image_url": None, "state": "missing"}

    state = "rebased_current" if accept_current else "captured"
    saved = save_cover_copy(nm_id, card["photos"][0], state)
    with connect_db() as db:
        product = db.execute("SELECT id FROM products WHERE nm_id=?", (nm_id,)).fetchone()
        if product:
            db.execute(
                """UPDATE products SET title=?, photos_json=?, cover_path=?, cover_state=?, cover_source_url=?, cover_hash=?
                   WHERE nm_id=?""",
                (card["title"], json.dumps(card["photos"]), saved["path"], state, card["photos"][0], saved["cover_hash"], nm_id),
            )
        else:
            db.execute(
                """INSERT INTO products(nm_id, title, photos_json, cover_path, cover_state, cover_source_url, cover_hash, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (nm_id, card["title"], json.dumps(card["photos"]), saved["path"], state, card["photos"][0], saved["cover_hash"], stamp()),
            )
    return {
        "available": True,
        "path": saved["path"],
        "image_url": saved["image_url"],
        "state": state,
        "cover_hash": saved["cover_hash"],
    }


def fetch_active_campaigns(nm_id: int) -> list[dict]:
    token = get_token("wb_promotion_token")
    if not token:
        raise RuntimeError("Сохраните токен WB с доступом к Продвижению.")
    cursor = ""
    seen: set[str] = set()
    matches: list[dict] = []
    while True:
        params = {"statuses": "9", "limit": 50}
        if cursor:
            params["next"] = cursor
        payload = wb_get(ADS_BASE, "/api/advert/v3/adverts", token, params)
        for advert in payload.get("adverts", []):
            nms = advert.get("nms") or []
            if any(int(item.get("nmId", -1)) == nm_id for item in nms):
                settings = advert.get("settings") or {}
                matches.append({
                    "advert_id": int(advert.get("advertId", 0)),
                    "name": settings.get("name") or f"Кампания {advert.get('advertId')}",
                    "payment_type": str(settings.get("paymentType") or "").lower(),
                    "status": int(advert.get("status", 0)),
                })
        next_cursor = payload.get("next") or ""
        if not next_cursor or next_cursor in seen:
            break
        seen.add(next_cursor)
        cursor = next_cursor
    return matches


def uploaded_photo_url(existing_photos: list[str], photo_number: int) -> str | None:
    """Build the CDN URL for a photo slot from a known URL of the same WB card."""
    for source_url in existing_photos:
        parsed = urlparse(source_url)
        host = (parsed.hostname or "").lower()
        if not (host == "wbbasket.ru" or host.endswith(".wbbasket.ru")):
            continue
        if "/images/big/" not in parsed.path:
            continue
        parent, filename = parsed.path.rsplit("/", 1)
        stem, dot, extension = filename.partition(".")
        if not dot or not stem.isdigit() or not extension:
            continue
        return urlunparse(parsed._replace(path=f"{parent}/{photo_number}.{extension}"))
    return None


def wb_photo_url_is_ready(photo_url: str) -> bool:
    """Check whether WB's CDN has finished exposing an uploaded photo slot."""
    request = Request(
        photo_url,
        headers={
            "Accept": "image/*",
            "Cache-Control": "no-cache",
            "User-Agent": "CTR-Lab-WB/0.4",
        },
    )
    try:
        with urlopen(request, timeout=15, context=TLS_CONTEXT) as response:
            content_type = response.headers.get_content_type()
            image_header = response.read(32)
            return content_type.startswith("image/") and len(image_header) >= 16
    except HTTPError as exc:
        if exc.code in (403, 404, 410):
            return False
        raise
    except (TimeoutError, socket.timeout):
        return False
    except URLError as exc:
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            return False
        raise


def retry_stale_upload_once(experiment_id: str, nm_id: int, photo_number: int, image_path: Path) -> bool:
    """Retry one unpublished image in its existing slot, never allocate a new slot."""
    with connect_db() as db:
        pending = db.execute(
            "SELECT upload_sent, upload_attempts, created_at, last_upload_at FROM pending_uploads WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
        if not pending or not int(pending["upload_sent"]) or int(pending["upload_attempts"]) >= 2:
            return False
        try:
            anchor = pending["last_upload_at"] or pending["created_at"]
            age = datetime.now().astimezone() - datetime.fromisoformat(anchor)
        except (TypeError, ValueError):
            return False
        if age < timedelta(minutes=15):
            return False
        attempt = int(pending["upload_attempts"]) + 1
        upload_stamp = stamp()
        # Save the attempt before the network call. If the request times out
        # after WB accepts it, the scheduler must not submit it repeatedly.
        db.execute(
            "UPDATE pending_uploads SET upload_attempts=?, last_upload_at=? WHERE experiment_id=?",
            (attempt, upload_stamp, experiment_id),
        )
        log_event(
            db,
            experiment_id,
            "info",
            f"Фото не появилось у WB. Повторно отправляю тот же файл в слот №{photo_number}; новый слот не создаю.",
        )
    wb_upload_image(nm_id, photo_number, str(image_path))
    return True


def apply_variant(experiment_id: str, variant_index: int) -> None:
    token = get_token("wb_content_token")
    if not token:
        raise RuntimeError("Сохраните токен WB с доступом к Контенту.")
    with connect_db() as db:
        row = db.execute(
            """SELECT e.*, p.nm_id, p.photos_json
               FROM experiments e JOIN products p ON p.id=e.product_id WHERE e.id=?""",
            (experiment_id,),
        ).fetchone()
        variants = db.execute(
            "SELECT image_url, local_path FROM variants WHERE experiment_id=? ORDER BY position",
            (experiment_id,),
        ).fetchall()
        pending = db.execute(
            "SELECT * FROM pending_uploads WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
    if not row or variant_index >= len(variants):
        raise RuntimeError("Не удалось найти вариант изображения для переключения.")

    original_photos = json.loads(row["original_photos_json"] or row["photos_json"])
    selected = variants[variant_index]
    nm_id = int(row["nm_id"])
    image_path = Path(selected["local_path"]).resolve() if selected["local_path"] else None
    if not image_path or not image_path.is_file():
        if variant_index == 0:
            raise RuntimeError("Исходный файл главного фото не сохранён. Восстановить старую ссылку безопасно нельзя.")
        raise RuntimeError(f"Не найден локальный файл варианта «{selected['label']}».")
    if not original_photos:
        raise RuntimeError("В тесте не сохранён список фотографий карточки.")
    secondary_photos = original_photos[1:]
    expected_cover = image_path.read_bytes()
    alternative_images: list[tuple[bytes, str]] = []
    for index, variant in enumerate(variants):
        if index == variant_index or not variant["local_path"]:
            continue
        alternative_path = Path(variant["local_path"]).resolve()
        if alternative_path.is_file():
            alternative_images.append((alternative_path.read_bytes(), alternative_path.suffix))
    pending_matches = pending and int(pending["variant_index"]) == variant_index
    if pending and not pending_matches:
        with connect_db() as db:
            db.execute("DELETE FROM pending_uploads WHERE experiment_id=?", (experiment_id,))
        pending = None

    if pending_matches:
        existing_photo_list = json.loads(pending["existing_photos_json"])
        target_url = pending["target_url"]
        save_sent = int(pending["save_sent"])
        url_save_attempts = int(pending["url_save_attempts"])
        direct_replace_attempts = int(pending["direct_replace_attempts"] or 0)
        previous_cover_hash = pending["previous_cover_hash"]
        last_upload_at = pending["last_upload_at"]
    else:
        current = fetch_card(nm_id)
        if not current["photos"] or current["photos"][1:] != secondary_photos:
            raise RuntimeError(
                "Состав дополнительных фото в карточке WB изменился. Обновите карточку перед переключением, чтобы не затронуть галерею."
            )
        existing_photo_list = current["photos"]
        try:
            current_cover, current_extension = download_wb_image(existing_photo_list[0])
            previous_cover_hash = hashlib.sha256(current_cover).hexdigest()
            if images_are_similar(
                expected_cover,
                current_cover,
                image_path.suffix,
                current_extension,
                alternative_images,
            ):
                return
        except Exception:
            previous_cover_hash = None
        with connect_db() as db:
            db.execute(
                """INSERT OR REPLACE INTO pending_uploads(
                       experiment_id, variant_index, photo_number, existing_photos_json, upload_sent, created_at,
                       target_url, save_sent, url_save_attempts, previous_cover_hash, check_failures, next_check_at
                   ) VALUES (?, ?, 1, ?, 0, ?, NULL, 0, 0, ?, 0, NULL)""",
                (experiment_id, variant_index, json.dumps(existing_photo_list), stamp(), previous_cover_hash),
            )
        target_url = None
        save_sent = 0
        url_save_attempts = 0
        direct_replace_attempts = 0
        last_upload_at = None

    if existing_photo_list[1:] != secondary_photos:
        raise RuntimeError("Сохранённый порядок дополнительных фотографий теста не совпадает с карточкой.")

    def order_is_confirmed(photos: list[str]) -> bool:
        if len(photos) < len(secondary_photos) + 1:
            return False
        if photos[1 : len(secondary_photos) + 1] != secondary_photos:
            return False
        try:
            current_cover, current_extension = download_wb_image(photos[0])
            return images_are_similar(
                expected_cover,
                current_cover,
                image_path.suffix,
                current_extension,
                alternative_images,
            )
        except Exception:
            return False

    def save_photo_list_by_url(current_photos: list[str]) -> None:
        nonlocal previous_cover_hash, target_url, url_save_attempts, save_sent
        if not current_photos or current_photos[1:] != secondary_photos:
            raise RuntimeError(
                "Дополнительные фото карточки изменились. Не отправляю новый порядок, чтобы не перезаписать галерею."
            )
        try:
            current_cover, _ = download_wb_image(current_photos[0])
            previous_cover_hash = hashlib.sha256(current_cover).hexdigest()
        except Exception:
            previous_cover_hash = None
        kie_key = get_token("kie_api_key")
        if not kie_key:
            raise RuntimeError("Подключите Kie.ai, чтобы передать сохранённое фото WB по прямой ссылке.")
        hosted_url = _upload_source_file(str(image_path), kie_key, nm_id)
        parsed_url = urlparse(hosted_url)
        if parsed_url.scheme != "https" or not parsed_url.path.lower().endswith(
            (".jpg", ".jpeg", ".png", ".webp")
        ):
            raise RuntimeError("Kie.ai не вернул прямую ссылку на файл изображения.")

        target_url = hosted_url
        save_sent = 1
        url_save_attempts += 1
        attempt_stamp = stamp()
        # Persist the target before the request: a reset after WB accepted the
        # write must resume verification instead of resending the image.
        with connect_db() as db:
            db.execute(
                """UPDATE pending_uploads SET target_url=?, save_sent=1, upload_sent=1,
                       url_save_attempts=?, last_upload_at=?, previous_cover_hash=?,
                       check_failures=0, next_check_at=NULL WHERE experiment_id=?""",
                (target_url, url_save_attempts, attempt_stamp, previous_cover_hash, experiment_id),
            )
            log_event(
                db,
                experiment_id,
                "info",
                "Передаю сохранённое фото WB по прямой ссылке и сохраняю порядок остальных фотографий.",
            )

        result = wb_request(
            CONTENT_BASE,
            "/content/v3/media/save",
            token,
            {"nmId": nm_id, "data": [hosted_url] + secondary_photos},
        )
        additional_errors = result.get("additionalErrors")
        if result.get("error") or additional_errors:
            details = result.get("errorText") or additional_errors or "ошибка Content API"
            raise RuntimeError(f"WB не подтвердил список фото: {details}")
        with connect_db() as db:
            log_event(
                db,
                experiment_id,
                "info",
                "WB принял список фото. Проверяю новую обложку и прежний порядок галереи.",
            )

    def replace_cover_directly(current_photos: list[str]) -> None:
        """Fallback to WB's binary upload for slot 1; secondary gallery stays untouched."""
        nonlocal direct_replace_attempts, previous_cover_hash, last_upload_at
        if not current_photos or current_photos[1:] != secondary_photos:
            raise RuntimeError(
                "Дополнительные фото карточки изменились. Не загружаю фото в слот №1, чтобы не затронуть галерею."
            )
        try:
            current_cover, _ = download_wb_image(current_photos[0])
            previous_cover_hash = hashlib.sha256(current_cover).hexdigest()
        except Exception:
            previous_cover_hash = None
        direct_replace_attempts += 1
        last_upload_at = stamp()
        # Mark before sending: a socket timeout can happen after WB receives the file.
        with connect_db() as db:
            db.execute(
                """UPDATE pending_uploads SET direct_replace_sent=1, direct_replace_attempts=?,
                       upload_sent=1, last_upload_at=?, previous_cover_hash=?, check_failures=0,
                       next_check_at=NULL WHERE experiment_id=?""",
                (direct_replace_attempts, last_upload_at, previous_cover_hash, experiment_id),
            )
            log_event(
                db,
                experiment_id,
                "info",
                "Передаю сохранённое контрольное фото напрямую в слот №1 WB; остальные 11 фото не меняю.",
            )
        wb_upload_image(nm_id, 1, str(image_path))
        with connect_db() as db:
            log_event(db, experiment_id, "info", "WB принял файл для слота №1. Проверяю фактическую обложку карточки.")

    current = fetch_card(nm_id)
    if save_sent and target_url and order_is_confirmed(current["photos"]):
        with connect_db() as db:
            db.execute("DELETE FROM pending_uploads WHERE experiment_id=?", (experiment_id,))
        return

    retry_age = timedelta(0)
    try:
        if last_upload_at:
            retry_age = datetime.now().astimezone() - datetime.fromisoformat(last_upload_at)
    except (TypeError, ValueError):
        pass

    # URL imports are accepted asynchronously by WB but may never become the
    # card cover. Try the native binary endpoint once, replacing only slot 1.
    if save_sent and url_save_attempts >= 1 and direct_replace_attempts == 0:
        replace_cover_directly(current["photos"])
        raise PhotoPropagationPending("WB принял фото в слот №1. Жду подтверждения обложки, не собирая CTR раньше времени.")

    # After the binary fallback, wait for WB's card read model to catch up before
    # considering the second, rate-limited URL import.
    if direct_replace_attempts:
        if url_save_attempts < 2 and retry_age >= timedelta(minutes=30):
            save_photo_list_by_url(current["photos"])
            raise PhotoPropagationPending("Повторно отправил список фото через WB API. Жду подтверждения обложки.")
        raise PhotoPropagationPending("WB ещё не подтвердил обложку. Проверю карточку после паузы.")

    if not save_sent or (url_save_attempts < 2 and retry_age >= timedelta(minutes=30)):
        save_photo_list_by_url(current["photos"])
        raise PhotoPropagationPending("WB принял список фото. Жду подтверждения обложки и порядка галереи.")

    raise PhotoPropagationPending("WB пока не подтвердил обложку. Повторю проверку с увеличенным интервалом.")


def fetch_ad_metrics(nm_id: int, advert_id: int, start_day: str, end_day: str) -> tuple[int | None, int | None, str | None]:
    token = get_token("wb_promotion_token")
    if not token:
        raise RuntimeError("Сохраните токен WB с доступом к Продвижению.")
    result = wb_request(
        ADS_BASE,
        "/adv/v1/normquery/stats",
        token,
        {
            "from": start_day,
            "to": end_day,
            "items": [{"advertId": advert_id, "nmId": nm_id}],
        },
    )
    views_total = 0
    clicks_total = 0
    has_rows = False
    missing_views = False
    for item in result.get("items", []):
        for daily in item.get("dailyStats", []) or []:
            stats = daily.get("stats")
            if isinstance(stats, list):
                entries = stats
            else:
                entries = [daily.get("stat") or {}]
            for stat in entries:
                has_rows = True
                if stat.get("views") is None:
                    missing_views = True
                else:
                    views_total += int(stat.get("views", 0) or 0)
                clicks_total += int(stat.get("clicks", 0) or 0)
    if missing_views:
        return None, clicks_total, "WB не вернул показы. Для прямого CTR нужна кампания с оплатой за показы (CPM)."
    if not has_rows:
        return 0, 0, None
    return views_total, clicks_total, None


def log_event(db: sqlite3.Connection, experiment_id: str, level: str, message: str) -> None:
    db.execute(
        "INSERT INTO events(experiment_id, level, message, created_at) VALUES (?, ?, ?, ?)",
        (experiment_id, level, message, stamp()),
    )


def make_stage(
    db: sqlite3.Connection,
    experiment_id: str,
    variant_index: int,
    start_day: date,
    baseline_views: int = 0,
    baseline_clicks: int = 0,
    baseline_ready: bool = False,
) -> int:
    # Store cumulative daily-report totals at the stage boundary so fast switches
    # on the same calendar day can be compared by deltas.
    sentinel = datetime(9999, 12, 31, 0, 0, tzinfo=TZ)
    cursor = db.execute(
        """INSERT INTO stages(
               experiment_id, variant_index, start_date, end_date, switch_at,
               baseline_views, baseline_clicks, baseline_ready, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            experiment_id,
            variant_index,
            start_day.isoformat(),
            start_day.isoformat(),
            stamp(sentinel),
            baseline_views,
            baseline_clicks,
            int(baseline_ready),
            stamp(),
        ),
    )
    stage_id = int(cursor.lastrowid)
    db.execute(
        "UPDATE experiments SET current_stage_id=?, current_variant_index=?, updated_at=? WHERE id=?",
        (stage_id, variant_index, stamp(), experiment_id),
    )
    return stage_id


def close_stage(stage: sqlite3.Row, end_day: date) -> None:
    views = max(0, int(stage["progress_views"] or 0))
    clicks = max(0, int(stage["progress_clicks"] or 0))
    ctr = clicks / views * 100 if views else None
    state = "warning" if stage["progress_error"] else "done"
    with connect_db() as db:
        db.execute(
            """UPDATE stages SET end_date=?, collect_after=NULL, views=?, clicks=?, ctr=?,
               collection_state=?, collection_error=progress_error WHERE id=? AND collection_state='pending'""",
            (end_day.isoformat(), views, clicks, ctr, state, int(stage["id"])),
        )


def fail_experiment(experiment_id: str, message: str) -> None:
    with connect_db() as db:
        db.execute(
            "UPDATE experiments SET status='error', error=?, updated_at=? WHERE id=?",
            (message, stamp(), experiment_id),
        )
        log_event(db, experiment_id, "error", message)


def reserve_promotion_stats_request() -> bool:
    global PROMOTION_STATS_LAST_REQUEST
    with PROMOTION_STATS_LOCK:
        current = time.monotonic()
        if current - PROMOTION_STATS_LAST_REQUEST < promotion_stats_min_interval_seconds():
            return False
        PROMOTION_STATS_LAST_REQUEST = current
        return True


def promotion_stats_min_interval_seconds() -> int:
    # WB documents 30-minute spacing for Basic tokens and a 6-second
    # interval for Personal/Service tokens on this statistics endpoint.
    token = get_token("wb_promotion_token")
    try:
        encoded = token.split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        account_type = str(payload.get("acc", ""))
        if account_type == "1" and not payload.get("for"):
            return 30 * 60 + 5
        if account_type in ("2", "3", "4") or payload.get("for"):
            return 6
    except (IndexError, ValueError, TypeError, UnicodeDecodeError, binascii.Error):
        pass
    return 30 * 60 + 5


def check_stage_progress(experiment: sqlite3.Row, stage: sqlite3.Row, now: datetime) -> None:
    if int(stage["target_reached"] or 0):
        return
    last_checked = stage["last_checked_at"]
    if last_checked:
        try:
            if now - datetime.fromisoformat(last_checked) < timedelta(seconds=promotion_stats_min_interval_seconds()):
                return
        except ValueError:
            pass
    if not reserve_promotion_stats_request():
        return
    try:
        total_views, total_clicks, warning = fetch_ad_metrics(
            int(experiment["nm_id"]),
            int(experiment["advert_id"]),
            str(experiment["metrics_start_date"]),
            now.date().isoformat(),
        )
        if not int(stage["baseline_ready"] or 0):
            if warning or total_views is None:
                with connect_db() as db:
                    db.execute(
                        "UPDATE stages SET last_checked_date=?, last_checked_at=?, progress_error=? WHERE id=?",
                        (now.date().isoformat(), stamp(now), warning or "Wildberries пока не вернул стартовые показы.", int(stage["id"])),
                    )
                    if warning:
                        log_event(db, experiment["id"], "warning", warning)
                return
            with connect_db() as db:
                baseline_views = max(int(stage["baseline_views"] or 0), int(total_views or 0))
                baseline_clicks = max(int(stage["baseline_clicks"] or 0), int(total_clicks or 0))
                db.execute(
                    """UPDATE stages SET baseline_views=?, baseline_clicks=?, baseline_ready=1,
                       progress_views=0, progress_clicks=0, last_checked_date=?, last_checked_at=?,
                       progress_error=NULL WHERE id=?""",
                    (baseline_views, baseline_clicks, now.date().isoformat(), stamp(now), int(stage["id"])),
                )
                log_event(db, experiment["id"], "info", "Фото подтверждено WB; зафиксировал стартовые показы этапа. Счётчик начнётся со следующего обновления статистики.")
            return
        baseline_views = int(stage["baseline_views"] or 0)
        baseline_clicks = int(stage["baseline_clicks"] or 0)
        views = max(int(stage["progress_views"] or 0), max(0, total_views - baseline_views)) if total_views is not None else None
        clicks = max(int(stage["progress_clicks"] or 0), max(0, total_clicks - baseline_clicks)) if total_clicks is not None else 0
        with connect_db() as db:
            db.execute(
                """UPDATE stages SET progress_views=?, progress_clicks=?, last_checked_date=?, last_checked_at=?,
                   progress_error=? WHERE id=?""",
                (views or 0, clicks or 0, now.date().isoformat(), stamp(now), warning, int(stage["id"])),
            )
            target = int(experiment["target_views"])
            if views is not None and views >= target:
                switch_at = now
                db.execute(
                    "UPDATE stages SET target_reached=1, switch_at=? WHERE id=?",
                    (stamp(switch_at), int(stage["id"])),
                )
                if int(experiment["current_variant_index"]) >= int(experiment["variant_count"]) - 1:
                    action = "Тест завершится, и исходное главное фото будет восстановлено."
                else:
                    action = "Переключаю фото на следующий вариант."
                message = f"WB подтвердил порог: {views:,} показов. {action}".replace(",", " ")
                log_event(db, experiment["id"], "info", message)
            elif warning:
                log_event(db, experiment["id"], "warning", warning)
    except Exception as exc:
        with connect_db() as db:
            db.execute(
                "UPDATE stages SET last_checked_date=?, last_checked_at=?, progress_error=? WHERE id=?",
                (now.date().isoformat(), stamp(now), public_error(exc), int(stage["id"])),
            )
            log_event(db, experiment["id"], "warning", f"Не удалось обновить показы; повторю проверку с учётом лимита WB: {public_error(exc)}")


def scheduler_tick() -> None:
    now = datetime.now(TZ)
    with connect_db() as db:
        scheduled = db.execute(
            "SELECT id, scheduled_start, initial_baseline_views, initial_baseline_clicks FROM experiments WHERE status='scheduled'"
        ).fetchall()
    for experiment in scheduled:
        if datetime.fromisoformat(experiment["scheduled_start"]) <= now:
            experiment_id = experiment["id"]
            if not pending_upload_check_due(experiment_id, now):
                continue
            try:
                apply_variant(experiment_id, 0)
                with connect_db() as db:
                    make_stage(
                        db,
                        experiment_id,
                        0,
                        now.date(),
                        baseline_views=int(experiment["initial_baseline_views"] or 0),
                        baseline_clicks=int(experiment["initial_baseline_clicks"] or 0),
                        baseline_ready=False,
                    )
                    db.execute(
                        "UPDATE experiments SET status='active', scheduled_start=NULL, error=NULL, updated_at=? WHERE id=?",
                        (stamp(), experiment_id),
                    )
                    log_event(db, experiment_id, "info", "Тест начался: установлен контрольный вариант. Порог считается по показам.")
            except PhotoPropagationPending:
                # Keep the test queued and apply exponential backoff to WB checks.
                defer_pending_upload_check(experiment_id)
                log_pending_upload_delay_once(experiment_id)
                continue
            except Exception as exc:
                if is_retryable_wb_failure(exc) and has_pending_wb_upload(experiment_id):
                    defer_pending_upload_check(experiment_id)
                    log_wb_retry_once(experiment_id, exc)
                    continue
                fail_experiment(experiment_id, public_error(exc))

    with connect_db() as db:
        active = db.execute(
            """SELECT e.id, e.target_views, e.current_variant_index, e.current_stage_id,
                      p.nm_id, e.advert_id,
                      (SELECT COUNT(*) FROM variants v WHERE v.experiment_id=e.id) AS variant_count,
                      (SELECT MIN(s.start_date) FROM stages s WHERE s.experiment_id=e.id) AS metrics_start_date
               FROM experiments e JOIN products p ON p.id=e.product_id WHERE e.status='active'"""
        ).fetchall()
    for experiment in active:
        if not pending_upload_check_due(experiment["id"], now):
            continue
        with connect_db() as db:
            stage = db.execute("SELECT * FROM stages WHERE id=?", (experiment["current_stage_id"],)).fetchone()
            count = int(db.execute("SELECT COUNT(*) FROM variants WHERE experiment_id=?", (experiment["id"],)).fetchone()[0])
        if not stage:
            continue
        try:
            with connect_db() as db:
                pending_current = db.execute(
                    "SELECT variant_index FROM pending_uploads WHERE experiment_id=?",
                    (experiment["id"],),
                ).fetchone()
            if pending_current and int(pending_current["variant_index"]) == int(experiment["current_variant_index"]):
                apply_variant(experiment["id"], int(experiment["current_variant_index"]))
                # Do not start a counter on the same tick that recovered a
                # pending upload; the next stats read establishes its baseline.
                continue
            if datetime.fromisoformat(stage["switch_at"]) <= now:
                next_index = int(experiment["current_variant_index"]) + 1
                if next_index >= count:
                    apply_variant(experiment["id"], 0)
                    close_stage(stage, now.date())
                    with connect_db() as db:
                        db.execute(
                            "UPDATE experiments SET status='completed', selected_variant_index=NULL, scheduled_start=NULL, error=NULL, updated_at=? WHERE id=?",
                            (stamp(), experiment["id"]),
                        )
                        log_event(db, experiment["id"], "info", "Пятый вариант набрал порог показов. Тест завершён, исходное главное фото восстановлено; можно выбрать фото по CTR.")
                    continue
                apply_variant(experiment["id"], next_index)
                close_stage(stage, now.date())
                with connect_db() as db:
                    next_baseline_views = int(stage["baseline_views"] or 0) + int(stage["progress_views"] or 0)
                    next_baseline_clicks = int(stage["baseline_clicks"] or 0) + int(stage["progress_clicks"] or 0)
                    make_stage(
                        db,
                        experiment["id"],
                        next_index,
                        now.date(),
                        baseline_views=next_baseline_views,
                        baseline_clicks=next_baseline_clicks,
                        baseline_ready=False,
                    )
                    log_event(db, experiment["id"], "info", f"Установлен вариант {next_index + 1} из {count}; он остаётся до порога показов.")
            else:
                check_stage_progress(experiment, stage, now)
        except PhotoPropagationPending:
            # Keep the stage active and back off while WB publishes the image.
            defer_pending_upload_check(experiment["id"])
            log_pending_upload_delay_once(experiment["id"])
            continue
        except Exception as exc:
            if is_retryable_wb_failure(exc) and has_pending_wb_upload(experiment["id"]):
                defer_pending_upload_check(experiment["id"])
                log_wb_retry_once(experiment["id"], exc)
                continue
            fail_experiment(experiment["id"], public_error(exc))

    with connect_db() as db:
        due = db.execute(
            """SELECT s.*, e.advert_id, p.nm_id
               FROM stages s JOIN experiments e ON e.id=s.experiment_id
               JOIN products p ON p.id=e.product_id
               WHERE s.collection_state='waiting' AND s.collect_after<=? AND e.status IN ('active','paused','error','finishing','completed')""",
            (stamp(now),),
        ).fetchall()
    for stage in due:
        if not reserve_promotion_stats_request():
            break
        try:
            views, clicks, warning = fetch_ad_metrics(
                int(stage["nm_id"]), int(stage["advert_id"]), stage["start_date"], stage["end_date"]
            )
            ctr = (clicks / views * 100) if views else None
            with connect_db() as db:
                db.execute(
                    """UPDATE stages SET views=?, clicks=?, ctr=?, collection_state=?, collection_error=? WHERE id=?""",
                    (views, clicks, ctr, "warning" if warning else "done", warning, int(stage["id"])),
                )
                log_event(
                    db,
                    stage["experiment_id"],
                    "warning" if warning else "info",
                    warning or f"Собрана статистика за {stage['start_date']} — {stage['end_date']}.",
                )
                exp_status = db.execute("SELECT status FROM experiments WHERE id=?", (stage["experiment_id"],)).fetchone()
                if exp_status and exp_status["status"] == "finishing":
                    pending = db.execute(
                        "SELECT COUNT(*) FROM stages WHERE experiment_id=? AND collection_state IN ('pending','waiting')",
                        (stage["experiment_id"],),
                    ).fetchone()[0]
                    if not pending:
                        db.execute(
                            "UPDATE experiments SET status='completed', updated_at=? WHERE id=?",
                            (stamp(), stage["experiment_id"]),
                        )
                        log_event(db, stage["experiment_id"], "info", "Итоговая статистика собрана. Можно сравнить CTR и выбрать главное фото.")
        except Exception as exc:
            with connect_db() as db:
                db.execute(
                    "UPDATE stages SET collection_state='waiting', collect_after=?, collection_error=? WHERE id=?",
                    (stamp(now + timedelta(hours=6)), public_error(exc), int(stage["id"])),
                )
                log_event(db, stage["experiment_id"], "warning", f"Повторю сбор статистики позже: {public_error(exc)}")


def scheduler_loop() -> None:
    while True:
        try:
            with EXPERIMENT_LOCK:
                scheduler_tick()
        except Exception as exc:
            print(f"Scheduler error: {exc}", flush=True)
        time.sleep(POLL_SECONDS)


def dashboard_state() -> dict:
    with connect_db() as db:
        experiments = db.execute(
            """SELECT e.*, p.nm_id, p.title
               FROM experiments e JOIN products p ON p.id=e.product_id
               ORDER BY e.created_at DESC"""
        ).fetchall()
        output = []
        for exp in experiments:
            variants = db.execute("SELECT * FROM variants WHERE experiment_id=? ORDER BY position", (exp["id"],)).fetchall()
            stages = db.execute("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (exp["id"],)).fetchall()
            event_rows = db.execute(
                "SELECT level, message, created_at FROM events WHERE experiment_id=? ORDER BY id DESC LIMIT 100",
                (exp["id"],),
            ).fetchall()
            pending_upload = db.execute(
                "SELECT variant_index, upload_sent, url_save_attempts, check_failures, last_upload_at, next_check_at FROM pending_uploads WHERE experiment_id=?",
                (exp["id"],),
            ).fetchone()
            summaries = []
            for variant in variants:
                relevant = [stage for stage in stages if int(stage["variant_index"]) == int(variant["position"])]
                valid = [stage for stage in relevant if stage["views"] is not None and stage["clicks"] is not None]
                views = sum(int(stage["views"]) for stage in valid)
                clicks = sum(int(stage["clicks"]) for stage in valid)
                summaries.append({
                    "position": int(variant["position"]),
                    "label": variant["label"], "image_url": variant["image_url"],
                    "views": views, "clicks": clicks,
                    "ctr": round(clicks / views * 100, 3) if views else None,
                    "periods": len(valid),
                })
            results_ready = exp["status"] == "completed" and bool(stages) and all(
                stage["collection_state"] in ("done", "warning", "skipped") for stage in stages
            )
            output.append({
                "id": exp["id"], "nm_id": exp["nm_id"], "title": exp["title"],
                "advert_id": exp["advert_id"], "target_views": exp["target_views"],
                "status": exp["status"], "scheduled_start": exp["scheduled_start"],
                "current_stage_id": exp["current_stage_id"],
                "current_variant_index": exp["current_variant_index"],
                "selected_variant_index": exp["selected_variant_index"], "results_ready": results_ready,
                "error": exp["error"], "variants": summaries,
                "stages": [dict(stage) for stage in stages],
                "events": summarize_wb_events(event_rows),
                "awaiting_photo_confirmation": bool(pending_upload and int(pending_upload["upload_sent"])),
                "photo_upload_attempts": int(pending_upload["url_save_attempts"] or 0) if pending_upload else 0,
                "photo_check_failures": int(pending_upload["check_failures"] or 0) if pending_upload else 0,
                "photo_next_check_at": pending_upload["next_check_at"] if pending_upload else None,
            })
    return {
        **credential_status(),
        "timezone": "Europe/Moscow",
        "competitor_reference_support": True,
        "experiments": output,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "CTRLab/0.1"

    def send_json(self, status: int, value: dict) -> None:
        body = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def read_json(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        if size > 2_500_000:
            raise ValueError("Слишком большой запрос.")
        raw = self.rfile.read(size)
        return json.loads(raw.decode("utf-8") or "{}")

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/state":
            self.send_json(200, dashboard_state())
            return
        request_url = urlparse(self.path)
        if request_url.path == "/api/products":
            force_refresh = "refresh=1" in request_url.query.split("&")
            try:
                self.send_json(200, {"products": fetch_product_catalog(force_refresh)})
            except Exception as exc:
                self.send_json(502, {"error": public_error(exc)})
            return
        if self.path.startswith("/generated/"):
            filename = self.path.removeprefix("/generated/")
            if not filename or Path(filename).name != filename:
                self.send_error(404)
                return
            with connect_db() as db:
                row = db.execute("SELECT local_path FROM generated_images WHERE filename=?", (filename,)).fetchone()
            if not row:
                self.send_error(404)
                return
            image_path = Path(row["local_path"]).resolve()
            if GENERATED_DIR.resolve() not in image_path.parents or not image_path.is_file():
                self.send_error(404)
                return
            content = image_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "private, max-age=3600")
            self.end_headers()
            self.wfile.write(content)
            return
        if self.path.startswith("/originals/"):
            filename = self.path.removeprefix("/originals/")
            if not filename or Path(filename).name != filename:
                self.send_error(404)
                return
            image_path = local_cover_path(str(ORIGINALS_DIR / filename))
            if not image_path:
                self.send_error(404)
                return
            content = image_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", mimetypes.guess_type(filename)[0] or "application/octet-stream")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "private, max-age=31536000, immutable")
            self.end_headers()
            self.wfile.write(content)
            return
        if self.path == "/":
            self.path = "/index.html"
        requested = (STATIC / self.path.lstrip("/")).resolve()
        if STATIC.resolve() not in requested.parents or not requested.is_file():
            self.send_error(404)
            return
        content = requested.read_bytes()
        content_type = "text/html; charset=utf-8" if requested.suffix == ".html" else "text/plain; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_POST(self) -> None:  # noqa: N802
        try:
            body = self.read_json()
            if self.path == "/api/credentials":
                self.send_json(200, save_credentials(body))
                return
            if self.path == "/api/generation-balance":
                api_key = get_token("kie_api_key")
                if not api_key:
                    raise RuntimeError("Сначала сохраните ключ Kie.ai.")
                self.send_json(200, {"credits": get_credit_balance(api_key)})
                return
            if self.path == "/api/lookup":
                nm_id = int(body.get("nm_id", 0))
                if nm_id <= 0:
                    raise ValueError("Укажите корректный артикул nmID.")
                card = fetch_card(nm_id)
                cover = ensure_product_cover(nm_id, card)
                cover_was_rebased = False
                if not cover["available"]:
                    # Legacy cards may have only mutable WB image URLs stored.
                    # Save the currently served cover as a new local baseline
                    # rather than blocking every future test on the lost file.
                    cover = ensure_product_cover(nm_id, card, accept_current=True)
                    cover_was_rebased = True
                card["cover_backup_available"] = cover["available"]
                card["cover_backup_url"] = cover["image_url"]
                card["cover_backup_state"] = cover["state"]
                card["cover_baseline_rebased"] = cover_was_rebased
                label_order = {label: index for index, (label, _) in enumerate(VARIANTS, start=1)}
                with connect_db() as db:
                    if cover["available"] and cover["cover_hash"]:
                        saved_rows = db.execute(
                            """SELECT id, filename, label FROM generated_images
                               WHERE nm_id=? AND cover_hash=? ORDER BY created_at DESC, rowid DESC""",
                            (nm_id, cover["cover_hash"]),
                        ).fetchall()
                    else:
                        saved_rows = db.execute(
                            "SELECT id, filename, label FROM generated_images WHERE nm_id=? ORDER BY created_at DESC, rowid DESC",
                            (nm_id,),
                        ).fetchall()
                latest_by_label = {}
                for row in saved_rows:
                    if row["label"] in label_order and row["label"] not in latest_by_label:
                        image_path = GENERATED_DIR / row["filename"]
                        if image_path.is_file():
                            latest_by_label[row["label"]] = {
                                "id": row["id"],
                                "label": row["label"],
                                "index": label_order[row["label"]],
                                "image_url": f"/generated/{row['filename']}",
                            }
                card["generated_variants"] = sorted(latest_by_label.values(), key=lambda item: item["index"])
                if get_token("wb_promotion_token"):
                    try:
                        card["active_campaigns"] = fetch_active_campaigns(nm_id)
                        card["campaign_lookup_error"] = None
                    except Exception as exc:
                        card["active_campaigns"] = []
                        card["campaign_lookup_error"] = public_error(exc)
                else:
                    card["active_campaigns"] = []
                    card["campaign_lookup_error"] = "Сохраните токен WB с доступом к Продвижению, чтобы найти активную рекламу."
                self.send_json(200, card)
                return
            if self.path == "/api/set-current-cover":
                nm_id = int(body.get("nm_id", 0))
                if nm_id <= 0:
                    raise ValueError("Укажите корректный артикул nmID.")
                with connect_db() as db:
                    active = db.execute(
                        """SELECT 1 FROM experiments e JOIN products p ON p.id=e.product_id
                           WHERE p.nm_id=? AND e.status IN ('scheduled','active','finishing','error') LIMIT 1""",
                        (nm_id,),
                    ).fetchone()
                if active:
                    raise ValueError("Сначала остановите активный тест. Затем можно сохранить текущее фото как новый контроль.")
                card = fetch_card(nm_id)
                cover = ensure_product_cover(nm_id, card, accept_current=True)
                self.send_json(200, {
                    "cover_backup_available": True,
                    "cover_backup_url": cover["image_url"],
                    "cover_backup_state": cover["state"],
                })
                return
            if self.path == "/api/generate-variants":
                self.generate_variants(body)
                return
            if self.path == "/api/regenerate-variant":
                nm_id = int(body.get("nm_id", 0))
                if nm_id <= 0:
                    raise ValueError("Укажите корректный артикул nmID.")
                competitor_images = body.get("competitor_images") or []
                if not isinstance(competitor_images, list) or len(competitor_images) > 4:
                    raise ValueError("Можно передать не более четырёх фото конкурентов.")
                if body.get("cost_confirmed") is not True:
                    raise ValueError("Перед генерацией подтвердите стоимость в приложении.")
                api_key = get_token("kie_api_key")
                if not api_key:
                    raise RuntimeError("Провайдер генерации Kie.ai не подключён.")
                card = fetch_card(nm_id)
                cover = ensure_product_cover(nm_id, card)
                if not cover["available"] or not cover["path"]:
                    raise RuntimeError("Сначала сохраните текущее фото как новый контроль. Старое исходное фото не сохранено.")
                item = regenerate_variant(
                    card["photos"][0],
                    api_key,
                    GENERATED_DIR,
                    nm_id,
                    str(body.get("label") or ""),
                    str(body.get("mode") or ""),
                    str(body.get("additional_instruction") or ""),
                    source_path=cover["path"],
                    reference_images=competitor_images,
                )
                row = {**item, "id": uuid.uuid4().hex}
                with connect_db() as db:
                    db.execute(
                        "INSERT INTO generated_images(id, nm_id, filename, local_path, label, prompt, cover_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (row["id"], nm_id, row["filename"], row["path"], row["label"], str(body.get("additional_instruction") or "")[:1500], cover["cover_hash"], stamp()),
                    )
                self.send_json(201, {
                    "variant": {
                        "id": row["id"],
                        "label": row["label"],
                        "index": row["index"],
                        "image_url": f"/generated/{row['filename']}",
                    }
                })
                return
            if self.path == "/api/experiments":
                with EXPERIMENT_LOCK:
                    self.create_experiment(body)
                return
            parts = self.path.strip("/").split("/")
            if len(parts) == 4 and parts[0] == "api" and parts[1] == "experiments":
                experiment_id, action = parts[2], parts[3]
                if action == "start":
                    with EXPERIMENT_LOCK:
                        self.start_experiment(experiment_id)
                    return
                if action == "stop":
                    with EXPERIMENT_LOCK:
                        self.stop_experiment(experiment_id)
                    return
                if action == "select":
                    with EXPERIMENT_LOCK:
                        self.select_variant(experiment_id, body)
                    return
                if action == "delete":
                    with EXPERIMENT_LOCK:
                        self.delete_experiment(experiment_id)
                    return
            self.send_json(404, {"error": "Маршрут не найден."})
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            self.send_json(400, {"error": str(exc)})
        except Exception as exc:
            self.send_json(502, {"error": public_error(exc)})

    def generate_variants(self, body: dict) -> None:
        nm_id = int(body.get("nm_id", 0))
        if nm_id <= 0:
            raise ValueError("Укажите корректный артикул nmID.")
        competitor_images = body.get("competitor_images") or []
        if not isinstance(competitor_images, list) or len(competitor_images) > 4:
            raise ValueError("Можно передать не более четырёх фото конкурентов.")
        if body.get("cost_confirmed") is not True:
            raise ValueError("Перед генерацией подтвердите стоимость в приложении.")
        with GENERATION_LOCK:
            if nm_id in GENERATING_NM_IDS:
                self.send_json(409, {"error": "Для этого товара генерация уже выполняется. Дождитесь результата; повторный запуск заблокирован, чтобы не списать кредиты повторно."})
                return
            GENERATING_NM_IDS.add(nm_id)
        try:
            api_key = get_token("kie_api_key")
            if not api_key:
                raise RuntimeError("Сначала сохраните ключ Kie.ai в настройках приложения.")
            card = fetch_card(nm_id)
            cover = ensure_product_cover(nm_id, card)
            if not cover["available"] or not cover["path"]:
                raise RuntimeError("Сначала сохраните текущее фото как новый контроль. Старое исходное фото не сохранено.")
            completed_labels = [str(label) for label in body.get("completed_labels", [])]
            created, errors = generate_kie_variants(
                card["photos"][0], api_key, GENERATED_DIR, nm_id, completed_labels,
                source_path=cover["path"], reference_images=competitor_images,
            )
            now = stamp()
            rows = []
            with connect_db() as db:
                for item in created:
                    row = {**item, "id": uuid.uuid4().hex}
                    rows.append(row)
                    db.execute(
                        "INSERT INTO generated_images(id, nm_id, filename, local_path, label, prompt, cover_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (row["id"], nm_id, row["filename"], row["path"], row["label"], "", cover["cover_hash"], now),
                    )
            self.send_json(201, {
                "variants": [
                    {"id": row["id"], "label": row["label"], "index": row["index"], "image_url": f"/generated/{row['filename']}"}
                    for row in rows
                ],
                "errors": errors,
            })
        finally:
            with GENERATION_LOCK:
                GENERATING_NM_IDS.discard(nm_id)

    def create_experiment(self, body: dict) -> None:
        if not get_token("wb_content_token"):
            raise RuntimeError("Добавьте WB API токен с доступом к Контенту.")
        if not get_token("wb_promotion_token"):
            raise RuntimeError("Добавьте WB API токен с доступом к Продвижению.")
        nm_id = int(body.get("nm_id", 0))
        advert_id = int(body.get("advert_id", 0))
        if nm_id <= 0 or advert_id <= 0:
            raise ValueError("Выберите найденную активную кампанию для артикула.")
        target_views = int(body.get("target_views", 0))
        if target_views != 200 and (target_views < 1000 or target_views > 5000 or target_views % 500):
            raise ValueError("Выберите 200 или от 1 000 до 5 000 показов с шагом 500.")
        generated_ids = body.get("generated_image_ids") or []
        if len(generated_ids) != 5 or len(set(generated_ids)) != 5:
            raise ValueError("Сначала создайте все 5 вариантов фото.")

        with connect_db() as db:
            existing = db.execute("SELECT id FROM products WHERE nm_id=?", (nm_id,)).fetchone()
            if existing:
                duplicate = db.execute(
                    "SELECT id FROM experiments WHERE product_id=? AND status IN ('scheduled','active','finishing')",
                    (int(existing["id"]),),
                ).fetchone()
                if duplicate:
                    raise ValueError("Для этого товара уже запущен или запланирован тест.")

        campaigns = fetch_active_campaigns(nm_id)
        campaign = next((item for item in campaigns if item["advert_id"] == advert_id), None)
        if not campaign:
            raise ValueError("Активная кампания для этого товара не найдена. Обновите поиск по nmID.")
        if campaign["payment_type"] != "cpm":
            raise ValueError("Выберите CPM кампанию: для CPC Wildberries может не вернуть показы, нужные для порога и CTR.")
        card = fetch_card(nm_id)
        cover = ensure_product_cover(nm_id, card)
        if not cover["available"] or not cover["path"]:
            raise ValueError(
                "Исходное фото для этой карточки не сохранено. Сохраните нынешнее фото как новый контроль перед созданием теста."
            )
        generated_by_id = {}
        with connect_db() as db:
            placeholders = ",".join("?" for _ in generated_ids)
            rows = db.execute(
                f"SELECT id, filename, local_path, label, cover_hash FROM generated_images WHERE nm_id=? AND cover_hash=? AND id IN ({placeholders})",
                [nm_id, cover["cover_hash"], *generated_ids],
            ).fetchall()
            generated_by_id = {row["id"]: row for row in rows}
        if len(generated_by_id) != 5:
            raise ValueError("Не удалось найти 5 сгенерированных фото для этого товара. Сгенерируйте их заново.")
        generated_rows = [generated_by_id[item_id] for item_id in generated_ids]
        started_at = datetime.now(TZ)
        if not reserve_promotion_stats_request():
            raise RuntimeError("Сейчас действует лимит WB на запросы статистики. Попробуйте запустить тест позже.")
        baseline_views, baseline_clicks, warning = fetch_ad_metrics(
            nm_id, advert_id, started_at.date().isoformat(), started_at.date().isoformat()
        )
        if warning or baseline_views is None:
            raise RuntimeError(warning or "Wildberries пока не вернул начальную статистику показов.")
        baseline_views = int(baseline_views or 0)
        baseline_clicks = int(baseline_clicks or 0)
        now = stamp(started_at)
        experiment_id = uuid.uuid4().hex[:12]
        with connect_db() as db:
            product_id = db.execute("SELECT id FROM products WHERE nm_id=?", (nm_id,)).fetchone()
            if product_id:
                db.execute(
                    "UPDATE products SET title=?, photos_json=? WHERE id=?",
                    (card["title"], json.dumps(card["photos"]), int(product_id["id"])),
                )
                product_pk = int(product_id["id"])
            else:
                cursor = db.execute(
                    "INSERT INTO products(nm_id, title, photos_json, created_at) VALUES (?, ?, ?, ?)",
                    (nm_id, card["title"], json.dumps(card["photos"]), now),
                )
                product_pk = int(cursor.lastrowid)
            db.execute(
                """INSERT INTO experiments(id, product_id, advert_id, duration_days, target_views, original_photos_json,
                                               next_photo_number, initial_baseline_views, initial_baseline_clicks,
                                               status, created_at, updated_at)
                   VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
                (experiment_id, product_pk, advert_id, target_views, json.dumps(card["photos"]),
                 len(card["photos"]) + 1, baseline_views, baseline_clicks, now, now),
            )
            db.execute(
                "INSERT INTO variants(experiment_id, position, label, image_url, local_path) VALUES (?, 0, 'Контроль', ?, ?)",
                (experiment_id, f"/originals/{Path(cover['path']).name}", cover["path"]),
            )
            db.executemany(
                "INSERT INTO variants(experiment_id, position, label, image_url, local_path) VALUES (?, ?, ?, ?, ?)",
                [
                    (experiment_id, idx, row["label"], f"/generated/{row['filename']}", row["local_path"])
                    for idx, row in enumerate(generated_rows, start=1)
                ],
            )
            formatted_target = f"{target_views:,}".replace(",", " ")
            log_event(db, experiment_id, "info", f"Тест создан. Каждый вариант остаётся до {formatted_target} показов.")
        try:
            apply_variant(experiment_id, 0)
            with connect_db() as db:
                make_stage(
                    db,
                    experiment_id,
                    0,
                    started_at.date(),
                    baseline_views=baseline_views,
                    baseline_clicks=baseline_clicks,
                    baseline_ready=False,
                )
                db.execute(
                    "UPDATE experiments SET status='active', scheduled_start=NULL, error=NULL, updated_at=? WHERE id=?",
                    (stamp(started_at), experiment_id),
                )
                log_event(db, experiment_id, "info", "Тест запущен сразу после создания.")
        except PhotoPropagationPending:
            defer_pending_upload_check(experiment_id)
            with connect_db() as db:
                db.execute(
                    """UPDATE experiments SET status='scheduled', scheduled_start=?, error=NULL,
                       initial_baseline_views=?, initial_baseline_clicks=?, updated_at=? WHERE id=?""",
                    (stamp(started_at), int(baseline_views or 0), int(baseline_clicks or 0), stamp(), experiment_id),
                )
                log_event(
                    db,
                    experiment_id,
                    "info",
                    "WB обрабатывает контрольное фото. Тест начнётся автоматически после его публикации.",
                )
            self.send_json(201, {
                "id": experiment_id, "title": card["title"], "nm_id": nm_id,
                "target_views": target_views, "status": "scheduled", "started_at": None,
            })
            return
        except Exception as exc:
            if is_retryable_wb_failure(exc) and has_pending_wb_upload(experiment_id):
                defer_pending_upload_check(experiment_id)
                with connect_db() as db:
                    db.execute(
                        """UPDATE experiments SET status='scheduled', scheduled_start=?, error=NULL,
                           initial_baseline_views=?, initial_baseline_clicks=?, updated_at=? WHERE id=?""",
                        (stamp(started_at), int(baseline_views or 0), int(baseline_clicks or 0), stamp(), experiment_id),
                    )
                log_wb_retry_once(experiment_id, exc)
                self.send_json(201, {
                    "id": experiment_id, "title": card["title"], "nm_id": nm_id,
                    "target_views": target_views, "status": "scheduled", "started_at": None,
                })
                return
            message = public_error(exc)
            fail_experiment(experiment_id, message)
            self.send_json(201, {
                "id": experiment_id, "title": card["title"], "nm_id": nm_id,
                "target_views": target_views, "status": "error", "error": message,
            })
            return
        self.send_json(201, {
            "id": experiment_id, "title": card["title"], "nm_id": nm_id,
            "target_views": target_views, "status": "active", "started_at": stamp(started_at),
        })

    def start_experiment(self, experiment_id: str) -> None:
        if not get_token("wb_content_token") or not get_token("wb_promotion_token"):
            raise RuntimeError("Добавьте WB токены с доступом к Контенту и Продвижению.")
        with connect_db() as db:
            exp = db.execute(
                "SELECT e.*, p.nm_id FROM experiments e JOIN products p ON p.id=e.product_id WHERE e.id=?",
                (experiment_id,),
            ).fetchone()
            if not exp:
                raise ValueError("Тест не найден.")
            if exp["status"] not in ("draft", "paused"):
                raise ValueError("Тест уже запущен или находится в состоянии ошибки.")
            variants = db.execute("SELECT COUNT(*) FROM variants WHERE experiment_id=?", (experiment_id,)).fetchone()[0]
            if int(variants) < 2:
                raise ValueError("В тесте должны быть минимум два варианта.")
            control = db.execute(
                "SELECT local_path FROM variants WHERE experiment_id=? AND position=0",
                (experiment_id,),
            ).fetchone()
            if not control or not control["local_path"] or not Path(control["local_path"]).is_file():
                raise ValueError("Исходное фото не сохранено локально. Без него безопасно запустить или восстановить тест нельзя.")
            nm_id = int(exp["nm_id"])
            advert_id = int(exp["advert_id"])
            product_id = int(exp["product_id"])
            target_views = int(exp["target_views"])
            metrics_start_date = db.execute(
                "SELECT MIN(start_date) FROM stages WHERE experiment_id=?",
                (experiment_id,),
            ).fetchone()[0]
        with connect_db() as db:
            other = db.execute(
                "SELECT id FROM experiments WHERE product_id=? AND id<>? AND status IN ('scheduled','active','finishing')",
                (product_id, experiment_id),
            ).fetchone()
        if other:
            raise ValueError("Для этого товара уже идёт другой тест.")
        campaigns = fetch_active_campaigns(nm_id)
        campaign = next((item for item in campaigns if item["advert_id"] == advert_id), None)
        if not campaign:
            raise ValueError("Активная кампания для артикула не найдена. Проверьте рекламу и попробуйте снова.")
        if campaign["payment_type"] != "cpm":
            raise ValueError("Для измерения показов и CTR требуется CPM кампания.")
        started_at = datetime.now(TZ)
        if not reserve_promotion_stats_request():
            raise RuntimeError("Сейчас действует лимит WB на запросы статистики. Попробуйте запустить тест позже.")
        baseline_views, baseline_clicks, warning = fetch_ad_metrics(
            nm_id,
            advert_id,
            str(metrics_start_date or started_at.date().isoformat()),
            started_at.date().isoformat(),
        )
        if warning or baseline_views is None:
            raise RuntimeError(warning or "Wildberries пока не вернул начальную статистику показов.")
        try:
            apply_variant(experiment_id, 0)
        except PhotoPropagationPending:
            defer_pending_upload_check(experiment_id)
            with connect_db() as db:
                db.execute(
                    """UPDATE experiments SET status='scheduled', scheduled_start=?, error=NULL,
                       initial_baseline_views=?, initial_baseline_clicks=?, updated_at=? WHERE id=?""",
                    (stamp(started_at), int(baseline_views or 0), int(baseline_clicks or 0), stamp(), experiment_id),
                )
                log_event(
                    db,
                    experiment_id,
                    "info",
                    "WB обрабатывает контрольное фото. Тест продолжится автоматически после публикации.",
                )
            self.send_json(200, {"ok": True, "status": "scheduled", "started_at": None, "target_views": target_views})
            return
        except Exception as exc:
            if is_retryable_wb_failure(exc) and has_pending_wb_upload(experiment_id):
                defer_pending_upload_check(experiment_id)
                with connect_db() as db:
                    db.execute(
                        """UPDATE experiments SET status='scheduled', scheduled_start=?, error=NULL,
                           initial_baseline_views=?, initial_baseline_clicks=?, updated_at=? WHERE id=?""",
                        (stamp(started_at), int(baseline_views or 0), int(baseline_clicks or 0), stamp(), experiment_id),
                    )
                log_wb_retry_once(experiment_id, exc)
                self.send_json(200, {"ok": True, "status": "scheduled", "started_at": None, "target_views": target_views})
                return
            raise
        with connect_db() as db:
            make_stage(
                db,
                experiment_id,
                0,
                started_at.date(),
                baseline_views=int(baseline_views or 0),
                baseline_clicks=int(baseline_clicks or 0),
                baseline_ready=False,
            )
            db.execute(
                "UPDATE experiments SET status='active', scheduled_start=NULL, error=NULL, updated_at=? WHERE id=?",
                (stamp(started_at), experiment_id),
            )
            log_event(db, experiment_id, "info", "Тест запущен сразу после нажатия кнопки.")
        self.send_json(200, {"ok": True, "started_at": stamp(started_at), "target_views": target_views})

    def stop_experiment(self, experiment_id: str) -> None:
        with connect_db() as db:
            exp = db.execute(
                "SELECT e.*, p.nm_id FROM experiments e JOIN products p ON p.id=e.product_id WHERE e.id=?",
                (experiment_id,),
            ).fetchone()
            if not exp:
                raise ValueError("Тест не найден.")
            was_completed = exp["status"] == "completed"
            should_restore = exp["status"] in ("active", "error", "completed")
            stage_id = exp["current_stage_id"]
        if should_restore:
            apply_variant(experiment_id, 0)
        now = datetime.now(TZ)
        with connect_db() as db:
            stage = db.execute("SELECT * FROM stages WHERE id=?", (stage_id,)).fetchone() if stage_id else None
        if stage and stage["collection_state"] == "pending":
            close_stage(stage, now.date())
        with connect_db() as db:
            next_status = "completed" if was_completed else "paused"
            db.execute(
                "UPDATE experiments SET status=?, scheduled_start=NULL, updated_at=? WHERE id=?",
                (next_status, stamp(), experiment_id),
            )
            log_event(
                db,
                experiment_id,
                "info",
                "Исходное главное фото восстановлено после теста." if was_completed else "Тест остановлен. Восстановлено исходное главное фото.",
            )
        self.send_json(200, {"ok": True})

    def delete_experiment(self, experiment_id: str) -> None:
        with connect_db() as db:
            exp = db.execute(
                "SELECT status FROM experiments WHERE id=?",
                (experiment_id,),
            ).fetchone()
        if not exp:
            raise ValueError("Тест не найден.")

        status = exp["status"]
        restored = status in ("active", "error", "finishing")
        if restored:
            # An active or failed run may have left a generated image on the WB card.
            # Put the control image back before removing its recovery record.
            apply_variant(experiment_id, 0)

        with connect_db() as db:
            db.execute("DELETE FROM experiments WHERE id=?", (experiment_id,))
        self.send_json(200, {"ok": True, "restored_original": restored})

    def select_variant(self, experiment_id: str, body: dict) -> None:
        position = int(body.get("position", -1))
        with connect_db() as db:
            exp = db.execute("SELECT status, current_variant_index FROM experiments WHERE id=?", (experiment_id,)).fetchone()
            if not exp:
                raise ValueError("Тест не найден.")
            if exp["status"] != "completed":
                raise ValueError("Выбрать итоговое фото можно после завершения теста.")
            stages = db.execute(
                "SELECT collection_state FROM stages WHERE experiment_id=?",
                (experiment_id,),
            ).fetchall()
            if not stages or any(stage["collection_state"] not in ("done", "warning", "skipped") for stage in stages):
                raise ValueError("Итоговая статистика Wildberries ещё собирается. Попробуйте позже.")
            variant = db.execute(
                "SELECT position, label FROM variants WHERE experiment_id=? AND position=?",
                (experiment_id, position),
            ).fetchone()
            if not variant:
                raise ValueError("Такого варианта в тесте нет.")
        if position != int(exp["current_variant_index"]):
            apply_variant(experiment_id, position)
        with connect_db() as db:
            db.execute(
                "UPDATE experiments SET selected_variant_index=?, current_variant_index=?, updated_at=? WHERE id=?",
                (position, position, stamp(), experiment_id),
            )
            log_event(db, experiment_id, "info", f"Выбрано главное фото: {variant['label']}.")
        self.send_json(200, {"ok": True, "selected_variant_index": position})

    def log_message(self, format: str, *args) -> None:
        # Keep request bodies and authorization headers out of the local log.
        return


def main() -> None:
    load_credentials()
    init_db()
    recover_accepted_upload_failures()
    threading.Thread(target=scheduler_loop, name="experiment-scheduler", daemon=True).start()
    host = "127.0.0.1"
    port = int(os.environ.get("CTR_LAB_PORT", "8787"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"CTR Lab запущен: http://{host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
