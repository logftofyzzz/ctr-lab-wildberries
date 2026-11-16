"""Nano Banana Pro image edits through the Kie.ai API."""

from __future__ import annotations

import json
import base64
import binascii
import mimetypes
import os
import struct
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from tls_config import TLS_CONTEXT


API_BASE = "https://api.kie.ai"
UPLOAD_BASE = "https://kieai.redpandaai.co"
MODEL = "nano-banana-pro"
CREDITS_PER_IMAGE = 18
MAX_RESULT_BYTES = 30 * 1024 * 1024
MAX_POLL_SECONDS = 600
MAX_REFERENCE_IMAGES = 4
MAX_REFERENCE_IMAGE_BYTES = 2 * 1024 * 1024

DESIGN_BRIEF = (
    "Act as a performance-focused marketplace listing art director. Design for a small mobile search thumbnail: "
    "make the real product immediately recognizable, prominent, legible, and visually separated from its background. "
    "Treat each output as a separate CTR hypothesis; vary the requested visual idea while keeping the product truthful. "
    "Never invent product features, materials, accessories, certifications, discounts, ratings, or claims. "
    "Preserve the exact item, packaging, logo, colors, shape, proportions, and existing printed text. "
    "Keep the composition clean and compliant for a Wildberries main image."
)

VARIANTS = [
    ("Чистая студия", "Create a clean, high-contrast studio catalog photo. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. Center the entire product, large and sharply focused, on a bright simple background with a natural soft shadow. Do not add text, badges, accessories, or claims."),
    ("Тёплый премиум", "Create a premium warm-neutral studio product photo with soft directional light and a subtle elegant surface. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. The full product must remain centered, large, and easy to recognize. No added text, badges, accessories, or claims."),
    ("Контрастный фон", "Create a polished product listing photo with a bold but tasteful solid background that contrasts with the product. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. Keep the full product centered, large, and sharply focused. No added text, badges, accessories, or claims."),
    ("Сценарий использования", "Create a realistic, minimal lifestyle product photo in a setting where this exact product would naturally be used. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. The product stays fully visible, centered, and visually dominant. Do not add parts, accessories, text, badges, or claims."),
    ("Мягкий свет", "Create a crisp editorial catalog photo with soft rim lighting, gentle depth, and a clean uncluttered background. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. The full product stays centered, large, and sharply focused. No added text, badges, accessories, or claims."),
]


def _request_json(url: str, api_key: str, payload: dict | None = None, timeout: int = 30) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "CTR-Lab-WB/0.3",
        },
        method="POST" if payload is not None else "GET",
    )
    try:
        with urlopen(request, timeout=timeout, context=TLS_CONTEXT) as response:
            body = response.read()
    except HTTPError as exc:
        details = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"Kie.ai вернул HTTP {exc.code}: {details or exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Не удалось связаться с Kie.ai: {exc.reason}") from exc
    try:
        result = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise RuntimeError("Kie.ai вернул ответ в неожиданном формате.") from exc
    code = result.get("code")
    if code not in (None, 200, "200"):
        message = str(result.get("msg") or result.get("message") or "Запрос отклонён")
        if str(code) == "402":
            raise RuntimeError("В Kie.ai недостаточно кредитов для генерации.")
        if str(code) == "401":
            raise RuntimeError("Kie.ai не принял ключ. Проверьте или перевыпустите ключ API.")
        raise RuntimeError(f"Kie.ai: {message} (код {code}).")
    return result


def get_credit_balance(api_key: str) -> int:
    result = _request_json(f"{API_BASE}/api/v1/chat/credit", api_key, timeout=20)
    balance = result.get("data")
    if isinstance(balance, dict):
        balance = balance.get("credits", balance.get("balance"))
    try:
        return int(balance)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Kie.ai вернул баланс в неожиданном формате.") from exc


def _validate_wb_url(source_url: str) -> None:
    parsed = urlparse(source_url)
    host = (parsed.hostname or "").lower()
    allowed = ("wbbasket.ru", "wb.ru", "wildberries.ru")
    if parsed.scheme != "https" or not any(host == domain or host.endswith("." + domain) for domain in allowed):
        raise ValueError("WB вернул неожиданный домен фотографии. Проверьте карточку товара.")


def _upload_source(source_url: str, api_key: str, nm_id: int) -> str:
    _validate_wb_url(source_url)
    extension = Path(urlparse(source_url).path).suffix.lower()
    if extension not in {".jpg", ".jpeg", ".png", ".webp"}:
        extension = ".jpg"
    result = _request_json(
        f"{UPLOAD_BASE}/api/file-url-upload",
        api_key,
        {
            "fileUrl": source_url,
            "uploadPath": "images/wildberries",
            "fileName": f"wb-{nm_id}{extension}",
        },
        timeout=60,
    )
    file_info = result.get("data") or {}
    download_url = file_info.get("downloadUrl")
    if not download_url:
        raise RuntimeError("Kie.ai не вернул ссылку на загруженное фото товара.")
    return str(download_url)


def _upload_source_file(source_path: str, api_key: str, nm_id: int) -> str:
    path = Path(source_path)
    if not path.is_file():
        raise RuntimeError("Не найден локально сохранённый файл исходного фото.")
    image_data = path.read_bytes()
    if not image_data or len(image_data) > 32 * 1024 * 1024:
        raise RuntimeError("Размер локального исходного фото не подходит для передачи в Kie.ai.")
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    boundary = "----CTRLabKie" + uuid.uuid4().hex
    fields = [
        ("uploadPath", "images/wildberries"),
        ("fileName", f"wb-{nm_id}-{uuid.uuid4().hex}{path.suffix or '.img'}"),
    ]
    chunks: list[bytes] = []
    for name, value in fields:
        chunks.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n".encode("utf-8")
        )
    chunks.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{path.name}\"\r\n"
        f"Content-Type: {mime}\r\n\r\n".encode("utf-8")
        + image_data
        + b"\r\n"
    )
    chunks.append(f"--{boundary}--\r\n".encode("ascii"))
    request = Request(
        f"{UPLOAD_BASE}/api/file-stream-upload",
        data=b"".join(chunks),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
            "User-Agent": "CTR-Lab-WB/0.3",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=60, context=TLS_CONTEXT) as response:
            result = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, ValueError, UnicodeDecodeError) as exc:
        raise RuntimeError("Не удалось передать сохранённое исходное фото в Kie.ai.") from exc
    code = result.get("code")
    if code not in (None, 200, "200"):
        raise RuntimeError(f"Kie.ai отклонил исходное фото: {result.get('msg') or code}.")
    download_url = (result.get("data") or {}).get("downloadUrl")
    if not download_url:
        raise RuntimeError("Kie.ai не вернул ссылку на загруженное исходное фото.")
    return str(download_url)


def _upload_reference_images(reference_images: list[dict], api_key: str, nm_id: int) -> list[str]:
    if not isinstance(reference_images, list):
        raise ValueError("Список фото конкурентов передан в неверном формате.")
    if len(reference_images) > MAX_REFERENCE_IMAGES:
        raise ValueError(f"Можно добавить не более {MAX_REFERENCE_IMAGES} фото конкурентов.")
    uploaded: list[str] = []
    for index, reference in enumerate(reference_images, start=1):
        mime = str(reference.get("mime") or "")
        if mime not in {"image/jpeg", "image/png", "image/webp"}:
            raise ValueError("Фото конкурента должно быть JPG, PNG или WebP.")
        encoded = str(reference.get("data") or "")
        if encoded.startswith("data:"):
            _, separator, encoded = encoded.partition(",")
            if not separator:
                raise ValueError("Не удалось прочитать фото конкурента.")
        try:
            image_data = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("Не удалось прочитать фото конкурента.") from exc
        if not image_data or len(image_data) > MAX_REFERENCE_IMAGE_BYTES:
            raise ValueError("Каждое фото конкурента должно быть не больше 2 МБ.")
        if mime == "image/jpeg" and not image_data.startswith(b"\xff\xd8\xff"):
            raise ValueError("Файл конкурента не похож на JPG.")
        if mime == "image/png" and not image_data.startswith(b"\x89PNG\r\n\x1a\n"):
            raise ValueError("Файл конкурента не похож на PNG.")
        if mime == "image/webp" and not (image_data.startswith(b"RIFF") and image_data[8:12] == b"WEBP"):
            raise ValueError("Файл конкурента не похож на WebP.")

        extension = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}[mime]
        boundary = "----CTRLabKie" + uuid.uuid4().hex
        chunks = [
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"uploadPath\"\r\n\r\nimages/wildberries/references\r\n".encode("utf-8"),
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"fileName\"\r\n\r\nwb-{nm_id}-competitor-{index}{extension}\r\n".encode("utf-8"),
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"competitor-{index}{extension}\"\r\nContent-Type: {mime}\r\n\r\n".encode("utf-8")
            + image_data
            + b"\r\n",
            f"--{boundary}--\r\n".encode("ascii"),
        ]
        request = Request(
            f"{UPLOAD_BASE}/api/file-stream-upload",
            data=b"".join(chunks),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "User-Agent": "CTR-Lab-WB/0.3",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=60, context=TLS_CONTEXT) as response:
                result = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, ValueError, UnicodeDecodeError) as exc:
            raise RuntimeError("Не удалось передать фото конкурента в Kie.ai.") from exc
        if result.get("code") not in (None, 200, "200"):
            raise RuntimeError(f"Kie.ai отклонил фото конкурента: {result.get('msg') or result.get('code')}.")
        download_url = (result.get("data") or {}).get("downloadUrl")
        if not download_url:
            raise RuntimeError("Kie.ai не вернул ссылку на фото конкурента.")
        uploaded.append(str(download_url))
    return uploaded


def _create_task(
    api_key: str,
    image_url: str,
    nm_id: int,
    label: str,
    style_prompt: str,
    additional_instruction: str = "",
    reference_urls: list[str] | None = None,
) -> str:
    prompt = (
        DESIGN_BRIEF + " "
        "Image 1 is the exact product to edit. Any following images are competitor examples for high-level visual research only. "
        "Use them only to identify broad patterns in framing, contrast, color balance, lighting, and visual hierarchy. "
        "Do not copy a competitor's product, brand, logo, text, badges, or exact composition. "
        "Edit the supplied Wildberries main product photo into one truthful, commercially clear catalog-photo variant. "
        "Preserve the exact real product identity and all existing printed text. Change only composition, lighting, "
        "or background as requested. Do not invent features or make the product look like a different model. "
        "Use a vertical 3:4 marketplace listing composition with the entire product visible and comfortably inside the frame. "
        + style_prompt
    )
    if additional_instruction:
        prompt += (
            " Apply this additional user direction only to the composition, lighting, or background; "
            "do not change the real product identity, geometry, color, material, accessories, or printed text: "
            + additional_instruction
        )
    result = _request_json(
        f"{API_BASE}/api/v1/jobs/createTask",
        api_key,
        {
            "model": MODEL,
            "input": {
                "prompt": prompt,
                "image_input": [image_url, *(reference_urls or [])],
                "aspect_ratio": "3:4",
                "resolution": "1K",
                "output_format": "png",
            },
        },
        timeout=45,
    )
    task_id = (result.get("data") or {}).get("taskId")
    if not task_id:
        raise RuntimeError(f"Kie.ai не вернул задачу для варианта «{label}».")
    return str(task_id)


def _result_urls(value: object) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    if not isinstance(value, dict):
        return []
    for key in ("resultUrls", "result_urls", "urls"):
        urls = value.get(key)
        if isinstance(urls, list):
            return [str(item) for item in urls if isinstance(item, str)]
    response = value.get("response")
    if isinstance(response, dict):
        return _result_urls(response)
    return []


def _wait_for_result(api_key: str, task_id: str) -> str:
    started = time.monotonic()
    delay = 3
    while time.monotonic() - started < MAX_POLL_SECONDS:
        result = _request_json(
            f"{API_BASE}/api/v1/jobs/recordInfo?{urlencode({'taskId': task_id})}",
            api_key,
            timeout=30,
        )
        info = result.get("data") or {}
        state = str(info.get("state") or "").lower()
        if state == "success":
            urls = _result_urls(info.get("resultJson")) or _result_urls(info)
            if not urls:
                raise RuntimeError("Задача завершилась, но Kie.ai не вернул ссылку на фото.")
            return urls[0]
        if state == "fail":
            reason = info.get("failMsg") or info.get("failCode") or "задача завершилась с ошибкой"
            raise RuntimeError(f"Kie.ai: {reason}")
        time.sleep(delay)
        delay = min(10, delay + 2)
    raise RuntimeError("Kie.ai не завершил генерацию за 10 минут.")


def _download_generated(url: str) -> bytes:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    allowed = ("redpandaai.co", "aiquickdraw.com", "kie.ai")
    if parsed.scheme != "https" or not any(host == domain or host.endswith("." + domain) for domain in allowed):
        raise RuntimeError("Kie.ai вернул ссылку на файл с неожиданного домена.")
    request = Request(url, headers={"Accept": "image/png,image/*", "User-Agent": "CTR-Lab-WB/0.3"})
    try:
        with urlopen(request, timeout=45, context=TLS_CONTEXT) as response:
            if not response.headers.get_content_type().startswith("image/"):
                raise RuntimeError("Kie.ai вернул результат не в формате изображения.")
            data = response.read(MAX_RESULT_BYTES + 1)
    except (HTTPError, URLError) as exc:
        raise RuntimeError("Не удалось скачать готовое фото из Kie.ai.") from exc
    if len(data) > MAX_RESULT_BYTES:
        raise RuntimeError("Сгенерированное изображение превышает 30 МБ.")
    if len(data) < 24 or data[:8] != b"\x89PNG\r\n\x1a\n":
        raise RuntimeError("Kie.ai вернул файл не в формате PNG.")
    width, height = struct.unpack(">II", data[16:24])
    if width < 700 or height < 933:
        raise RuntimeError(f"Kie.ai вернул фото {width}×{height}; для WB нужно минимум 700×933.")
    # Kie.ai can round a requested 3:4 canvas by a few pixels (for example,
    # 896×1200). Accept that small rounding drift; reject materially different
    # aspect ratios so non-portrait images never enter the WB test.
    if abs((width / height) - (3 / 4)) > 0.005:
        raise RuntimeError(f"Kie.ai вернул фото {width}×{height} вместо портретного формата 3:4 для карточки WB. Вариант не сохранён; попробуйте сгенерировать его ещё раз.")
    return data


def _generate_one(
    api_key: str,
    image_url: str,
    nm_id: int,
    index: int,
    item: tuple[str, str],
    target_dir: Path,
    additional_instruction: str = "",
    reference_urls: list[str] | None = None,
) -> dict:
    label, style_prompt = item
    task_id = _create_task(api_key, image_url, nm_id, label, style_prompt, additional_instruction, reference_urls)
    result_url = _wait_for_result(api_key, task_id)
    image_bytes = _download_generated(result_url)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{nm_id}-{uuid.uuid4().hex}.png"
    path = target_dir / filename
    path.write_bytes(image_bytes)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return {
        "index": index,
        "filename": filename,
        "path": str(path),
        "label": label,
        "width": struct.unpack(">I", image_bytes[16:20])[0],
        "height": struct.unpack(">I", image_bytes[20:24])[0],
    }


def regenerate_variant(
    source_url: str,
    api_key: str,
    target_dir: Path,
    nm_id: int,
    label: str,
    mode: str,
    additional_instruction: str = "",
    source_path: str | None = None,
    reference_images: list[dict] | None = None,
) -> dict:
    item = next((variant for variant in VARIANTS if variant[0] == label), None)
    if item is None:
        raise ValueError("Выберите один из пяти вариантов фото.")
    instruction = additional_instruction.strip()
    if len(instruction) > 1500:
        raise ValueError("Дополнительная правка слишком длинная. Ограничение — 1 500 символов.")
    if mode == "replace":
        instruction = (
            "Create a fresh and clearly different execution of this same photo concept. "
            "Use a distinct composition, background, and lighting from the previous result while preserving the exact product."
        )
    elif mode == "edit":
        if not instruction:
            raise ValueError("Напишите, что изменить или дополнить в промте.")
    else:
        raise ValueError("Выберите действие: заменить вариант или применить правку.")

    credits = get_credit_balance(api_key)
    if credits < CREDITS_PER_IMAGE:
        raise RuntimeError(
            f"В Kie.ai недостаточно кредитов: доступно {credits}, для одного фото ориентировочно нужно {CREDITS_PER_IMAGE}."
        )
    image_url = _upload_source_file(source_path, api_key, nm_id) if source_path else _upload_source(source_url, api_key, nm_id)
    reference_urls = _upload_reference_images(reference_images or [], api_key, nm_id)
    index = VARIANTS.index(item) + 1
    return _generate_one(api_key, image_url, nm_id, index, item, target_dir, instruction, reference_urls)


def generate_variants(
    source_url: str,
    api_key: str,
    target_dir: Path,
    nm_id: int,
    completed_labels: list[str] | None = None,
    source_path: str | None = None,
    reference_images: list[dict] | None = None,
) -> tuple[list[dict], list[str]]:
    completed = set(completed_labels or [])
    missing = [(i, item) for i, item in enumerate(VARIANTS, start=1) if item[0] not in completed]
    if not missing:
        return [], []
    credits = get_credit_balance(api_key)
    required = len(missing) * CREDITS_PER_IMAGE
    if credits < required:
        raise RuntimeError(
            f"В Kie.ai недостаточно кредитов: доступно {credits}, для {len(missing)} фото ориентировочно нужно {required}."
        )
    image_url = _upload_source_file(source_path, api_key, nm_id) if source_path else _upload_source(source_url, api_key, nm_id)
    reference_urls = _upload_reference_images(reference_images or [], api_key, nm_id)
    created: list[dict] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(5, len(missing))) as executor:
        futures = {
            executor.submit(_generate_one, api_key, image_url, nm_id, index, item, target_dir, "", reference_urls): item[0]
            for index, item in missing
        }
        for future in as_completed(futures):
            label = futures[future]
            try:
                created.append(future.result())
            except Exception as exc:
                errors.append(f"{label}: {exc}")
    created.sort(key=lambda item: item["index"])
    errors.sort()
    return created, errors
