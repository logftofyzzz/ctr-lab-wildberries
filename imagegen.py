"""OpenAI image edits used to create product-photo variants."""

from __future__ import annotations

import base64
import json
import os
import struct
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from tls_config import TLS_CONTEXT


MAX_SOURCE_BYTES = 24 * 1024 * 1024
MAX_RESULT_BYTES = 30 * 1024 * 1024
MODEL = "gpt-image-2.5-sunburst"

VARIANTS = [
    ("Чистая студия", "Create a clean, high-contrast studio catalog photo. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. Center the entire product, large and sharply focused, on a bright simple background with a natural soft shadow. Do not add text, badges, accessories, or claims."),
    ("Тёплый премиум", "Create a premium warm-neutral studio product photo with soft directional light and a subtle elegant surface. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. The full product must remain centered, large, and easy to recognize. No added text, badges, accessories, or claims."),
    ("Контрастный фон", "Create a polished product listing photo with a bold but tasteful solid background that contrasts with the product. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. Keep the full product centered, large, and sharply focused. No added text, badges, accessories, or claims."),
    ("Сценарий использования", "Create a realistic, minimal lifestyle product photo in a setting where this exact product would naturally be used. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. The product stays fully visible, centered, and visually dominant. Do not add parts, accessories, text, badges, or claims."),
    ("Мягкий свет", "Create a crisp editorial catalog photo with soft rim lighting, gentle depth, and a clean uncluttered background. Keep the exact same product, packaging, logo, colors, shape, proportions, and all existing label text unchanged. The full product stays centered, large, and sharply focused. No added text, badges, accessories, or claims."),
]


def _error_text(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        body = exc.read().decode("utf-8", "replace")[:800]
        try:
            payload = json.loads(body)
            error = payload.get("error", {})
            detail = error.get("message") or payload.get("message") or body
            error_code = error.get("code") or error.get("type") or ""
        except (ValueError, AttributeError):
            detail = body
            error_code = ""
        if exc.code == 429 and (
            error_code == "insufficient_quota"
            or error_code == "credit_balance_exhausted"
            or "no credits remaining" in detail.lower()
            or "credit_balance_exhausted" in detail.lower()
            or "insufficient_quota" in detail.lower()
        ):
            return (
                "OpenAI API отклонил генерацию: на API-аккаунте закончились кредиты. "
                "Пополните баланс в настройках оплаты OpenAI, затем нажмите «Сгенерировать заново»: "
                "https://platform.openai.com/settings/organization/billing/"
            )
        return f"OpenAI API вернул HTTP {exc.code}: {detail or exc.reason}"
    if isinstance(exc, URLError):
        return f"Не удалось связаться с OpenAI API: {exc.reason}"
    return str(exc)


def _download_source(url: str) -> tuple[bytes, str, str]:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    allowed = ("wbbasket.ru", "wb.ru", "wildberries.ru")
    if parsed.scheme != "https" or not any(host == domain or host.endswith("." + domain) for domain in allowed):
        raise ValueError("WB вернул неожиданный домен фотографии. Проверьте карточку товара.")
    request = Request(url, headers={"User-Agent": "CTR-Lab-WB/0.2", "Accept": "image/*"})
    with urlopen(request, timeout=30, context=TLS_CONTEXT) as response:
        content_type = response.headers.get_content_type()
        if not content_type.startswith("image/"):
            raise ValueError("Главное фото карточки не вернулось как изображение.")
        data = response.read(MAX_SOURCE_BYTES + 1)
    if len(data) > MAX_SOURCE_BYTES:
        raise ValueError("Файл исходного фото больше 24 МБ.")
    extension = {"image/png": ".png", "image/webp": ".webp", "image/jpeg": ".jpg"}.get(content_type)
    if not extension:
        raise ValueError(f"Формат исходного фото не поддерживается: {content_type}.")
    return data, content_type, extension


def _multipart_request(api_key: str, image_data: bytes, content_type: str, filename: str, prompt: str) -> dict:
    boundary = "----CTRLab" + uuid.uuid4().hex
    parts: list[bytes] = []

    def add_field(name: str, value: str) -> None:
        parts.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n".encode("utf-8")
        )

    add_field("model", MODEL)
    add_field("prompt", prompt)
    add_field("quality", "medium")
    add_field("size", "auto")
    add_field("output_format", "png")
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"image\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n".encode("utf-8")
        + image_data
        + b"\r\n"
    )
    parts.append(f"--{boundary}--\r\n".encode("ascii"))
    request = Request(
        "https://api.openai.com/v1/images/edits",
        data=b"".join(parts),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
            "User-Agent": "CTR-Lab-WB/0.2",
        },
        method="POST",
    )
    with urlopen(request, timeout=240, context=TLS_CONTEXT) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload


def generate_five_variants(source_url: str, api_key: str, target_dir: Path, nm_id: int) -> list[dict]:
    source, content_type, extension = _download_source(source_url)
    target_dir.mkdir(parents=True, exist_ok=True)
    created: list[dict] = []
    try:
        for index, (label, style_prompt) in enumerate(VARIANTS, start=1):
            prompt = (
                "Edit the supplied Wildberries main product photo into one truthful, commercially clear catalog-photo variant. "
                "Preserve the exact real product identity and all existing printed text. Change only composition, lighting, "
                "or background as requested. Do not invent features or make the product look like a different model. "
                "Use a vertical marketplace listing composition with the entire product visible. "
                + style_prompt
            )
            result = _multipart_request(api_key, source, content_type, f"wb-{nm_id}{extension}", prompt)
            images = result.get("data") or []
            if not images or not images[0].get("b64_json"):
                raise RuntimeError("OpenAI API не вернул изображение.")
            image_bytes = base64.b64decode(images[0]["b64_json"], validate=True)
            if len(image_bytes) > MAX_RESULT_BYTES:
                raise ValueError("Сгенерированное изображение превышает 30 МБ.")
            if len(image_bytes) < 24 or image_bytes[:8] != b"\x89PNG\r\n\x1a\n":
                raise ValueError("OpenAI API вернул файл в неожиданном формате.")
            width, height = struct.unpack(">II", image_bytes[16:24])
            if width < 700 or height < 900:
                raise ValueError(f"Фото {index} имеет разрешение {width}×{height}; для WB нужно минимум 700×900.")
            filename = f"{nm_id}-{uuid.uuid4().hex}.png"
            path = target_dir / filename
            path.write_bytes(image_bytes)
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
            created.append({"filename": filename, "path": str(path), "label": label, "width": width, "height": height})
        return created
    except Exception as exc:
        for item in created:
            Path(item["path"]).unlink(missing_ok=True)
        if isinstance(exc, (ValueError, RuntimeError)):
            raise
        raise RuntimeError(_error_text(exc)) from exc
