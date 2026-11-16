"""Verified TLS configuration using the operating system CA bundle when available."""

from __future__ import annotations

import os
import ssl
from pathlib import Path


def build_tls_context() -> ssl.SSLContext:
    # Some python.org macOS builds point OpenSSL at a CA bundle that is not
    # installed. Prefer an explicitly configured bundle, then macOS's bundle.
    configured = os.environ.get("SSL_CERT_FILE", "").strip()
    default = ssl.get_default_verify_paths()
    candidates = [
        configured or None,
        default.cafile,
        "/etc/ssl/cert.pem",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return ssl.create_default_context(cafile=candidate)

    try:
        import certifi
    except ImportError:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=certifi.where())


TLS_CONTEXT = build_tls_context()
