from __future__ import annotations

import ssl

import certifi


def default_ssl_context() -> ssl.SSLContext:
    """Use a consistent Mozilla CA bundle across Linux and framework Python builds."""

    return ssl.create_default_context(cafile=certifi.where())
