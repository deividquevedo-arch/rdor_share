"""Regex por token tolerante a acentos (PT), a partir de token ja normalizado (sem acento)."""

from __future__ import annotations

import re

_MAP: dict[str, str] = {
    "a": "aàáâãäåAÀÁÂÃÄÅ",
    "e": "eèéêëEÈÉÊË",
    "i": "iìíîïIÌÍÎÏ",
    "o": "oòóôõöOÒÓÔÕÖ",
    "u": "uùúûüUÙÚÛÜ",
    "c": "cçCÇ",
    "n": "nñNÑ",
}


def token_accent_regex(norm_ascii_token: str) -> str:
    """Padrao case-insensitive para um token; ``norm_ascii_token`` tipicamente saida coerente com ``norm``."""
    parts: list[str] = []
    for c in norm_ascii_token:
        cl = c.lower()
        if cl in _MAP:
            parts.append(f"[{re.escape(_MAP[cl])}]")
        elif c.isalpha():
            parts.append(re.escape(c))
        else:
            parts.append(re.escape(c))
    return "(?i)" + "".join(parts)
