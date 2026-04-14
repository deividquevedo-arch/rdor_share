"""Normalizacao para comparacao (acentos removidos, lower)."""

from __future__ import annotations

import re
import unicodedata


def norm(s: str) -> str:
    s = s.lower().strip()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s
