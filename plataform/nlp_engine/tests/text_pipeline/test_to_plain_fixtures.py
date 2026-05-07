"""Regressao com fixtures sinteticas (sem PHI); baseline do modulo, nao paridade byte-a-byte com notebook."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from nlp_engine.text_pipeline import to_plain

_FIXTURES = Path(__file__).parent / "fixtures"


def _load_pair(base: str) -> tuple[str, str]:
    raw = (_FIXTURES / f"{base}_input.txt").read_text(encoding="utf-8")
    expected = (_FIXTURES / f"{base}_expected.txt").read_text(encoding="utf-8").rstrip("\r\n")
    return raw, expected


def _norm_sentence(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())


@pytest.mark.parametrize(
    "base",
    ["plain", "html", "rtf", "negacao"],
)
def test_to_plain_fixture_matches_expected(base: str) -> None:
    raw, expected = _load_pair(base)
    assert _norm_sentence(to_plain(raw)) == _norm_sentence(expected)
