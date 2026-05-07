"""Regex tolerante a acentos por token."""

from __future__ import annotations

import re

from nlp_engine.text_pipeline.accent_pattern import token_accent_regex


def test_token_accent_regex_matches_accented_surface() -> None:
    rx = token_accent_regex("nodulo")
    assert re.search(rx, "Nódulo")
    assert re.search(rx, "nodulo")
