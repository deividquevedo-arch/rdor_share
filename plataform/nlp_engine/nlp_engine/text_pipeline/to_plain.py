"""Conversao RTF/HTML/plain -> texto limpo (paridade com notebook biliar, T01.1)."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence
from typing import Match

import ftfy
from striprtf.striprtf import rtf_to_text as striprtf_to_text

from nlp_engine.text_pipeline.boilerplate import (
    drop_boilerplate_lines,
    strip_trailing_line_patterns,
)
from nlp_engine.text_pipeline.footer import remove_final_laudo
from nlp_engine.text_pipeline.html_plain import html_to_plain
from nlp_engine.text_pipeline.rtf_fallback import rtf_to_text_fallback

_UNITS = frozenset(
    {
        "mg",
        "g",
        "kg",
        "mcg",
        "ug",
        "μg",
        "ml",
        "l",
        "dl",
        "cl",
        "mm",
        "cm",
        "m",
        "km",
        "bpm",
        "mmhg",
        "pa",
        "ua",
        "u/l",
        "ui",
        "iu",
        "u",
        "meq",
        "mol",
        "mmol",
        "nmol",
        "na",
        "k",
        "cl",
        "co2",
        "o2",
        "sat",
        "spo2",
        "fio2",
        "cr",
        "vdrl",
        "ph",
    }
)


def _normalize_literal_escapes(raw: str) -> str:
    if "\\r\\n" in raw:
        raw = raw.replace("\\r\\n", "\n")
    if "\\n" in raw:
        raw = re.sub(r"\\n(?=[\s{\\]|$)", "\n", raw)
    return raw


def _detect_format(raw: str) -> tuple[bool, bool]:
    head = raw[:512].lstrip()
    is_rtf = head.startswith("{\\rtf") or "\\rtf" in head.lower()[:128]
    is_html = bool(
        re.search(r"</?(html|head|body|div|p|span|br|table|tr|td|img)\b", raw[:2000], re.I)
    )
    return is_rtf, is_html


def _convert_rtf(raw: str) -> str:
    try:
        try:
            import pypandoc

            return pypandoc.convert_text(raw, "plain", format="rtf", extra_args=["--wrap=none"])
        except Exception:
            return striprtf_to_text(raw)
    except Exception:
        return rtf_to_text_fallback(raw)


def _convert_html_or_raw(raw: str, is_html: bool) -> str:
    if is_html:
        return html_to_plain(raw)
    return raw


def _apply_ftfy_and_nfkc(txt: str) -> str:
    try:
        txt = ftfy.fix_text(txt)
    except Exception:
        pass
    txt = txt.replace("\u200b", "").replace("\u200c", "").replace("\xa0", " ")
    return unicodedata.normalize("NFKC", txt)


def _collapse_caps_all_lines(txt: str) -> str:
    rx_caps_line = re.compile(r"^(?:[A-ZÁ-Ú]\s+){2,}[A-ZÁ-Ú]$", re.UNICODE)
    rx_caps_inline = re.compile(r"(?<!\w)([A-ZÁ-Ú](?:\s+[A-ZÁ-Ú]){2,})(?!\w)", re.UNICODE)

    def _collapse_caps(line: str) -> str:
        if rx_caps_line.match(line.strip()):
            return line.replace(" ", "")
        return rx_caps_inline.sub(lambda m: m.group(1).replace(" ", ""), line)

    return "\n".join(_collapse_caps(ln) for ln in txt.splitlines())


def _build_ocr_prefix_regexes() -> tuple[re.Pattern[str], re.Pattern[str], re.Pattern[str]]:
    unit_alt = "|".join(sorted(_UNITS, key=len, reverse=True))
    rx_dosage = re.compile(rf"(?i)^\d{{1,3}}\s*(?:{unit_alt})\b")
    rx_flow = re.compile(r"(?i)^\d{1,3}\s*[lL]\s*/\s*min\b")
    rx_ocr = re.compile(r"(?i)\b(\d{1,2})([A-Za-zÁ-Úá-ú]{3,})\b")
    return rx_dosage, rx_flow, rx_ocr


def _strip_digit_prefix(
    m: Match[str], rx_dosage: re.Pattern[str], rx_flow: re.Pattern[str]
) -> str:
    tok = m.group(0)
    if rx_dosage.match(tok) or rx_flow.match(tok) or m.group(2).lower() in _UNITS:
        return tok
    return m.group(2)


def _strip_ocr_prefixes(txt: str) -> str:
    rx_dosage, rx_flow, rx_ocr = _build_ocr_prefix_regexes()
    return rx_ocr.sub(lambda m: _strip_digit_prefix(m, rx_dosage, rx_flow), txt)


def _fix_intra_word_accents_loop(txt: str, rounds: int = 4) -> str:
    acc = "ÁÉÍÓÚÂÊÎÔÛÃÕÄËÏÖÜáéíóúâêîôûãõäëïöüÇç"
    for _ in range(rounds):
        before = txt
        txt = re.sub(rf"(?i)\b([{acc}])\s+([A-Za-z]{{1,30}})\b", r"\1\2", txt)
        txt = re.sub(rf"(?i)\b([A-Za-z]{{1,30}})\s+([{acc}][A-Za-z]{{1,5}})\b", r"\1\2", txt)
        txt = re.sub(rf"(?i)\b([A-Za-z])\s+([{acc}])\s+([A-Za-z])\b", r"\1\2\3", txt)
        txt = re.sub(r"(?i)çã\s+o\b", "ção", txt)
        txt = re.sub(r"(?i)çõ\s+es\b", "ções", txt)
        txt = re.sub(r"(?i)ã\s+o\b", "ão", txt)
        txt = re.sub(r"(?i)õ\s+es\b", "ões", txt)
        txt = re.sub(rf"(?i)\b([A-Za-z{acc}]{{1,40}})\s*-\s*([A-Za-z{acc}]{{1,40}})\b", r"\1-\2", txt)
        txt = re.sub(rf"\b([A-Za-z]{{1,4}})\s+(?=[{acc}])", r"\1", txt)
        txt = re.sub(rf"\b([{acc}])\s+(?=[A-Za-z]{{1,3}}\b)", r"\1", txt)
        if txt == before:
            break
    return txt


def _final_spacing_and_punct(txt: str) -> str:
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\s*\n\s*", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    txt = re.sub(r"\s+([.,;:])", r"\1", txt)
    txt = re.sub(r"([\(\[\{])\s+", r"\1", txt)
    txt = re.sub(r"\s+([\)\]\}])", r"\1", txt)
    return txt


def to_plain(s: str, *, trailing_line_patterns: Sequence[str] | None = None) -> str:
    if not s:
        return ""

    raw = s.replace("\r\n", "\n").replace("\r", "\n")
    raw = _normalize_literal_escapes(raw)

    is_rtf, is_html = _detect_format(raw)
    txt = raw
    try:
        if is_rtf:
            txt = _convert_rtf(raw)
        else:
            txt = _convert_html_or_raw(raw, is_html)
    except Exception:
        txt = raw

    txt = _apply_ftfy_and_nfkc(txt)
    txt = _collapse_caps_all_lines(txt)
    txt = _strip_ocr_prefixes(txt)
    txt = _fix_intra_word_accents_loop(txt)
    txt = drop_boilerplate_lines(txt)
    txt = _final_spacing_and_punct(txt)
    txt = remove_final_laudo(txt).strip()
    if trailing_line_patterns:
        txt = strip_trailing_line_patterns(txt, trailing_line_patterns).strip()
    return txt
