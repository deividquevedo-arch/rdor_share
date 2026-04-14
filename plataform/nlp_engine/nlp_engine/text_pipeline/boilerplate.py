"""Remocao de boilerplate RTF/HTML e linhas de ruido OCR (Grupo 1b consolidado, T01.4).

SPEC:
- Entrada: texto plano ja convertido (apos RTF/HTML e correcoes basicas).
- Saida: texto sem linhas-meta de fonte/gerador e sem avisos de sistema por linha.
- Nao faz: `remove_final_laudo` (fica em `footer.py`); matching clinico de orgaos.

Padroes de linha final (assinaturas etc.) opcionais via lista de regex — injetada pelo notebook.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from nlp_engine.text_pipeline.norm import norm

_FONT_WORDS = frozenset(
    {
        "times",
        "times new roman",
        "arial",
        "calibri",
        "cambria",
        "courier",
        "courier new",
        "helvetica",
        "symbol",
        "wingdings",
        "opensymbol",
        "ms mincho",
        "simsun",
        "century",
        "cambria math",
    }
)

_HEADING_MANY_RX = re.compile(r"(?:[^;]{1,40};\s*){5,}")
_RTF_META_TOKENS = (
    "fonttbl",
    "colortbl",
    "stylesheet",
    "generator",
    "heading",
    "listoverridetable",
    "listtable",
)
_RX_FONT_FAMILY_FRAGMENT = re.compile(r"(arial|times|calibri|courier|symbol|wingdings)", re.I)
_RX_RTFISH_SEMICOLON = re.compile(
    r"\b(font|heading|colortbl|stylesheet|style)\b", re.I
)
_RX_CREATED_META = re.compile(r"(created by|generator|html\s*to\s*rtf|jword)", re.I)
_RX_HEADING_ENUM = re.compile(r"(?:heading\s*\d+\s*;\s*){2,}heading\s*\d+\s*;?", re.I)
_RX_PUNCT_ONLY = re.compile(r"[,.\-–—\s]+")
_RX_INCOMPLETE_VIEW = re.compile(
    r"laudo\s+pode\s+nao\s+estar\s+completo\s+na\s+visualiza\w+\s+em\b"
)


def _inline_boilerplate_subs(ln: str) -> str:
    ln = re.sub(
        r"(?i)\blaudo\s+(?:gerado|liberado)\s+por\s+um?\s*sistema\s+especialista\b\.?",
        " ",
        ln,
    )
    ln = re.sub(
        r"(?i)\bpara\s+visualiza\w*\s+(?:o\s+conte[uú]do\s+do\s+)?laudo\b.?"
        r"\b(?:acesse|acessar)\b.?\b(?:viewer|op[cç][aã]o\s+imagem|imagem\s+dispon[ií]vel)\b"
        r"\.?",
        " ",
        ln,
    )
    return re.sub(
        r"(?i)\blaudo\s+pode\s+na[oã]\s+estar\s+completo\s+na\s+visualiza\w+\s+em\s+"
        r"(?:rtf|imagem)\b\.?",
        " ",
        ln,
    )


def _drop_line_semantic(s: str, sl: str, ns: str) -> bool:
    if s.count(";") >= 3:
        hits = sum(1 for w in _FONT_WORDS if w in sl)
        if hits >= 1 or _RX_RTFISH_SEMICOLON.search(sl):
            return True
        if _HEADING_MANY_RX.fullmatch(s):
            return True

    if _RX_HEADING_ENUM.fullmatch(sl):
        return True
    if _RX_CREATED_META.search(sl):
        return True
    if _RX_PUNCT_ONLY.fullmatch(s):
        return True

    if "laudo" in ns and "visualiza" in ns and (
        "rtf" in ns or "imagem" in ns or "viewer" in ns or "sistema" in ns or "especialista" in ns
    ):
        return True
    if _RX_INCOMPLETE_VIEW.search(ns):
        return True

    return ";" in s and sum(1 for w in _FONT_WORDS if w in sl) >= 2 and len(s) <= 160


def _drop_line_rtf_shreds(s: str, sl: str) -> bool:
    if any(tok in sl for tok in _RTF_META_TOKENS):
        return True
    return bool(s.count(";") >= 2 and _RX_FONT_FAMILY_FRAGMENT.search(sl))


def drop_boilerplate_lines(text: str) -> str:
    """Remove linhas e trechos inline tipicos de RTF/HTML export e avisos de visualizacao."""
    if not text:
        return ""

    out: list[str] = []
    for raw_in in text.splitlines():
        ln = _inline_boilerplate_subs(raw_in)
        s = ln.strip()
        sl = s.lower()

        if not s:
            out.append(ln)
            continue
        if _drop_line_rtf_shreds(s, sl):
            continue
        if _drop_line_semantic(s, sl, norm(s)):
            continue

        out.append(ln)

    return "\n".join(out).strip()


def strip_trailing_line_patterns(text: str, patterns: Sequence[str]) -> str:
    """Remove linhas consecutivas no final que casam qualquer regex em `patterns`."""
    if not text or not patterns:
        return text

    compiled = [re.compile(p, re.IGNORECASE) for p in patterns if p.strip()]
    if not compiled:
        return text

    lines = text.splitlines()
    i = len(lines)
    while i > 0:
        line = lines[i - 1].strip()
        if not line:
            i -= 1
            continue
        if any(c.search(line) for c in compiled):
            i -= 1
        else:
            break

    return "\n".join(lines[:i]).rstrip()
