"""Segmentacao por cabecalhos NOME: (paridade notebook biliar, S01 T01.2).

SPEC (resumo):
- Input: texto plano (pos-to_plain); lista de organ ids alvo; mapa organ_id -> lista de aliases
  (vindo do YAML via notebook; sem aliases clinicos hardcoded neste modulo).
- Output: blocos com organ, start, end, text, header_norm; fecha no proximo cabecalho global.
- Nao faz: fallback por ancora spaCy (ver ``anchors.organ_anchors`` / ``segment_by_organs``).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from nlp_engine.text_pipeline.norm import norm

# Mesmo padrao do legado: linha com bullet opcional, NOME:, resto opcional na mesma linha.
HEADER_RX = re.compile(
    r"^\s*[-•]?\s*([A-ZÁ-Úa-zá-ú/ ]{2,})\s*:\s*",
    re.UNICODE,
)


def line_spans(text: str) -> list[tuple[int, int, str]]:
    spans: list[tuple[int, int, str]] = []
    off = 0
    for line in text.splitlines(True):
        spans.append((off, off + len(line), line))
        off += len(line)
    return spans


def norm_header_name(s: str) -> str:
    return norm(s.replace(":", "").replace("-", " ").strip())


def list_header_positions(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for start, _end, line in line_spans(text):
        m = HEADER_RX.match(line)
        if m:
            out.append((start, norm_header_name(m.group(1))))
    return out


def _wanted_phrases(
    target_organs: list[str],
    header_aliases: Mapping[str, Iterable[str]],
) -> set[str]:
    wanted: set[str] = set()
    for o in target_organs:
        aliases = header_aliases.get(o)
        if aliases is None:
            wanted.add(o)
        else:
            wanted.update(aliases)
    return wanted


def _matches_wanted(header_norm: str, wanted_raw: set[str]) -> bool:
    return any(header_norm.startswith(norm_header_name(w)) for w in wanted_raw)


def _map_organ(
    header_norm: str,
    target_organs: list[str],
    header_aliases: Mapping[str, Iterable[str]],
) -> str | None:
    for o in target_organs:
        aliases = header_aliases.get(o, (o,))
        if any(header_norm.startswith(norm_header_name(w)) for w in aliases):
            return o
    return None


def extract_section_lines(text: str, header_name: str) -> list[str]:
    """Linhas da secao `header_name` ate o proximo cabecalho (mesma semantica do legado)."""
    lines_sp = line_spans(text)
    headers: list[tuple[int, str]] = []
    for s, e, line in lines_sp:
        m = HEADER_RX.match(line)
        if m:
            headers.append((s, norm_header_name(m.group(1))))
    if not headers:
        return []
    target = norm_header_name(header_name)
    starts = [s for s, name in headers if name.startswith(target)]
    if not starts:
        return []
    s0 = starts[0]
    nexts = [S for S, _ in headers if S > s0]
    s1 = nexts[0] if nexts else len(text)
    sub = text[s0:s1]
    return [ln.strip() for _, _, ln in line_spans(sub)]


def segment_by_headers_plain(
    text: str,
    target_organs: list[str],
    header_aliases: Mapping[str, Iterable[str]],
) -> list[dict[str, Any]]:
    """Delimita blocos por cabecalhos; mesmo criterio do `segment_by_headers` do legado (sem spaCy)."""
    if not text or not target_organs:
        return []

    all_headers = list_header_positions(text)
    if not all_headers:
        return []

    wanted_raw = _wanted_phrases(target_organs, header_aliases)
    organ_headers = [(s, name) for s, name in all_headers if _matches_wanted(name, wanted_raw)]
    if not organ_headers:
        return []

    all_sorted = sorted(all_headers, key=lambda x: x[0])
    blocks: list[dict[str, Any]] = []
    for s, name in sorted(organ_headers, key=lambda x: x[0]):
        next_starts = [S for S, _ in all_sorted if S > s]
        end = next_starts[0] if next_starts else len(text)
        if end <= s:
            continue
        mapped = _map_organ(name, target_organs, header_aliases)
        blocks.append(
            {
                "organ": mapped or target_organs[0],
                "start": s,
                "end": end,
                "text": text[s:end],
                "header_norm": name,
            }
        )
    return blocks
