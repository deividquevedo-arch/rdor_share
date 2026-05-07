#!/usr/bin/env python3
"""Relatorio de paridade S01: ``to_plain(Laudo)`` vs coluna ``laudo_tratado`` (CSV).

Mesma logica de normalizacao que ``tests/local/test_hml_sample_to_plain_parity.py`` (modo relaxado).
Sem dependencia de pytest. Uso:

    .venv\\Scripts\\python.exe scripts\\report_to_plain_parity.py --csv tests\\fixtures\\hml_parity_minimal.csv
    .venv\\Scripts\\python.exe scripts\\report_to_plain_parity.py --csv _local_samples\\diamond\\export.csv --max-rows 500
"""

from __future__ import annotations

import argparse
import csv
import difflib
import re
from dataclasses import dataclass
from pathlib import Path

from nlp_engine.text_pipeline.to_plain import to_plain

# Aviso institucional comum em ``laudo_tratado`` HML/Diamond que ``to_plain`` ja remove do corpo;
# opcao ``strip_rtf_notice`` remove a linha em **ambos** os lados so para a metrica de paridade.
_RTF_WEBRIS_NOTICE_LINE = re.compile(
    r"(?is)^.*Este\s+Laudo\s+pode.*completo.*WebRIS.*$",
)

_MAX_DIFF_LINES = 15
_MAX_SNIP = 400


def _norm_for_compare(s: str) -> str:
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    lines_out: list[str] = []
    for raw in t.split("\n"):
        part = " ".join(raw.split())
        if part:
            lines_out.append(part)
    return "\n".join(lines_out)


def _strip_rtf_webris_notice_lines(norm_text: str) -> str:
    """Remove linhas de aviso RTF/WebRIS (texto operacional, nao achado clinico)."""
    kept: list[str] = []
    for line in norm_text.split("\n"):
        if _RTF_WEBRIS_NOTICE_LINE.match(line.strip()):
            continue
        kept.append(line)
    return "\n".join(kept)


def _normalize_rtf_list_markers(norm_text: str) -> str:
    """Normaliza escapes RTF de hifen: inicio ``\\-`` e inline `` \\- `` (ex.: telefones no rodape)."""
    out: list[str] = []
    for line in norm_text.split("\n"):
        if line.startswith("\\-"):
            line = "- " + line[2:].lstrip()
        line = re.sub(r" \\- ", " - ", line)
        out.append(line)
    return "\n".join(out)


def _sniff_delimiter(path: Path) -> str:
    with path.open(encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        return ";"


def _read_rows(path: Path) -> list[dict[str, str]]:
    delim = _sniff_delimiter(path)
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=delim))


@dataclass(frozen=True)
class ParitySummary:
    path: str
    n_rows: int
    n_skipped_empty: int
    n_compared: int
    n_match: int
    n_mismatch: int
    match_rate: float
    first_mismatch: str | None


def run_parity(
    path: Path,
    *,
    max_rows: int | None = None,
    strip_rtf_notice: bool = False,
    normalize_rtf_list_markers: bool = False,
) -> ParitySummary:
    rows = _read_rows(path)
    if max_rows is not None:
        rows = rows[: max(0, max_rows)]
    skipped = 0
    n_match = 0
    n_compared = 0
    first_bad: str | None = None

    for i, row in enumerate(rows, start=1):
        laudo = (row.get("Laudo") or "").strip()
        tratado = (row.get("laudo_tratado") or "").strip()
        if not laudo or not tratado:
            skipped += 1
            continue
        n_compared += 1
        got = to_plain(laudo)
        exp_n = _norm_for_compare(tratado)
        got_n = _norm_for_compare(got)
        if strip_rtf_notice:
            exp_n = _strip_rtf_webris_notice_lines(exp_n)
            got_n = _strip_rtf_webris_notice_lines(got_n)
        if normalize_rtf_list_markers:
            exp_n = _normalize_rtf_list_markers(exp_n)
            got_n = _normalize_rtf_list_markers(got_n)
        if exp_n == got_n:
            n_match += 1
        elif first_bad is None:
            a_lines = (exp_n + "\n").splitlines(keepends=True)
            b_lines = (got_n + "\n").splitlines(keepends=True)
            diff = "".join(
                difflib.unified_diff(
                    a_lines,
                    b_lines,
                    fromfile="laudo_tratado(norm)",
                    tofile="to_plain(norm)",
                    n=2,
                    lineterm="\n",
                )
            )[:2000]
            snip_exp = exp_n[:_MAX_SNIP].replace("\n", "\\n")
            snip_got = got_n[:_MAX_SNIP].replace("\n", "\\n")
            first_bad = (
                f"Linha {i}: esperado (inicio)={snip_exp!r} obtido={snip_got!r}\n{diff[: _MAX_DIFF_LINES * 60]}"
            )

    rate = (n_match / n_compared) if n_compared else 0.0
    return ParitySummary(
        path=str(path.resolve()),
        n_rows=len(rows),
        n_skipped_empty=skipped,
        n_compared=n_compared,
        n_match=n_match,
        n_mismatch=n_compared - n_match,
        match_rate=rate,
        first_mismatch=first_bad,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Relatorio paridade to_plain vs laudo_tratado")
    p.add_argument("--csv", type=Path, required=True, help="CSV com colunas Laudo e laudo_tratado")
    p.add_argument("--max-rows", type=int, default=None, help="Limitar linhas de dados apos cabecalho")
    p.add_argument(
        "--strip-rtf-notice",
        action="store_true",
        help="Ignora linha de aviso RTF/WebRIS em ambos os lados (paridade operacional vs HML)",
    )
    p.add_argument(
        "--normalize-rtf-list-markers",
        action="store_true",
        help="Trata '\\-' como '- ' no inicio da linha (export HML vs to_plain)",
    )
    p.add_argument(
        "--parity-relaxed",
        action="store_true",
        help="Equivale a --strip-rtf-notice --normalize-rtf-list-markers",
    )
    args = p.parse_args()
    if not args.csv.is_file():
        raise SystemExit(f"Ficheiro nao encontrado: {args.csv}")
    strip = args.strip_rtf_notice or args.parity_relaxed
    norm_bullets = args.normalize_rtf_list_markers or args.parity_relaxed
    s = run_parity(
        args.csv,
        max_rows=args.max_rows,
        strip_rtf_notice=strip,
        normalize_rtf_list_markers=norm_bullets,
    )
    print(f"Ficheiro: {s.path}")
    print(f"Linhas no lote: {s.n_rows} | ignoradas (Laudo/tratado vazio): {s.n_skipped_empty}")
    print(f"Comparadas: {s.n_compared} | match: {s.n_match} | mismatch: {s.n_mismatch}")
    print(f"Taxa de match: {s.match_rate:.4f}")
    if s.first_mismatch:
        print("\n--- Primeiro mismatch ---\n")
        print(s.first_mismatch)


if __name__ == "__main__":
    main()
