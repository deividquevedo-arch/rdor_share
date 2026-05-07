#!/usr/bin/env python3
"""Estratifica FN/FP da amostra Pulmao: exm_class, escopo (torax vs misto vs abdome), cruzado com bucket.

Saida: JSON com **apenas contagens** (sem texto de laudo) para colar em evidencia / delta.
Requer: bucket export (mismatches), expected (legacy_class, ...), input (exm_laudo_texto) para heuristica de escopo.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import sys

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.compare_pulmao_audit_vs_legacy import _read_rows_pulmao

# Heuristica de escopo (nao clinica; so para estratificar G4) — titulo/lead do laudo.
_RE_TORAX = re.compile(
    r"(?:tomografia|tc)\s+[^.\n]{0,80}?(?:t[óo]rax|tórax|tor[áa]cic)",
    re.IGNORECASE,
)
_RE_ABD = re.compile(
    r"(?:tomografia|tc|resson[âa]ncia)\s+[^.\n]{0,100}?(?:abd[oô]me|pelve|hep[áa]t|p[âa]ncre|bexig|rim|renais?)",
    re.IGNORECASE,
)
_RE_PULM = re.compile(r"\b(?:pulm[ãa]o|pulm[ôo]es|par[êe]nquima\s+pulmon)\b", re.IGNORECASE)


def _scope_label(laudo: str) -> str:
    """torax | abdome_ou_pelve | misto | indeterminado"""
    t = (laudo or "")[:12_000]
    has_tor = bool(_RE_TORAX.search(t) or _RE_PULM.search(t))
    has_abd = bool(_RE_ABD.search(t))
    if has_tor and has_abd:
        return "misto"
    if has_abd and not has_tor:
        return "abdome_ou_pelve"
    if has_tor and not has_abd:
        return "torax"
    return "indeterminado"


def _read_indexed(
    path: Path, key: str
) -> dict[str, dict[str, str]]:
    rows = _read_rows_pulmao(path)
    out: dict[str, dict[str, str]] = {}
    for r in rows:
        k = (r.get(key) or r.get("id_exame") or r.get("id_pct") or "").strip()
        if k:
            out[k] = r
    return out


def run_stratify(
    *,
    buckets_csv: Path,
    expected_csv: Path,
    input_csv: Path,
) -> dict[str, Any]:
    b_rows = _read_rows_pulmao(buckets_csv)
    exp_by_id = _read_indexed(expected_csv, "id_exame")
    inp_by_id = _read_indexed(input_csv, "id_exame")

    fn_by_class: Counter[str] = Counter()
    fp_by_class: Counter[str] = Counter()
    fn_by_bucket: Counter[str] = Counter()
    fp_by_bucket: Counter[str] = Counter()
    fn_by_scope: Counter[str] = Counter()
    fp_by_scope: Counter[str] = Counter()
    fn_bucket_x_scope: Counter[tuple[str, str]] = Counter()

    n_fn = n_fp = 0
    for b in b_rows:
        iid = (b.get("id_exame") or "").strip()
        if not iid:
            continue
        leg = (b.get("legacy_flag") or "").strip().upper()
        mot = (b.get("motor_flag") or "").strip().upper()
        bucket = (b.get("bucket") or "").strip() or "sem_bucket"
        is_fn = leg == "S" and mot == "N"
        is_fp = leg == "N" and mot == "S"
        if not is_fn and not is_fp:
            continue
        ex = exp_by_id.get(iid, {})
        raw_class = (ex.get("legacy_class") or ex.get("exm_class") or "").strip()
        first = raw_class.split("//-//")[0].strip() if raw_class else ""
        parts = [p.strip() for p in first.replace(" ", "").split("/") if p.strip()]
        primary = (parts[0] if parts else first) or "(vazio)"
        if len(primary) > 32:
            primary = primary[:29] + "..."

        laudo = (inp_by_id.get(iid) or {}).get("exm_laudo_texto") or ""
        sc = _scope_label(str(laudo))

        if is_fn:
            n_fn += 1
            fn_by_class[primary] += 1
            fn_by_bucket[bucket] += 1
            fn_by_scope[sc] += 1
            fn_bucket_x_scope[(bucket, sc)] += 1
        if is_fp:
            n_fp += 1
            fp_by_class[primary] += 1
            fp_by_bucket[bucket] += 1
            fp_by_scope[sc] += 1

    return {
        "n_fn": n_fn,
        "n_fp": n_fp,
        "fn_by_legacy_class_top": fn_by_class.most_common(25),
        "fp_by_legacy_class_top": fp_by_class.most_common(15),
        "fn_by_bucket": dict(fn_by_bucket),
        "fp_by_bucket": dict(fp_by_bucket),
        "fn_by_scope_heuristic": dict(fn_by_scope),
        "fp_by_scope_heuristic": dict(fp_by_scope),
        "fn_bucket_x_scope": {
            f"{a}|{b}": c for (a, b), c in sorted(fn_bucket_x_scope.items())
        },
        "nota": "escopo=heuristica em titulo/lead; revisar G4. legacy_class=segmento A/A01 trunchado a 32 chars",
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Estratificar mismatches Pulmao (agregados so)")
    ap.add_argument("--buckets-csv", type=Path, required=True)
    ap.add_argument("--expected-csv", type=Path, required=True)
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("-o", "--out-json", type=Path, default=None)
    args = ap.parse_args()

    r = run_stratify(
        buckets_csv=args.buckets_csv,
        expected_csv=args.expected_csv,
        input_csv=args.input_csv,
    )
    text = json.dumps(r, ensure_ascii=False, indent=2)
    print(text)
    if args.out_json:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
