#!/usr/bin/env python3
"""Classifica linhas de mismatch motor vs legado (Pulmão) em buckets heuristicos (S06b T06.7)."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

# `python scripts/bucket_…py` nao coloca a raiz do repo em sys.path (diferente de `pytest` + conftest).
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.compare_pulmao_audit_vs_legacy import _read_rows_pulmao


def _motor_flag(row: dict[str, str]) -> str:
    return "S" if str(row.get("fl_relevante", "")).strip() == "1" else "N"


def _legacy_flag(row: dict[str, str]) -> str:
    raw = row.get("exm_encaminhamento_nlp") or row.get("expected_encaminhamento") or ""
    return str(raw).strip().upper()


def _key_m(r: dict[str, str]) -> str:
    return str(r.get("id_exame", "")).strip()


def _key_l(r: dict[str, str]) -> str:
    s = (r.get("id_exame") or r.get("id_pct") or r.get("id", "")) or ""
    return str(s).strip()


# Heuristicas: referencia, nao cobertura clinica; revisao humana recomendada.
_CHEST = re.compile(
    r"\b(pulm(ao|ões|oes|ao)|par[eê]nquima|lobo (superior|inferior|dir|esq)|"
    r"tor[áa]x|pleur|derrame pleur|hemitor[áa]x|hilo pulmon|br[ôo]nqu|traqueia)\b",
    re.IGNORECASE,
)
_ABDOMEN_OUT = re.compile(
    r"\b(f[íi]gado|hep[áa]t|p[âa]ncre[áa]|ves[ií]cula biliar|c[óo]lon|bexig|"
    r"rins?\b|renal|supra-?renal|aorta abdom|pelve)\b",
    re.IGNORECASE,
)
_FINDISH = re.compile(
    r"\b(n[oó]dul|micron[oó]dul|opacidad|consolid|vidro\s*fos|enfisem|"
    r"espicul|subs[oó]lid|densidade|brota(mento)?|microcalcif)\b",
    re.IGNORECASE,
)


def _n_from_audit(row: dict[str, str]) -> tuple[int, int]:
    try:
        n_pos = int(str(row.get("n_positive_spans", "0") or 0).strip() or 0)
    except ValueError:
        n_pos = 0
    try:
        n_neg = int(str(row.get("n_negated_spans", "0") or 0).strip() or 0)
    except ValueError:
        n_neg = 0
    return n_pos, n_neg


def _bucket(
    text: str,
    *,
    legacy_s: bool,
    motor_s: bool,
    n_pos: int,
    n_neg: int,
) -> str:
    if not legacy_s and not motor_s:
        return "alinhado_N"
    if legacy_s and motor_s:
        return "alinhado_S"
    t = (text or "")[:20_000]

    if not legacy_s and motor_s:
        if _ABDOMEN_OUT.search(t) and not _CHEST.search(t):
            return "fp_escopo_suspeita_extra_torax"
        return "fp_outro"

    # FN: legacy S, motor N
    if n_neg >= 1 and n_pos == 0:
        return "negacao_ou_achado_sob_negacao"
    if _ABDOMEN_OUT.search(t) and not _CHEST.search(t):
        return "escopo_extra_pulmao_suspeita_abdomen_ou_pelve"
    if n_pos == 0 and not _FINDISH.search(t):
        return "contexto_comparativo_sem_achado_lexical_obvio"
    if n_pos == 0 and _FINDISH.search(t) and not _CHEST.search(t):
        return "possivel_proximidade_ou_ancora_toracica"
    if n_pos == 0 and _FINDISH.search(t) and _CHEST.search(t):
        return "lexico_pulmao_ou_gate_regra"
    return "contexto_comparativo_outro"


def run_bucket(
    *,
    audit_csv: Path,
    legacy_csv: Path,
    input_csv: Path | None,
) -> list[dict[str, Any]]:
    aud = {_key_m(r): r for r in _read_rows_pulmao(audit_csv) if _key_m(r)}
    leg = {_key_l(r): r for r in _read_rows_pulmao(legacy_csv) if _key_l(r)}
    text_by_id: dict[str, str] = {}
    if input_csv and input_csv.is_file():
        for r in _read_rows_pulmao(input_csv):
            k = (r.get("id_exame") or r.get("id_pct") or "").strip()
            if k:
                text_by_id[k] = (r.get("exm_laudo_texto") or "")[:20_000]

    out: list[dict[str, Any]] = []
    for k in sorted(set(aud) & set(leg)):
        a = aud[k]
        l = leg[k]
        m_f = _motor_flag(a)
        l_f = _legacy_flag(l)
        if m_f == l_f:
            continue
        n_pos, n_neg = _n_from_audit(a)
        text = text_by_id.get(k, a.get("exm_laudo_texto_tratado") or "")
        b = _bucket(
            text,
            legacy_s=l_f == "S",
            motor_s=m_f == "S",
            n_pos=n_pos,
            n_neg=n_neg,
        )
        summ = a.get("summary_compact_json", "")
        try:
            s_arr = json.loads(summ) if summ else []
        except json.JSONDecodeError:
            s_arr = []
        out.append(
            {
                "id_exame": k,
                "bucket": b,
                "legacy_flag": l_f,
                "motor_flag": m_f,
                "n_positive_spans": n_pos,
                "n_negated_spans": n_neg,
                "summary_compact_n": len(s_arr) if isinstance(s_arr, list) else 0,
                "legacy_class": l.get("legacy_class") or l.get("exm_class") or "",
                "text_snippet": (text or "")[:240].replace("\n", " "),
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Bucketiza mismatches entre audit motor e expected/legado (Pulmao)"
    )
    ap.add_argument("--audit-csv", type=Path, required=True)
    ap.add_argument("--legacy-csv", type=Path, required=True)
    ap.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="CSV de entrada (exm_laudo_texto) para trechos; opcional se audit ja tiver texto",
    )
    ap.add_argument("-o", "--out-csv", type=Path, required=True)
    args = ap.parse_args()

    rows = run_bucket(
        audit_csv=args.audit_csv,
        legacy_csv=args.legacy_csv,
        input_csv=args.input_csv,
    )
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else [
        "id_exame", "bucket", "legacy_flag", "motor_flag",
        "n_positive_spans", "n_negated_spans", "summary_compact_n",
        "legacy_class", "text_snippet",
    ]
    with args.out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    c = Counter(r["bucket"] for r in rows)
    print(f"mismatches={len(rows)} by_bucket={dict(c)}")


if __name__ == "__main__":
    main()
