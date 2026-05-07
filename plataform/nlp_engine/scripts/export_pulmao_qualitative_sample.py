#!/usr/bin/env python3
"""Amostra linhas para revisao qualitativa (15–20): ids + bucket + classe; sem texto de laudo no CSV.

O revisor abre o laudo localmente pelo id. Colunas vazias para preencher causa raiz offline.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.compare_pulmao_audit_vs_legacy import _read_rows_pulmao


def run_export(
    *,
    buckets_csv: Path,
    expected_csv: Path,
    seed: int,
    n_lexico: int,
    n_sem_lex: int,
    n_fp: int,
) -> list[dict[str, str]]:
    buckets = _read_rows_pulmao(buckets_csv)
    exp = {
        (r.get("id_exame") or "").strip(): r
        for r in _read_rows_pulmao(expected_csv)
        if (r.get("id_exame") or "").strip()
    }

    by_b: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in buckets:
        b = (row.get("bucket") or "").strip()
        by_b[b].append(row)

    rng = random.Random(seed)

    def pick(bucket: str, n: int) -> list[dict[str, str]]:
        rows = by_b.get(bucket, [])
        if not rows or n <= 0:
            return []
        k = min(n, len(rows))
        sample = rng.sample(rows, k=k) if len(rows) > k else list(rows)
        out: list[dict[str, str]] = []
        for s in sample:
            iid = (s.get("id_exame") or "").strip()
            ex = exp.get(iid, {})
            cls = (ex.get("legacy_class") or ex.get("exm_class") or "")[:80]
            out.append(
                {
                    "id_exame": iid,
                    "bucket": bucket,
                    "legacy_class": cls,
                    "legacy_flag": s.get("legacy_flag", ""),
                    "motor_flag": s.get("motor_flag", ""),
                    "root_cause": "",
                    "notes_reviewer": "",
                }
            )
        return out

    rows_out: list[dict[str, str]] = []
    rows_out.extend(pick("lexico_pulmao_ou_gate_regra", n_lexico))
    rows_out.extend(pick("contexto_comparativo_sem_achado_lexical_obvio", n_sem_lex))
    fp_picked = 0
    for bname in sorted(by_b.keys()):
        if bname.startswith("fp_") and fp_picked < n_fp:
            need = n_fp - fp_picked
            part = pick(bname, need)
            rows_out.extend(part)
            fp_picked += len(part)
    if fp_picked < n_fp:
        for row in buckets:
            if fp_picked >= n_fp:
                break
            if (row.get("legacy_flag", "").upper() == "N" and row.get("motor_flag", "").upper() == "S"):
                iid = (row.get("id_exame") or "").strip()
                if not iid or any(x["id_exame"] == iid for x in rows_out):
                    continue
                ex = exp.get(iid, {})
                rows_out.append(
                    {
                        "id_exame": iid,
                        "bucket": (row.get("bucket") or ""),
                        "legacy_class": (ex.get("legacy_class") or "")[:80],
                        "legacy_flag": row.get("legacy_flag", ""),
                        "motor_flag": row.get("motor_flag", ""),
                        "root_cause": "",
                        "notes_reviewer": "",
                    }
                )
                fp_picked += 1
    return rows_out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Exporta amostra qualitativa (ids) para revisao offline"
    )
    ap.add_argument("--buckets-csv", type=Path, required=True)
    ap.add_argument("--expected-csv", type=Path, required=True)
    ap.add_argument("-o", "--out-csv", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n-lexico-gate", type=int, default=7, help="lexico_pulmao_ou_gate_regra")
    ap.add_argument("--n-sem-lex", type=int, default=8, help="contexto_comparativo_sem_achado_lexical_obvio")
    ap.add_argument("--n-fp", type=int, default=2)
    args = ap.parse_args()

    rows: list[dict[str, Any]] = run_export(
        buckets_csv=args.buckets_csv,
        expected_csv=args.expected_csv,
        seed=args.seed,
        n_lexico=args.n_lexico_gate,
        n_sem_lex=args.n_sem_lex,
        n_fp=args.n_fp,
    )
    if not rows:
        print("nenhuma linha; verifique buckets e nomes de bucket")
        return
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    with args.out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        w.writeheader()
        w.writerows(rows)
    print(f"linhas={len(rows)} -> {args.out_csv}")


if __name__ == "__main__":
    main()
