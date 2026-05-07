#!/usr/bin/env python3
"""Monta amostra padrao Pulmao (input + expected) a partir do legado disponivel.

Objetivo:
- gerar um dataset canônico para validacao local motor vs legado no MESMO conjunto;
- preservar representatividade da amostra disponivel (default: usa todas as linhas);
- explicitar "deve acontecer" (S) vs "nao deve acontecer" (N) em expected.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path
from typing import Any


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=","))


def _fl_from_enc(enc: str) -> int:
    return 1 if (enc or "").strip().upper() == "S" else 0


def _behavior_label(enc: str) -> str:
    return "deve_acontecer" if (enc or "").strip().upper() == "S" else "nao_deve_acontecer"


def _pick_rows(rows: list[dict[str, str]], max_rows: int | None) -> list[dict[str, str]]:
    if max_rows is None or max_rows <= 0 or max_rows >= len(rows):
        return rows
    # Mantem proporcionalidade S/N no recorte.
    s_rows = [r for r in rows if (r.get("exm_encaminhamento_nlp") or "").strip().upper() == "S"]
    n_rows = [r for r in rows if (r.get("exm_encaminhamento_nlp") or "").strip().upper() != "S"]
    n_s = round(max_rows * (len(s_rows) / len(rows)))
    n_n = max_rows - n_s
    return s_rows[:n_s] + n_rows[:n_n]


def run_build(
    *,
    source_legacy_csv: Path,
    out_dir: Path,
    max_rows: int | None = None,
) -> dict[str, Any]:
    if not source_legacy_csv.is_file():
        raise FileNotFoundError(f"CSV legado nao encontrado: {source_legacy_csv}")

    rows = _read_csv(source_legacy_csv)
    if not rows:
        raise ValueError("CSV legado sem linhas")

    picked = _pick_rows(rows, max_rows=max_rows)
    out_dir.mkdir(parents=True, exist_ok=True)

    id_pct_counts: Counter[str] = Counter(
        (r.get("id_pct") or "").strip() for r in picked if (r.get("id_pct") or "").strip()
    )

    input_csv = out_dir / "pulmao_standard_input.csv"
    expected_csv = out_dir / "pulmao_standard_expected.csv"

    input_fields = [
        "id_exame",
        "id_pct",
        "exm_an",
        "exm_mod",
        "exm_tipo",
        "dt_exame",
        "exm_laudo_texto",
    ]
    expected_fields = [
        "id_exame",
        "id_pct",
        "exm_an",
        "expected_encaminhamento",
        "expected_fl_relevante",
        "expected_behavior",
        "legacy_class",
        "legacy_frase_selec",
    ]

    with input_csv.open("w", encoding="utf-8-sig", newline="") as f_in, expected_csv.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f_exp:
        w_in = csv.DictWriter(f_in, fieldnames=input_fields, delimiter=";")
        w_exp = csv.DictWriter(f_exp, fieldnames=expected_fields, delimiter=";")
        w_in.writeheader()
        w_exp.writeheader()

        for i, r in enumerate(picked):
            id_pct = (r.get("id_pct") or "").strip()
            exm_an = (r.get("exm_an") or "").strip()
            enc = (r.get("exm_encaminhamento_nlp") or "").strip().upper()
            if id_pct and id_pct_counts.get(id_pct, 0) > 1:
                exm_key = exm_an or "noexm"
                id_exame = f"{id_pct}__{exm_key}__{i}"
            else:
                id_exame = id_pct or exm_an

            w_in.writerow(
                {
                    "id_exame": id_exame,
                    "id_pct": id_pct,
                    "exm_an": exm_an,
                    "exm_mod": (r.get("exm_mod") or "").strip(),
                    "exm_tipo": (r.get("exm_tipo") or "").strip(),
                    "dt_exame": (r.get("exm_data") or "").strip(),
                    "exm_laudo_texto": r.get("exm_laudo_texto") or "",
                }
            )
            w_exp.writerow(
                {
                    "id_exame": id_exame,
                    "id_pct": id_pct,
                    "exm_an": exm_an,
                    "expected_encaminhamento": enc,
                    "expected_fl_relevante": _fl_from_enc(enc),
                    "expected_behavior": _behavior_label(enc),
                    "legacy_class": (r.get("exm_class") or "").strip(),
                    "legacy_frase_selec": (r.get("exm_frase_selec") or "").strip(),
                }
            )

    enc_counter = Counter((r.get("exm_encaminhamento_nlp") or "").strip().upper() for r in picked)
    class_counter = Counter((r.get("exm_class") or "").strip() for r in picked)
    summary = {
        "source_rows": len(rows),
        "sample_rows": len(picked),
        "enc_distribution": dict(enc_counter),
        "top_classes": class_counter.most_common(10),
        "input_csv": str(input_csv),
        "expected_csv": str(expected_csv),
    }
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Gerar amostra padrao Pulmao para validacao local")
    ap.add_argument(
        "--legacy-csv",
        type=Path,
        default=Path("_local_samples/diamond/dev_tb_diamond_mod_pulmao_saida.csv"),
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path("_local_samples/standard/pulmao"),
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="0 = usar todas as linhas disponiveis",
    )
    args = ap.parse_args()

    summary = run_build(
        source_legacy_csv=args.legacy_csv,
        out_dir=args.out_dir,
        max_rows=args.max_rows if args.max_rows > 0 else None,
    )

    print(
        f"sample_rows={summary['sample_rows']} enc={summary['enc_distribution']} "
        f"input={summary['input_csv']} expected={summary['expected_csv']}"
    )


if __name__ == "__main__":
    main()
