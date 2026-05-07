#!/usr/bin/env python3
"""Gera um CSV sintetico (sem PHI) para usar com `scripts/demo_engine_e2e.py --csv`.

Colunas alinhadas ao contrato de entrada do motor. Linhas pensadas para a config
fixa de demo do modo CSV (bexiga / calculos / paredes) em `demo_engine_e2e.py`.

Saida default: `_local_samples/exports/demo_motor_export_sintetico.csv` (gitignored).

Exemplo:

    .venv\\Scripts\\python.exe scripts\\gen_synthetic_motor_export_csv.py
    .venv\\Scripts\\python.exe scripts\\gen_synthetic_motor_export_csv.py -o "%USERPROFILE%\\Desktop\\meu_export.csv"
    .venv\\Scripts\\python.exe scripts\\demo_engine_e2e.py --csv _local_samples\\exports\\demo_motor_export_sintetico.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _default_out() -> Path:
    root = Path(__file__).resolve().parents[1]
    return root / "_local_samples" / "exports" / "demo_motor_export_sintetico.csv"


def _rows() -> list[dict[str, str]]:
    return [
        {
            "id_exame": "SYN-EXP-001",
            "id_paciente": "P-SYN-001",
            "id_unidade": "U-SYN-01",
            "exm_laudo_texto": "Bexiga com paredes de espessura habitual no exame sintetico.",
            "exm_mod": "US",
            "exm_tipo": "pelvica",
            "dt_exame": "2026-04-14",
        },
        {
            "id_exame": "SYN-EXP-002",
            "id_paciente": "P-SYN-002",
            "id_unidade": "U-SYN-01",
            "exm_laudo_texto": "Bexiga sem calculos radiopacos no estudo sintetico.",
            "exm_mod": "US",
            "exm_tipo": "pelvica",
            "dt_exame": "2026-04-14",
        },
        {
            "id_exame": "SYN-EXP-003",
            "id_paciente": "P-SYN-003",
            "id_unidade": "U-SYN-01",
            "exm_laudo_texto": (
                "<p>Bexiga distendida.</p><p>Observa-se calculos no interior da bexiga.</p>"
            ),
            "exm_mod": "CT",
            "exm_tipo": "abdome",
            "dt_exame": "2026-04-14",
        },
    ]


def main() -> None:
    p = argparse.ArgumentParser(description="CSV sintetico para demo_engine_e2e --csv")
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Ficheiro de saida (default: _local_samples/exports/demo_motor_export_sintetico.csv)",
    )
    args = p.parse_args()
    out = args.output if args.output is not None else _default_out()
    out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id_exame",
        "id_paciente",
        "id_unidade",
        "exm_laudo_texto",
        "exm_mod",
        "exm_tipo",
        "dt_exame",
    ]
    rows = _rows()
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        w.writerows(rows)
    print(f"Escrito: {out.resolve()} ({len(rows)} linhas)")


if __name__ == "__main__":
    main()
