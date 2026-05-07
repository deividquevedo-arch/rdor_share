#!/usr/bin/env python3
"""Compara auditoria do motor Pulmao vs saida legado (mesmo input).

Logica partilhada: `scripts/audit_legacy_compare.py`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.audit_legacy_compare import read_rows_semico_first, run_compare_audit_vs_legacy

_read_rows_pulmao = read_rows_semico_first


def run_compare(
    *,
    audit_csv: Path,
    legacy_csv: Path,
    out_json: Path | None,
) -> dict:
    return run_compare_audit_vs_legacy(
        audit_csv=audit_csv,
        legacy_csv=legacy_csv,
        out_json=out_json,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Comparar Pulmao: audit motor vs saida legado")
    ap.add_argument("--audit-csv", type=Path, required=True)
    ap.add_argument("--legacy-csv", type=Path, required=True)
    ap.add_argument("--out-json", type=Path, default=None)
    args = ap.parse_args()

    res = run_compare(
        audit_csv=args.audit_csv,
        legacy_csv=args.legacy_csv,
        out_json=args.out_json,
    )
    print(
        f"joined={res['n_joined']} match={res['n_match']} mismatch={res['n_mismatch']} "
        f"rate={res['match_rate']}"
    )


if __name__ == "__main__":
    main()
