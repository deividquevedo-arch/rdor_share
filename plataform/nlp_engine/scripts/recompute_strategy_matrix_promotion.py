#!/usr/bin/env python3
"""Reavalia vencedor da matriz a partir de JSON ja gerado (sem reexecutar audit).

Util quando a matriz completa ja correu e quer comparar perfis de promocao
(`fp_ceiling` vs `fn_priority`) sem custo de CPU.

Exemplo::

    .venv\\Scripts\\python.exe scripts\\recompute_strategy_matrix_promotion.py ^
      _local_samples\\exports\\hepatologia_diamond_bench\\hepatologia_strategy_matrix.json ^
      --promotion-profile fn_priority
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.run_hepatologia_strategy_matrix import recompute_promotion_from_report


def main() -> None:
    ap = argparse.ArgumentParser(description="Recomputar winner de relatorio strategy matrix")
    ap.add_argument("report_json", type=Path, help="hepatologia_strategy_matrix.json")
    ap.add_argument(
        "--promotion-profile",
        type=str,
        required=True,
        choices=["fp_ceiling", "fn_priority"],
    )
    ap.add_argument(
        "--scenarios-yaml",
        type=Path,
        default=None,
        help="Opcional: YAML da matriz (default: campo inputs.scenarios_yaml do JSON ou repo)",
    )
    ap.add_argument("--out-json", type=Path, default=None, help="Opcional: gravar resultado")
    args = ap.parse_args()

    if not args.report_json.is_file():
        raise SystemExit(f"ficheiro nao encontrado: {args.report_json}")

    data = json.loads(args.report_json.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise SystemExit("JSON invalido")

    res = recompute_promotion_from_report(
        data,
        profile=args.promotion_profile,
        scenarios_yaml=args.scenarios_yaml,
    )
    text = json.dumps(res, ensure_ascii=False, indent=2)
    print(text)
    if args.out_json:
        args.out_json.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
