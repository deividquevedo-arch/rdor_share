#!/usr/bin/env python3
"""Bancada Hepatologia (ate 2000 linhas): motor vs legado com caminhos convencionais em _local_samples.

Nao versiona PHI: os CSVs ficam fora do Git (ver README). Este script apenas orquestra comandos
ja existentes (audit, compare, matriz de estrategias).

Exemplos (a partir de ``plataform/nlp_engine``)::

    # Bancada habitual: um so CSV (laudo + cod_achado_relevante + id_predicao), ex. query_hepato_validate.csv.
    # Se o ficheiro existir em _local_samples/diamond/, e usado automaticamente quando nao passa --input-csv/--legacy-csv.
    .venv\\Scripts\\python.exe scripts\\run_hepatologia_diamond_bench.py --mode baseline --max-rows 2000

    # Forcar explicitamente o query (ou variavel NLP_HEPATO_QUERY_VALIDATE para outro caminho)::
    .venv\\Scripts\\python.exe scripts\\run_hepatologia_diamond_bench.py --mode baseline --max-rows 2000 --from-query-validate

    # Matriz de cenarios + tabela-resumo::
    .venv\\Scripts\\python.exe scripts\\run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 --print-table

    # Alternativa: amostra standard (input + expected) gerada a partir do export Diamond ``saida``::
    .venv\\Scripts\\python.exe scripts\\build_hepatologia_standard_sample.py ^
      --source diamond --legacy-csv _local_samples\\diamond\\tb_diamond_mod_hepatologia_saida.csv ^
      --out-dir _local_samples\\standard\\hepatologia --max-positive 1000 --max-negative 1000
    .venv\\Scripts\\python.exe scripts\\run_hepatologia_diamond_bench.py --mode baseline ^
      --input-csv _local_samples\\standard\\hepatologia\\hepatologia_standard_input.csv ^
      --legacy-csv _local_samples\\standard\\hepatologia\\hepatologia_standard_expected.csv

    # CSVs noutro sitio::
    .venv\\Scripts\\python.exe scripts\\run_hepatologia_diamond_bench.py --mode baseline ^
      --input-csv C:\\dados\\entrada.csv --legacy-csv C:\\dados\\gold.csv
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.audit_engine_from_csv import run_audit
from scripts.audit_legacy_compare import run_compare_audit_vs_legacy
from scripts.run_hepatologia_strategy_matrix import format_summary_table, run_matrix

_DEFAULT_IN = _ROOT / "_local_samples" / "standard" / "hepatologia" / "hepatologia_standard_input.csv"
_DEFAULT_LEGACY = _ROOT / "_local_samples" / "standard" / "hepatologia" / "hepatologia_standard_expected.csv"
_QUERY_VALIDATE_DEFAULT = _ROOT / "_local_samples" / "diamond" / "query_hepato_validate.csv"
_DEFAULT_OUT = _ROOT / "_local_samples" / "exports" / "hepatologia_diamond_bench"


def _resolve_inputs(
    *,
    from_query_validate: bool,
    input_csv: Path,
    legacy_csv: Path,
) -> tuple[Path, Path]:
    """Prioridade: --from-query-validate; caminhos explicitos; query_hepato_validate; amostra standard."""
    env_path = (os.environ.get("NLP_HEPATO_QUERY_VALIDATE") or "").strip()
    query_path = Path(env_path) if env_path else _QUERY_VALIDATE_DEFAULT

    if from_query_validate:
        return query_path, query_path
    if input_csv != _DEFAULT_IN or legacy_csv != _DEFAULT_LEGACY:
        return input_csv, legacy_csv
    if query_path.is_file():
        return query_path, query_path
    if _DEFAULT_IN.is_file() and _DEFAULT_LEGACY.is_file():
        return _DEFAULT_IN, _DEFAULT_LEGACY
    return input_csv, legacy_csv


def _die_missing(input_csv: Path, legacy_csv: Path) -> None:
    msg = (
        "CSV(s) em falta. Opcoes:\n\n"
        "  A) Coloque o export de validacao em _local_samples\\\\diamond\\\\query_hepato_validate.csv "
        "(ou defina NLP_HEPATO_QUERY_VALIDATE) e corra com --from-query-validate ou deixe os defaults.\n\n"
        "  B) Gere hepatologia_standard_{input,expected}.csv:\n"
        "  .venv\\\\Scripts\\\\python.exe scripts\\\\build_hepatologia_standard_sample.py "
        "--source diamond "
        "--legacy-csv _local_samples\\\\diamond\\\\tb_diamond_mod_hepatologia_saida.csv "
        "--out-dir _local_samples\\\\standard\\\\hepatologia "
        "--max-positive 1000 --max-negative 1000\n\n"
        "  C) Passe --input-csv e --legacy-csv (audit: ``proced_laudo_exame``/``exm_laudo_texto``; "
        "gold: ``cod_achado_relevante`` ou ``expected_encaminhamento``).\n"
    )
    miss = [str(p) for p in (input_csv, legacy_csv) if not p.is_file()]
    if miss:
        sys.stderr.write(msg)
        sys.stderr.write("Em falta: " + ", ".join(miss) + "\n")
        raise SystemExit(2)


def run_baseline(
    *,
    input_csv: Path,
    legacy_csv: Path,
    base_config_yaml: Path,
    out_dir: Path,
    max_rows: int,
    only_cod_123: bool = False,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    audit_p = out_dir / "baseline_audit.csv"
    cmp_p = out_dir / "baseline_compare.json"
    run_audit(
        input_csv,
        audit_p,
        max_rows=max_rows,
        config_yaml=base_config_yaml,
        engine_version="diamond-bench-baseline",
    )
    cmp = run_compare_audit_vs_legacy(
        audit_csv=audit_p,
        legacy_csv=legacy_csv,
        out_json=cmp_p,
        only_cod_123=only_cod_123,
    )
    return {
        "mode": "baseline",
        "audit_csv": str(audit_p.resolve()),
        "compare_json": str(cmp_p.resolve()),
        "compare": cmp,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Bancada Diamond Hepatologia vs legado (baseline ou matriz)")
    ap.add_argument("--mode", choices=("baseline", "matrix"), default="matrix")
    ap.add_argument("--input-csv", type=Path, default=_DEFAULT_IN)
    ap.add_argument("--legacy-csv", type=Path, default=_DEFAULT_LEGACY)
    ap.add_argument(
        "--from-query-validate",
        action="store_true",
        help="Usa o mesmo CSV para audit e gold (query Diamond: proced_laudo_exame, id_predicao, cod_achado_relevante).",
    )
    ap.add_argument(
        "--base-config-yaml",
        type=Path,
        default=_ROOT / "configs" / "hepatologia" / "config.yaml",
    )
    ap.add_argument(
        "--scenarios-yaml",
        type=Path,
        default=_ROOT / "configs" / "hepatologia" / "scenarios" / "strategy_matrix.yaml",
    )
    ap.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    ap.add_argument("--out-json", type=Path, default=None)
    ap.add_argument("--max-rows", type=int, default=2000)
    ap.add_argument("--bootstrap-n", type=int, default=400)
    ap.add_argument("--only-scenarios", type=str, default="", help="So modo matrix. Virgulas.")
    ap.add_argument("--print-table", action="store_true")
    ap.add_argument(
        "--only-cod-123",
        action="store_true",
        help="Compara apenas linhas do gold com cod_achado_relevante iniciando em 1/2/3.",
    )
    ap.add_argument(
        "--promotion-profile",
        type=str,
        default=None,
        metavar="fp_ceiling|fn_priority",
        help="So modo matrix. fn_priority = minimizar FN com orcamento FP (ver strategy_matrix.yaml).",
    )
    args = ap.parse_args()

    inp, leg = _resolve_inputs(
        from_query_validate=args.from_query_validate,
        input_csv=args.input_csv,
        legacy_csv=args.legacy_csv,
    )
    _die_missing(inp, leg)

    if args.mode == "baseline":
        res = run_baseline(
            input_csv=inp,
            legacy_csv=leg,
            base_config_yaml=args.base_config_yaml,
            out_dir=args.out_dir,
            max_rows=args.max_rows,
            only_cod_123=args.only_cod_123,
        )
        out = args.out_json or (args.out_dir / "hepatologia_diamond_bench_baseline.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
        c = res["compare"]
        print(f"joined={c.get('n_joined')} match_rate={c.get('match_rate')} status={c.get('status')}")
        print(f"JSON: {out.resolve()}")
        return

    only: frozenset[str] | None = None
    if args.only_scenarios.strip():
        only = frozenset(x.strip() for x in args.only_scenarios.split(",") if x.strip())

    prom_raw = (args.promotion_profile or "").strip()
    if prom_raw and prom_raw not in ("fp_ceiling", "fn_priority"):
        raise SystemExit("--promotion-profile deve ser fp_ceiling ou fn_priority")
    prom_override = prom_raw or None

    res = run_matrix(
        input_csv=inp,
        legacy_csv=leg,
        base_config_yaml=args.base_config_yaml,
        scenarios_yaml=args.scenarios_yaml,
        out_dir=args.out_dir,
        max_rows=args.max_rows,
        bootstrap_n=args.bootstrap_n,
        bootstrap_seed=7,
        only_scenario_ids=only,
        promotion_profile_override=prom_override,
        only_cod_123=args.only_cod_123,
    )
    out = args.out_json or (args.out_dir / "hepatologia_strategy_matrix.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Relatorio: {out.resolve()}")
    if args.print_table:
        spec = yaml.safe_load(args.scenarios_yaml.read_text(encoding="utf-8-sig"))
        bid = str((spec.get("matrix_spec") or {}).get("baseline_scenario_id") or "baseline")
        print()
        print(format_summary_table(res, baseline_id=bid))


if __name__ == "__main__":
    main()
