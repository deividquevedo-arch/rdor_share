#!/usr/bin/env python3
"""Executa gate A/B/C (rule, hybrid, hybrid+router) com metricas pareadas."""

from __future__ import annotations

import argparse
import copy
import csv
import json
import math
import random
import sys
from pathlib import Path
from typing import Any

import yaml

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.audit_engine_from_csv import run_audit
from scripts.audit_legacy_compare import legacy_s_n_from_row, read_rows_semico_first, run_compare_audit_vs_legacy


def _key(row: dict[str, str]) -> str:
    pid = str(row.get("id_predicao", "")).strip()
    if pid:
        return f"PRED::{pid}"
    ex = str(row.get("id_exame", "")).strip()
    return f"EX::{ex}" if ex else ""


def _mcnemar(audit_a: Path, audit_b: Path, legacy_csv: Path) -> dict[str, Any]:
    a = {_key(r): r for r in read_rows_semico_first(audit_a) if _key(r)}
    b = {_key(r): r for r in read_rows_semico_first(audit_b) if _key(r)}
    legacy = {}
    for r in read_rows_semico_first(legacy_csv):
        k = _key(r)
        if not k:
            ex = str((r.get("id_exame") or r.get("id_pct") or r.get("id") or "")).strip()
            k = f"EX::{ex}" if ex else ""
        if k:
            legacy[k] = r
    keys = sorted(set(a) & set(b) & set(legacy))
    ab = 0
    ba = 0
    for k in keys:
        truth = legacy_s_n_from_row(legacy[k])
        pa = "S" if str(a[k].get("fl_relevante", "")).strip() == "1" else "N"
        pb = "S" if str(b[k].get("fl_relevante", "")).strip() == "1" else "N"
        a_ok = pa == truth
        b_ok = pb == truth
        if a_ok and not b_ok:
            ab += 1
        elif b_ok and not a_ok:
            ba += 1
    if ab + ba == 0:
        return {"a_only": ab, "b_only": ba, "chi2": 0.0, "significant_0_05": False}
    chi2 = ((abs(ab - ba) - 1) ** 2) / (ab + ba)
    return {"a_only": ab, "b_only": ba, "chi2": round(chi2, 4), "significant_0_05": chi2 >= 3.841}


def _bootstrap_match_ci(audit_csv: Path, legacy_csv: Path, n_boot: int = 400, seed: int = 7) -> tuple[float, float]:
    audit = {_key(r): r for r in read_rows_semico_first(audit_csv) if _key(r)}
    legacy = {}
    for r in read_rows_semico_first(legacy_csv):
        k = _key(r)
        if not k:
            ex = str((r.get("id_exame") or r.get("id_pct") or r.get("id") or "")).strip()
            k = f"EX::{ex}" if ex else ""
        if k:
            legacy[k] = r
    keys = sorted(set(audit) & set(legacy))
    if not keys:
        return (0.0, 0.0)
    rng = random.Random(seed)
    rates: list[float] = []
    for _ in range(n_boot):
        sample = [keys[rng.randrange(0, len(keys))] for _ in range(len(keys))]
        ok = 0
        for k in sample:
            pred = "S" if str(audit[k].get("fl_relevante", "")).strip() == "1" else "N"
            if pred == legacy_s_n_from_row(legacy[k]):
                ok += 1
        rates.append(ok / len(sample))
    rates.sort()
    lo = rates[int(0.025 * (len(rates) - 1))]
    hi = rates[int(0.975 * (len(rates) - 1))]
    return (round(lo, 4), round(hi, 4))


def _scenario_cfg(base: dict[str, Any], scenario: str) -> dict[str, Any]:
    cfg = copy.deepcopy(base)
    nlp = cfg.setdefault("nlp", {})
    emb = nlp.setdefault("embeddings", {})
    if scenario == "A_rule_only":
        emb["use_embeddings"] = False
        nlp["llm_router"] = {"enabled": False}
    elif scenario == "B_hybrid_calibrated":
        emb["use_embeddings"] = True
        emb["decision_mode"] = "hybrid"
        nlp["llm_router"] = {"enabled": False}
    elif scenario == "C_hybrid_llm_router":
        emb["use_embeddings"] = True
        emb["decision_mode"] = "hybrid"
        nlp["llm_router"] = {
            "enabled": True,
            "uncertainty_band": [0.35, 0.65],
            "negative_context_patterns": [
                r"sem\s+c[aá]lculos",
                r"aus[êe]ncia\s+de\s+c[aá]lculos",
                r"n[aã]o\s+h[áa]\s+c[aá]lculos",
            ],
            "positive_context_patterns": [
                r"colelit[ií]ase",
                r"microlit[ií]ase",
                r"barro\s+biliar|lama\s+biliar",
            ],
        }
    else:
        raise ValueError(f"cenario invalido: {scenario}")
    return cfg


def run_gate(input_csv: Path, legacy_csv: Path, config_yaml: Path, out_dir: Path) -> dict[str, Any]:
    base = yaml.safe_load(config_yaml.read_text(encoding="utf-8-sig"))
    if not isinstance(base, dict):
        raise ValueError("config_yaml invalido")
    out_dir.mkdir(parents=True, exist_ok=True)
    scenarios = ["A_rule_only", "B_hybrid_calibrated", "C_hybrid_llm_router"]
    results: dict[str, Any] = {"scenarios": {}}
    for sc in scenarios:
        cfg = _scenario_cfg(base, sc)
        cfg_p = out_dir / f"{sc}.yaml"
        cfg_p.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
        audit_p = out_dir / f"{sc}_audit.csv"
        cmp_p = out_dir / f"{sc}_compare.json"
        run_audit(input_csv, audit_p, max_rows=2000, config_yaml=cfg_p, engine_version=f"gate-{sc}")
        cmp = run_compare_audit_vs_legacy(audit_csv=audit_p, legacy_csv=legacy_csv, out_json=cmp_p)
        ci = _bootstrap_match_ci(audit_p, legacy_csv)
        results["scenarios"][sc] = {
            "audit_csv": str(audit_p),
            "compare_json": str(cmp_p),
            "match_rate": cmp["match_rate"],
            "match_rate_ci95_bootstrap": ci,
            "n_joined": cmp["n_joined"],
            "confusion_matrix": cmp["confusion_matrix_legacy_vs_motor"],
        }
    a = Path(results["scenarios"]["A_rule_only"]["audit_csv"])
    b = Path(results["scenarios"]["B_hybrid_calibrated"]["audit_csv"])
    c = Path(results["scenarios"]["C_hybrid_llm_router"]["audit_csv"])
    results["paired_tests"] = {
        "B_vs_A_mcnemar": _mcnemar(a, b, legacy_csv),
        "C_vs_B_mcnemar": _mcnemar(b, c, legacy_csv),
    }
    mr_a = float(results["scenarios"]["A_rule_only"]["match_rate"])
    mr_b = float(results["scenarios"]["B_hybrid_calibrated"]["match_rate"])
    mr_c = float(results["scenarios"]["C_hybrid_llm_router"]["match_rate"])
    results["gate_decision"] = {
        "promote": mr_c >= mr_b >= mr_a,
        "selected": "C_hybrid_llm_router" if mr_c >= mr_b else "B_hybrid_calibrated",
        "delta_B_vs_A": round(mr_b - mr_a, 4),
        "delta_C_vs_B": round(mr_c - mr_b, 4),
    }
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Gate A/B/C Hepatologia")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--legacy-csv", type=Path, required=True)
    ap.add_argument("--config-yaml", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("_local_samples/exports/hepatologia_abc_gate"))
    ap.add_argument("--out-json", type=Path, default=None)
    args = ap.parse_args()
    res = run_gate(args.input_csv, args.legacy_csv, args.config_yaml, args.out_dir)
    out = args.out_json or (args.out_dir / "hepatologia_abc_gate.json")
    out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Gate salvo em: {out.resolve()}")


if __name__ == "__main__":
    main()
