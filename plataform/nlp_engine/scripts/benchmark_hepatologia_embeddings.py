#!/usr/bin/env python3
"""Benchmark A/B embeddings em Hepatologia: baseline vs MiniLM vs BioBERTpt."""

from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from pathlib import Path
from typing import Any

import yaml

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.audit_engine_from_csv import run_audit
from scripts.audit_legacy_compare import (
    legacy_s_n_from_row,
    read_rows_semico_first,
    run_compare_audit_vs_legacy,
)


def _wilson_ci(success: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return (0.0, 0.0)
    p = success / total
    den = 1 + (z * z) / total
    centre = p + (z * z) / (2 * total)
    margin = z * math.sqrt((p * (1 - p) + (z * z) / (4 * total)) / total)
    lo = max(0.0, (centre - margin) / den)
    hi = min(1.0, (centre + margin) / den)
    return (round(lo, 4), round(hi, 4))


def _fbeta2(tp: int, fp: int, fn: int) -> float:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    beta2 = 4.0
    den = beta2 * precision + recall
    if den == 0:
        return 0.0
    return round((1 + beta2) * precision * recall / den, 4)


def _build_metrics(compare_res: dict[str, Any]) -> dict[str, Any]:
    cm = compare_res["confusion_matrix_legacy_vs_motor"]
    tp = int(cm["legacy_S_motor_S"])
    fn = int(cm["legacy_S_motor_N"])
    fp = int(cm["legacy_N_motor_S"])
    tn = int(cm["legacy_N_motor_N"])
    n = int(compare_res["n_joined"])
    match = int(compare_res["n_match"])
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    return {
        "n_joined": n,
        "match_rate": compare_res["match_rate"],
        "match_rate_ci95": _wilson_ci(match, n),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "recall_ci95": _wilson_ci(tp, tp + fn) if (tp + fn) else (0.0, 0.0),
        "f_beta_2": _fbeta2(tp, fp, fn),
        "confusion_matrix": {"tp": tp, "fn": fn, "fp": fp, "tn": tn},
    }


def _mcnemar_vs_baseline(
    *,
    baseline_audit_csv: Path,
    candidate_audit_csv: Path,
    legacy_csv: Path,
) -> dict[str, Any]:
    def _k(row: dict[str, str]) -> str:
        pid = str(row.get("id_predicao", "")).strip()
        if pid:
            return f"PRED::{pid}"
        return f"EX::{str(row.get('id_exame', '')).strip()}"

    base = {_k(r): r for r in read_rows_semico_first(baseline_audit_csv) if _k(r)}
    cand = {_k(r): r for r in read_rows_semico_first(candidate_audit_csv) if _k(r)}
    legacy = {}
    for r in read_rows_semico_first(legacy_csv):
        pid = str((r.get("id_predicao") or r.get("idPredicao") or "")).strip()
        key = f"PRED::{pid}" if pid else ""
        if not key:
            ex = str((r.get("id_exame") or r.get("id_pct") or r.get("id") or "")).strip()
            key = f"EX::{ex}" if ex else ""
        if key:
            legacy[key] = r
    keys = sorted(set(base) & set(cand) & set(legacy))
    b = 0  # baseline correto, candidato incorreto
    c = 0  # baseline incorreto, candidato correto
    for k in keys:
        truth = legacy_s_n_from_row(legacy[k])
        base_ok = ("S" if str(base[k].get("fl_relevante", "")).strip() == "1" else "N") == truth
        cand_ok = ("S" if str(cand[k].get("fl_relevante", "")).strip() == "1" else "N") == truth
        if base_ok and not cand_ok:
            b += 1
        elif cand_ok and not base_ok:
            c += 1
    if b + c == 0:
        return {"b": b, "c": c, "chi2": 0.0, "significant_0_05": False}
    chi2 = ((abs(b - c) - 1) ** 2) / (b + c)
    return {"b": b, "c": c, "chi2": round(chi2, 4), "significant_0_05": chi2 >= 3.841}


def _config_for_scenario(base_cfg: dict[str, Any], *, model_name: str, use_embeddings: bool) -> dict[str, Any]:
    cfg = copy.deepcopy(base_cfg)
    nlp = cfg.setdefault("nlp", {})
    emb = nlp.setdefault("embeddings", {})
    emb["use_embeddings"] = use_embeddings
    emb["decision_mode"] = "fallback"
    emb["embedding_backend"] = "auto"
    emb["embedding_model"] = model_name
    emb["similarity_threshold"] = float(emb.get("similarity_threshold", 0.78))
    emb["ambiguity_band"] = emb.get("ambiguity_band", [0.30, 0.70])
    return cfg


def _apply_biobertpt_cli_overrides(
    cfg_raw: dict[str, Any],
    *,
    threshold: float | None,
    band: tuple[float, float] | None,
) -> None:
    if threshold is None and band is None:
        return
    nlp = cfg_raw.setdefault("nlp", {})
    emb = nlp.setdefault("embeddings", {})
    by_m = emb.setdefault("similarity_threshold_by_model", {})
    bio = by_m.setdefault("pucpr/biobertpt-all", {})
    if threshold is not None:
        bio["similarity_threshold"] = threshold
    if band is not None:
        bio["ambiguity_band"] = [band[0], band[1]]


def run_benchmark(
    *,
    input_csv: Path,
    expected_csv: Path,
    config_yaml: Path,
    out_dir: Path,
    biobertpt_threshold: float | None = None,
    biobertpt_band: tuple[float, float] | None = None,
) -> dict[str, Any]:
    cfg_raw = yaml.safe_load(config_yaml.read_text(encoding="utf-8-sig"))
    if not isinstance(cfg_raw, dict):
        raise ValueError("config_yaml invalido")
    _apply_biobertpt_cli_overrides(
        cfg_raw,
        threshold=biobertpt_threshold,
        band=biobertpt_band,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    scenarios = [
        ("baseline_no_embeddings", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", False),
        ("ab_minilm", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", True),
        ("ab_biobertpt", "pucpr/biobertpt-all", True),
    ]
    results: dict[str, Any] = {"scenarios": {}}
    baseline_audit: Path | None = None
    for scenario_name, model_name, use_emb in scenarios:
        scenario_cfg = _config_for_scenario(cfg_raw, model_name=model_name, use_embeddings=use_emb)
        cfg_path = out_dir / f"{scenario_name}.yaml"
        cfg_path.write_text(yaml.safe_dump(scenario_cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
        audit_csv = out_dir / f"{scenario_name}_audit.csv"
        compare_json = out_dir / f"{scenario_name}_compare.json"
        run_audit(
            input_csv,
            audit_csv,
            max_rows=100_000,
            config_yaml=cfg_path,
            engine_version=f"benchmark-{scenario_name}",
            validate_output_invariants=True,
        )
        cmp_res = run_compare_audit_vs_legacy(
            audit_csv=audit_csv,
            legacy_csv=expected_csv,
            out_json=compare_json,
        )
        metrics = _build_metrics(cmp_res)
        results["scenarios"][scenario_name] = {
            "model_name": model_name,
            "use_embeddings": use_emb,
            "audit_csv": str(audit_csv),
            "compare_json": str(compare_json),
            "metrics": metrics,
        }
        if scenario_name == "baseline_no_embeddings":
            baseline_audit = audit_csv
    if baseline_audit is None:
        raise RuntimeError("baseline nao gerado")
    base_fb2 = float(results["scenarios"]["baseline_no_embeddings"]["metrics"]["f_beta_2"])
    decisions: dict[str, Any] = {}
    for scenario_name in ("ab_minilm", "ab_biobertpt"):
        sc = results["scenarios"][scenario_name]
        fb2 = float(sc["metrics"]["f_beta_2"])
        delta_fb2 = round(fb2 - base_fb2, 4)
        mc = _mcnemar_vs_baseline(
            baseline_audit_csv=baseline_audit,
            candidate_audit_csv=Path(sc["audit_csv"]),
            legacy_csv=expected_csv,
        )
        accepted = delta_fb2 >= 0 and (
            mc["significant_0_05"] or float(sc["metrics"]["match_rate"]) >= float(results["scenarios"]["baseline_no_embeddings"]["metrics"]["match_rate"])
        )
        decisions[scenario_name] = {
            "delta_f_beta_2_vs_baseline": delta_fb2,
            "mcnemar_vs_baseline": mc,
            "decision": "aceite" if accepted else "ajuste",
        }
    results["acceptance_gate"] = decisions
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark Hepatologia embeddings A/B")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--expected-csv", type=Path, required=True)
    ap.add_argument("--config-yaml", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("_local_samples/exports/hepatologia_embeddings_ab"))
    ap.add_argument("--out-json", type=Path, default=None)
    ap.add_argument(
        "--biobertpt-threshold",
        type=float,
        default=None,
        help="Sobrescreve similarity_threshold apenas para pucpr/biobertpt-all (refino FP)",
    )
    ap.add_argument(
        "--biobertpt-band-lo",
        type=float,
        default=None,
        help="Com --biobertpt-band-hi: ambiguity_band do BioBERTpt",
    )
    ap.add_argument(
        "--biobertpt-band-hi",
        type=float,
        default=None,
        help="Com --biobertpt-band-lo: ambiguity_band do BioBERTpt",
    )
    args = ap.parse_args()
    band: tuple[float, float] | None = None
    if args.biobertpt_band_lo is not None or args.biobertpt_band_hi is not None:
        if args.biobertpt_band_lo is None or args.biobertpt_band_hi is None:
            ap.error("use --biobertpt-band-lo e --biobertpt-band-hi juntos")
        band = (args.biobertpt_band_lo, args.biobertpt_band_hi)
    result = run_benchmark(
        input_csv=args.input_csv,
        expected_csv=args.expected_csv,
        config_yaml=args.config_yaml,
        out_dir=args.out_dir,
        biobertpt_threshold=args.biobertpt_threshold,
        biobertpt_band=band,
    )
    out_json = args.out_json or (args.out_dir / "hepatologia_embeddings_benchmark.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Benchmark salvo em: {out_json.resolve()}")


if __name__ == "__main__":
    main()
