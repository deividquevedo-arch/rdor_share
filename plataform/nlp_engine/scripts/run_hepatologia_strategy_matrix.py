#!/usr/bin/env python3
"""Corre matriz de cenários YAML (sem treino) vs baseline: audit + compare + McNemar + bootstrap.

Lê `configs/hepatologia/scenarios/strategy_matrix.yaml` (ou ficheiro indicado), faz deep-merge
de cada `config_patch` sobre o YAML base, gera `hepatologia_strategy_matrix.json`.
"""

from __future__ import annotations

import argparse
import copy
import json
import random
import sys
import time
from collections import Counter
from statistics import median
from pathlib import Path
from typing import Any, Mapping

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


def _gold_cod_lead(row: dict[str, str]) -> str:
    c = str(row.get("cod_achado_relevante", "") or "").strip()[:1]
    return c if c in ("1", "2", "3") else "other"


def _motor_pred_sn(row: dict[str, str]) -> str:
    return "S" if str(row.get("fl_relevante", "")).strip() == "1" else "N"


def _metrics_by_cod_123(audit_csv: Path, legacy_csv: Path) -> dict[str, Any]:
    """Métricas por classe gold 1/2/3 no pareamento audit↔legacy (chaves comuns)."""
    audit = {_key(r): r for r in read_rows_semico_first(audit_csv) if _key(r)}
    legacy: dict[str, dict[str, str]] = {}
    for r in read_rows_semico_first(legacy_csv):
        k = _key(r)
        if not k:
            ex = str((r.get("id_exame") or r.get("id_pct") or r.get("id") or "")).strip()
            k = f"EX::{ex}" if ex else ""
        if k:
            legacy[k] = r
    keys = sorted(set(audit) & set(legacy))
    cod_counts: dict[str, int] = {"1": 0, "2": 0, "3": 0}
    matrix: dict[str, dict[str, int]] = {
        c: {"n": 0, "motor_S": 0, "motor_N": 0, "correct": 0, "wrong": 0} for c in ("1", "2", "3")
    }
    tp = fn = fp = tn = 0
    n_labeled = 0
    for k in keys:
        leg = legacy[k]
        lead = _gold_cod_lead(leg)
        if lead == "other":
            continue
        n_labeled += 1
        cod_counts[lead] = cod_counts.get(lead, 0) + 1
        pred = _motor_pred_sn(audit[k])
        truth = legacy_s_n_from_row(leg)
        bucket = matrix[lead]
        bucket["n"] += 1
        if pred == "S":
            bucket["motor_S"] += 1
        else:
            bucket["motor_N"] += 1
        if pred == truth:
            bucket["correct"] += 1
            if truth == "S":
                tp += 1
            else:
                tn += 1
        else:
            bucket["wrong"] += 1
            if truth == "S":
                fn += 1
            else:
                fp += 1
    by_cod_out: dict[str, Any] = {}
    for c in ("1", "2", "3"):
        d = matrix[c]
        n = d["n"]
        acc_c = round(d["correct"] / n, 4) if n else None
        by_cod_out[c] = {**d, "accuracy_class": acc_c}
    denom_p = tp + fn
    denom_n = tn + fp
    return {
        "n_labeled_123": n_labeled,
        "cod_counts": cod_counts,
        "by_cod": by_cod_out,
        "aggregate_sn_on_labeled": {
            "tp": tp,
            "fn": fn,
            "fp": fp,
            "tn": tn,
            "accuracy": round((tp + tn) / n_labeled, 4) if n_labeled else 0.0,
            "recall_positive": round(tp / denom_p, 4) if denom_p else None,
            "recall_negative": round(tn / denom_n, 4) if denom_n else None,
        },
    }


def _mcnemar(audit_a: Path, audit_b: Path, legacy_csv: Path) -> dict[str, Any]:
    a = {_key(r): r for r in read_rows_semico_first(audit_a) if _key(r)}
    b = {_key(r): r for r in read_rows_semico_first(audit_b) if _key(r)}
    legacy: dict[str, dict[str, str]] = {}
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


def _bootstrap_match_ci(
    audit_csv: Path, legacy_csv: Path, n_boot: int = 400, seed: int = 7
) -> tuple[float, float]:
    audit = {_key(r): r for r in read_rows_semico_first(audit_csv) if _key(r)}
    legacy: dict[str, dict[str, str]] = {}
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


def _deep_merge(base: Any, patch: Any) -> Any:
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            if k in out and isinstance(out[k], dict) and isinstance(v, dict):
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = copy.deepcopy(v)
        return out
    return copy.deepcopy(patch)


def _fp_fn(cmp: dict[str, Any]) -> tuple[int, int]:
    cm = cmp.get("confusion_matrix_legacy_vs_motor") or {}
    fp = int(cm.get("legacy_N_motor_S", 0))
    fn = int(cm.get("legacy_S_motor_N", 0))
    return fp, fn


def _to_float(raw: Any) -> float | None:
    try:
        if raw is None:
            return None
        val = float(str(raw).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    if val < 0.0:
        return 0.0
    if val > 1.0:
        return 1.0
    return val


def _uncertainty_from_confidence(confidence_score: float | None) -> float | None:
    if confidence_score is None:
        return None
    # 0.5 = maior incerteza; 0.0/1.0 = menor incerteza.
    return round(1.0 - (2.0 * abs(confidence_score - 0.5)), 6)


def _parse_jsonish(raw: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str):
        return None
    txt = raw.strip()
    if not txt:
        return None
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return None


def _pick_first(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj.get(key)
        for v in obj.values():
            got = _pick_first(v, key)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for item in obj:
            got = _pick_first(item, key)
            if got is not None:
                return got
    return None


def _extract_llm_observability(row: dict[str, str]) -> dict[str, Any]:
    containers = [
        _parse_jsonish(row.get("summary_compact_json")),
        _parse_jsonish(row.get("exm_laudo_resultado")),
    ]
    llm_called = None
    llm_error = None
    decision_source = None
    llm_model = None
    llm_router_mode = None
    for cont in containers:
        if cont is None:
            continue
        if llm_called is None:
            llm_called = _pick_first(cont, "llm_called")
        if llm_error is None:
            llm_error = _pick_first(cont, "llm_error")
        if decision_source is None:
            decision_source = _pick_first(cont, "decision_source")
        if llm_model is None:
            llm_model = _pick_first(cont, "llm_model")
        if llm_router_mode is None:
            llm_router_mode = _pick_first(cont, "llm_router_mode")

    as_bool = str(llm_called).strip().lower() in {"1", "true", "t", "yes", "y"}
    err_txt = str(llm_error or "").strip()
    return {
        "llm_called": as_bool,
        "llm_error": err_txt,
        "decision_source": str(decision_source or "NA"),
        "llm_model": str(llm_model or ""),
        "llm_router_mode": str(llm_router_mode or ""),
    }


def _build_observability(audit_csv: Path) -> dict[str, Any]:
    rows = read_rows_semico_first(audit_csv)
    unc_cases: list[dict[str, Any]] = []
    unc_values: list[float] = []
    llm_called_n = 0
    llm_error_n = 0
    decision_src = Counter()
    llm_models = Counter()
    router_modes = Counter()

    for r in rows:
        conf = _to_float(r.get("confidence_score"))
        unc = _uncertainty_from_confidence(conf)
        if unc is not None:
            unc_values.append(unc)
            unc_cases.append(
                {
                    "id_exame": str(r.get("id_exame", "")),
                    "id_predicao": str(r.get("id_predicao", "")),
                    "confidence_score": conf,
                    "uncertainty_score": unc,
                }
            )

        llm = _extract_llm_observability(r)
        if llm["llm_called"]:
            llm_called_n += 1
        if llm["llm_error"]:
            llm_error_n += 1
        decision_src[llm["decision_source"]] += 1
        if llm["llm_model"]:
            llm_models[llm["llm_model"]] += 1
        if llm["llm_router_mode"]:
            router_modes[llm["llm_router_mode"]] += 1

    unc_sorted_desc = sorted(unc_cases, key=lambda x: x["uncertainty_score"], reverse=True)
    unc_sorted_asc = sorted(unc_cases, key=lambda x: x["uncertainty_score"])
    n_rows = len(rows)
    unc_global = {
        "count_with_confidence": len(unc_values),
        "mean_uncertainty": round(sum(unc_values) / len(unc_values), 4) if unc_values else None,
        "median_uncertainty": round(median(unc_values), 4) if unc_values else None,
        "max_uncertainty": round(max(unc_values), 4) if unc_values else None,
        "min_uncertainty": round(min(unc_values), 4) if unc_values else None,
    }
    return {
        "uncertainty": {
            "global": unc_global,
            "high_uncertainty_cases_top": unc_sorted_desc[:50],
            "low_uncertainty_cases_top": unc_sorted_asc[:10],
        },
        "llm_observability": {
            "rows": n_rows,
            "llm_called": llm_called_n,
            "llm_called_rate": round(llm_called_n / n_rows, 4) if n_rows else 0.0,
            "llm_error": llm_error_n,
            "llm_error_rate": round(llm_error_n / n_rows, 4) if n_rows else 0.0,
            "decision_source_distribution": dict(decision_src),
            "llm_model_distribution": dict(llm_models),
            "llm_router_mode_distribution": dict(router_modes),
        },
    }


def format_summary_table(res: dict[str, Any], *, baseline_id: str) -> str:
    """Tabela legivel: match_rate, FP/FN vs legado, delta match vs baseline, McNemar."""
    base_mr: float | None = None
    base_fp = base_fn = 0
    for r in res.get("scenarios", []):
        if r.get("scenario_id") == baseline_id:
            base_mr = float(r["match_rate"])
            base_fp, base_fn = _fp_fn(r["compare"])
            break
    lines = [
        "scenario_id | match_rate | FP(N->S) | FN(S->N) | d_mr_vs_base | mcnemar_sig_0.05 | sec",
        "--------------+------------+----------+-----------+--------------+------------------+-----",
    ]
    paired = res.get("paired_vs_baseline") or {}
    for r in res.get("scenarios", []):
        sid = str(r.get("scenario_id", ""))
        mr = float(r["match_rate"])
        fp, fn = _fp_fn(r["compare"])
        dmr = "" if base_mr is None or sid == baseline_id else f"{round(mr - base_mr, 4):+}"
        mcn = ""
        sec = str(r.get("elapsed_seconds", ""))
        if sid != baseline_id and sid in paired:
            mcn = "yes" if paired[sid].get("mcnemar_vs_baseline", {}).get("significant_0_05") else "no"
        elif sid == baseline_id:
            mcn = "-"
            dmr = "-"
        lines.append(f"{sid[:28]:28} | {mr:10.4f} | {fp:8} | {fn:9} | {dmr:12} | {mcn:16} | {sec}")
    lines.append("")
    lines.append("observability (por cenário):")
    for r in res.get("scenarios", []):
        sid = str(r.get("scenario_id", ""))
        obs = r.get("observability") if isinstance(r.get("observability"), dict) else {}
        unc = obs.get("uncertainty") if isinstance(obs.get("uncertainty"), dict) else {}
        unc_global = unc.get("global") if isinstance(unc.get("global"), dict) else {}
        llm = obs.get("llm_observability") if isinstance(obs.get("llm_observability"), dict) else {}
        hi = len(unc.get("high_uncertainty_cases_top", [])) if isinstance(unc.get("high_uncertainty_cases_top"), list) else 0
        lo = len(unc.get("low_uncertainty_cases_top", [])) if isinstance(unc.get("low_uncertainty_cases_top"), list) else 0
        lines.append(
            f"- {sid[:28]} unc_mean={unc_global.get('mean_uncertainty')} "
            f"high_top={hi} low_top={lo} llm_called_rate={llm.get('llm_called_rate')} "
            f"llm_error_rate={llm.get('llm_error_rate')}"
        )
        m123 = r.get("metrics_by_cod_123") if isinstance(r.get("metrics_by_cod_123"), dict) else {}
        agg = (
            m123.get("aggregate_sn_on_labeled")
            if isinstance(m123.get("aggregate_sn_on_labeled"), dict)
            else {}
        )
        if agg and m123.get("n_labeled_123"):
            lines.append(
                f"  cod123 n={m123.get('n_labeled_123')} acc={agg.get('accuracy')} "
                f"rec_pos={agg.get('recall_positive')} rec_neg={agg.get('recall_negative')}"
            )
    w = res.get("winner_under_promotion_rules")
    prof = str((res.get("promotion") or {}).get("active_profile", ""))
    lines.append("")
    lines.append(f"promotion_profile={prof} baseline_fp={base_fp} baseline_fn={base_fn} winner={w}")
    return "\n".join(lines)


def _promotion_profile(
    matrix_spec: Mapping[str, Any],
    override: str | None,
) -> str:
    prom = matrix_spec.get("promotion") if isinstance(matrix_spec.get("promotion"), dict) else {}
    raw = (override or "").strip() or str(prom.get("active_profile") or "fp_ceiling").strip()
    if raw not in ("fp_ceiling", "fn_priority"):
        return "fp_ceiling"
    return raw


def _pick_winner(
    rows: list[dict[str, Any]],
    *,
    baseline_id: str,
    profile: str,
    promotion_block: Mapping[str, Any],
) -> dict[str, Any] | None:
    base = next((r for r in rows if r["scenario_id"] == baseline_id), None)
    if not base:
        return None
    b_mr = float(base["match_rate"])
    b_fp, b_fn = _fp_fn(base["compare"])
    candidates: list[dict[str, Any]] = []
    for r in rows:
        if r["scenario_id"] == baseline_id:
            continue
        mr = float(r["match_rate"])
        fp, fn = _fp_fn(r["compare"])
        candidates.append({"scenario_id": r["scenario_id"], "match_rate": mr, "fp": fp, "fn": fn})

    if profile == "fn_priority":
        fcfg = promotion_block.get("fn_priority") if isinstance(promotion_block.get("fn_priority"), dict) else {}
        ratio = float(fcfg.get("fp_ratio_max", 2.0))
        if ratio <= 0:
            ratio = 2.0
        fp_cap = int(b_fp * ratio)
        req_mr = bool(fcfg.get("require_match_rate_gte_baseline", True))
        req_fn = bool(fcfg.get("require_fn_lt_baseline", True))
        eligible: list[dict[str, Any]] = []
        for c in candidates:
            if fp_cap >= 0 and c["fp"] > fp_cap:
                continue
            if req_mr and c["match_rate"] < b_mr:
                continue
            if req_fn and c["fn"] >= b_fn:
                continue
            eligible.append(dict(c))
        if not eligible:
            return None
        eligible.sort(key=lambda x: (x["fn"], -x["match_rate"], x["fp"]))
        w = eligible[0]
        return {
            **w,
            "promotion_profile": profile,
            "fp_cap_applied": fp_cap,
            "baseline_fp": b_fp,
            "baseline_fn": b_fn,
        }

    # fp_ceiling (default)
    picked: list[dict[str, Any]] = []
    for c in candidates:
        if c["match_rate"] >= b_mr and c["fp"] <= b_fp:
            picked.append(dict(c))
    if not picked:
        return None
    picked.sort(key=lambda x: (-x["match_rate"], x["fp"], x["fn"]))
    w = picked[0]
    return {**w, "promotion_profile": "fp_ceiling", "baseline_fp": b_fp, "baseline_fn": b_fn}


def recompute_promotion_from_report(
    report: Mapping[str, Any],
    *,
    profile: str,
    scenarios_yaml: Path | None = None,
) -> dict[str, Any]:
    """Recalcula `winner_under_promotion_rules` a partir de JSON ja emitido (sem reauditar).

    Usa `report['scenarios']` (match_rate + compare.confusion_matrix) e regras em
    ``scenarios_yaml`` → ``matrix_spec.promotion`` (ex.: ``fn_priority.fp_ratio_max``).
    """
    if profile not in ("fp_ceiling", "fn_priority"):
        raise ValueError(f"perfil invalido: {profile!r} (use fp_ceiling ou fn_priority)")

    matrix_spec = report.get("matrix_spec") if isinstance(report.get("matrix_spec"), dict) else {}
    baseline_id = str(matrix_spec.get("baseline_scenario_id") or "baseline")
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("report sem lista 'scenarios'")

    yaml_path = scenarios_yaml
    if yaml_path is None:
        raw_in = report.get("inputs")
        if isinstance(raw_in, dict):
            p = raw_in.get("scenarios_yaml")
            if isinstance(p, str) and p.strip():
                yaml_path = Path(p)
        if yaml_path is None:
            yaml_path = _ROOT / "configs" / "hepatologia" / "scenarios" / "strategy_matrix.yaml"

    if not yaml_path.is_file():
        raise FileNotFoundError(f"scenarios_yaml nao encontrado: {yaml_path}")

    spec = yaml.safe_load(yaml_path.read_text(encoding="utf-8-sig"))
    ms = spec.get("matrix_spec") if isinstance(spec.get("matrix_spec"), dict) else {}
    prom_block = ms.get("promotion") if isinstance(ms.get("promotion"), dict) else {}

    winner = _pick_winner(
        list(scenarios),
        baseline_id=baseline_id,
        profile=profile,
        promotion_block=prom_block,
    )
    return {
        "baseline_id": baseline_id,
        "promotion_profile": profile,
        "scenarios_yaml_used": str(yaml_path.resolve()),
        "winner_under_promotion_rules": winner,
    }


def run_matrix(
    *,
    input_csv: Path,
    legacy_csv: Path,
    base_config_yaml: Path,
    scenarios_yaml: Path,
    out_dir: Path,
    max_rows: int,
    bootstrap_n: int,
    bootstrap_seed: int,
    only_scenario_ids: frozenset[str] | None = None,
    promotion_profile_override: str | None = None,
    only_cod_123: bool = False,
) -> dict[str, Any]:
    raw_spec = yaml.safe_load(scenarios_yaml.read_text(encoding="utf-8-sig"))
    if not isinstance(raw_spec, dict):
        raise ValueError("scenarios_yaml invalido")
    matrix_spec = raw_spec.get("matrix_spec") or {}
    scenario_defs = raw_spec.get("scenarios")
    if not isinstance(scenario_defs, list):
        raise ValueError("scenarios_yaml deve conter lista 'scenarios'")

    base_cfg = yaml.safe_load(base_config_yaml.read_text(encoding="utf-8-sig"))
    if not isinstance(base_cfg, dict):
        raise ValueError("base_config_yaml invalido")
    base_cv = str(base_cfg.get("config_version", "unknown"))

    out_dir.mkdir(parents=True, exist_ok=True)
    cfg_dir = out_dir / "generated_configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)

    rows_out: list[dict[str, Any]] = []
    baseline_id = str(matrix_spec.get("baseline_scenario_id") or "baseline")

    for sc in scenario_defs:
        if not isinstance(sc, dict):
            continue
        sid = str(sc.get("id", "")).strip()
        if not sid:
            continue
        if only_scenario_ids is not None and sid != baseline_id and sid not in only_scenario_ids:
            continue
        patch = sc.get("config_patch") or {}
        if not isinstance(patch, dict):
            patch = {}
        merged = _deep_merge(base_cfg, patch)
        merged["config_version"] = f"{base_cv}+strategy.{sid}"

        cfg_p = cfg_dir / f"{sid}.yaml"
        cfg_p.write_text(yaml.safe_dump(merged, allow_unicode=True, sort_keys=False), encoding="utf-8")
        audit_p = out_dir / f"{sid}_audit.csv"
        cmp_p = out_dir / f"{sid}_compare.json"

        t0 = time.perf_counter()
        run_audit(
            input_csv,
            audit_p,
            max_rows=max_rows,
            config_yaml=cfg_p,
            engine_version=f"strategy-matrix-{sid}",
        )
        cmp = run_compare_audit_vs_legacy(
            audit_csv=audit_p,
            legacy_csv=legacy_csv,
            out_json=cmp_p,
            only_cod_123=only_cod_123,
        )
        ci = _bootstrap_match_ci(audit_p, legacy_csv, n_boot=bootstrap_n, seed=bootstrap_seed)
        elapsed = round(time.perf_counter() - t0, 2)

        row: dict[str, Any] = {
            "scenario_id": sid,
            "description": sc.get("description", ""),
            "strategies": sc.get("strategies", []),
            "audit_csv": str(audit_p.resolve()),
            "compare_json": str(cmp_p.resolve()),
            "match_rate": cmp.get("match_rate"),
            "match_rate_ci95_bootstrap": {"lo": ci[0], "hi": ci[1], "n_boot": bootstrap_n},
            "n_joined": cmp.get("n_joined"),
            "elapsed_seconds": elapsed,
            "legacy_distribution": cmp.get("legacy_distribution"),
            "motor_distribution": cmp.get("motor_distribution"),
            "compare": {
                "status": cmp.get("status"),
                "confusion_matrix_legacy_vs_motor": cmp.get("confusion_matrix_legacy_vs_motor"),
            },
            "observability": _build_observability(audit_p),
            "metrics_by_cod_123": _metrics_by_cod_123(audit_p, legacy_csv),
        }
        llm_obs = (
            row.get("observability", {}).get("llm_observability", {})
            if isinstance(row.get("observability"), dict)
            else {}
        )
        row["ab_metrics_minimal"] = {
            "match_rate": row.get("match_rate"),
            "fp": _fp_fn(row["compare"])[0],
            "fn": _fp_fn(row["compare"])[1],
            "metrics_by_cod_123_accuracy": (
                (row.get("metrics_by_cod_123") or {})
                .get("aggregate_sn_on_labeled", {})
                .get("accuracy")
            ),
            "llm_called_rate": llm_obs.get("llm_called_rate"),
            "llm_error_rate": llm_obs.get("llm_error_rate"),
        }
        rows_out.append(row)

    baseline_audit = out_dir / f"{baseline_id}_audit.csv"
    if not baseline_audit.is_file():
        raise FileNotFoundError(f"baseline audit em falta: {baseline_audit}")

    paired: dict[str, Any] = {}
    for r in rows_out:
        sid = r["scenario_id"]
        if sid == baseline_id:
            continue
        ap = Path(r["audit_csv"])
        paired[sid] = {
            "mcnemar_vs_baseline": _mcnemar(baseline_audit, ap, legacy_csv),
        }

    prom_block = matrix_spec.get("promotion") if isinstance(matrix_spec.get("promotion"), dict) else {}
    profile = _promotion_profile(matrix_spec, promotion_profile_override)
    winner = _pick_winner(
        rows_out,
        baseline_id=baseline_id,
        profile=profile,
        promotion_block=prom_block,
    )

    return {
        "ab_playbook": {
            "version": "v1",
            "pipeline": ["audit", "compare", "matrix", "promotion"],
            "required_metrics": [
                "match_rate",
                "fp",
                "fn",
                "metrics_by_cod_123.accuracy",
                "llm_called_rate",
                "llm_error_rate",
                "mcnemar.significant_0_05",
            ],
        },
        "matrix_spec": matrix_spec,
        "promotion": {
            "active_profile": profile,
            "override_from_cli": promotion_profile_override is not None,
            "winner_under_promotion_rules": winner,
        },
        "inputs": {
            "input_csv": str(input_csv.resolve()),
            "legacy_csv": str(legacy_csv.resolve()),
            "base_config_yaml": str(base_config_yaml.resolve()),
            "scenarios_yaml": str(scenarios_yaml.resolve()),
            "max_rows": max_rows,
            "only_cod_123": only_cod_123,
        },
        "scenarios": rows_out,
        "paired_vs_baseline": paired,
        "winner_under_promotion_rules": winner,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Matriz de estrategias Hepatologia (YAML-only, sem treino)")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--legacy-csv", type=Path, required=True)
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
    ap.add_argument("--out-dir", type=Path, default=_ROOT / "_local_samples" / "exports" / "hepatologia_strategy_matrix")
    ap.add_argument("--out-json", type=Path, default=None)
    ap.add_argument("--max-rows", type=int, default=2000)
    ap.add_argument("--bootstrap-n", type=int, default=400)
    ap.add_argument("--bootstrap-seed", type=int, default=7)
    ap.add_argument(
        "--only-scenarios",
        type=str,
        default="",
        help="Lista separada por virgulas de ids (sempre inclui baseline). Ex.: baseline,S5_hybrid_calibrated",
    )
    ap.add_argument(
        "--print-table",
        action="store_true",
        help="Imprime tabela-resumo no stdout apos o JSON.",
    )
    ap.add_argument(
        "--promotion-profile",
        type=str,
        default=None,
        metavar="fp_ceiling|fn_priority",
        help="Sobrescreve matrix_spec.promotion.active_profile (omitir = usar YAML).",
    )
    ap.add_argument(
        "--only-cod-123",
        action="store_true",
        help="Compara apenas linhas do gold com cod_achado_relevante iniciando em 1/2/3.",
    )
    args = ap.parse_args()

    only: frozenset[str] | None = None
    if args.only_scenarios.strip():
        only = frozenset(x.strip() for x in args.only_scenarios.split(",") if x.strip())

    prom_raw = (args.promotion_profile or "").strip()
    if prom_raw and prom_raw not in ("fp_ceiling", "fn_priority"):
        raise SystemExit("--promotion-profile deve ser fp_ceiling ou fn_priority")
    prom_override = prom_raw or None

    res = run_matrix(
        input_csv=args.input_csv,
        legacy_csv=args.legacy_csv,
        base_config_yaml=args.base_config_yaml,
        scenarios_yaml=args.scenarios_yaml,
        out_dir=args.out_dir,
        max_rows=args.max_rows,
        bootstrap_n=args.bootstrap_n,
        bootstrap_seed=args.bootstrap_seed,
        only_scenario_ids=only,
        promotion_profile_override=prom_override,
        only_cod_123=args.only_cod_123,
    )
    out = args.out_json or (args.out_dir / "hepatologia_strategy_matrix.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Relatorio: {out.resolve()}")
    if args.print_table:
        bid = str((res.get("matrix_spec") or {}).get("baseline_scenario_id") or "baseline")
        print()
        print(format_summary_table(res, baseline_id=bid))


if __name__ == "__main__":
    main()
