"""Fumo do runner de matriz de cenários (sem PHI)."""

from __future__ import annotations

from pathlib import Path

import yaml

from scripts.run_hepatologia_diamond_bench import _resolve_inputs
from scripts.run_hepatologia_strategy_matrix import _pick_winner, recompute_promotion_from_report, run_matrix

_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_IN = _ROOT / "_local_samples" / "standard" / "hepatologia" / "hepatologia_standard_input.csv"
_DEFAULT_LEGACY = _ROOT / "_local_samples" / "standard" / "hepatologia" / "hepatologia_standard_expected.csv"
_IN = _ROOT / "tests" / "fixtures" / "hepatologia_e2e_input.csv"
_EXP = _ROOT / "tests" / "fixtures" / "hepatologia_e2e_expected.csv"
_BASE = _ROOT / "configs" / "hepatologia" / "config.yaml"


def test_strategy_matrix_runner_smoke(tmp_path: Path) -> None:
    spec = {
        "matrix_spec": {"baseline_scenario_id": "baseline"},
        "scenarios": [
            {"id": "baseline", "description": "ref", "strategies": [], "config_patch": {}},
            {
                "id": "S3_chars_180",
                "description": "proximidade",
                "strategies": ["S3"],
                "config_patch": {"nlp": {"finding_organ_max_chars": 180}},
            },
        ],
    }
    scen = tmp_path / "matrix_min.yaml"
    scen.write_text(yaml.safe_dump(spec, allow_unicode=True), encoding="utf-8")
    out_dir = tmp_path / "out"
    res = run_matrix(
        input_csv=_IN,
        legacy_csv=_EXP,
        base_config_yaml=_BASE,
        scenarios_yaml=scen,
        out_dir=out_dir,
        max_rows=10,
        bootstrap_n=50,
        bootstrap_seed=3,
    )
    assert len(res["scenarios"]) == 2
    assert res["scenarios"][0]["scenario_id"] == "baseline"
    assert "winner_under_promotion_rules" in res
    assert "paired_vs_baseline" in res
    assert "S3_chars_180" in res["paired_vs_baseline"]
    m123 = res["scenarios"][0].get("metrics_by_cod_123")
    assert isinstance(m123, dict)
    assert "by_cod" in m123 and "aggregate_sn_on_labeled" in m123
    obs = res["scenarios"][0]["observability"]
    assert "uncertainty" in obs
    assert "llm_observability" in obs
    unc = obs["uncertainty"]
    assert isinstance(unc.get("high_uncertainty_cases_top"), list)
    assert isinstance(unc.get("low_uncertainty_cases_top"), list)
    assert len(unc.get("high_uncertainty_cases_top", [])) <= 50
    assert len(unc.get("low_uncertainty_cases_top", [])) <= 10
    llm = obs["llm_observability"]
    assert "llm_called_rate" in llm
    assert "llm_error_rate" in llm


def test_strategy_matrix_only_scenarios_filter(tmp_path: Path) -> None:
    spec = {
        "matrix_spec": {"baseline_scenario_id": "baseline"},
        "scenarios": [
            {"id": "baseline", "strategies": [], "config_patch": {}},
            {
                "id": "S3_chars_180",
                "strategies": ["S3"],
                "config_patch": {"nlp": {"finding_organ_max_chars": 180}},
            },
            {
                "id": "S4_neg_9",
                "strategies": ["S4"],
                "config_patch": {"nlp": {"negation_window": 9}},
            },
        ],
    }
    scen = tmp_path / "matrix_three.yaml"
    scen.write_text(yaml.safe_dump(spec, allow_unicode=True), encoding="utf-8")
    out_dir = tmp_path / "out2"
    res = run_matrix(
        input_csv=_IN,
        legacy_csv=_EXP,
        base_config_yaml=_BASE,
        scenarios_yaml=scen,
        out_dir=out_dir,
        max_rows=10,
        bootstrap_n=20,
        bootstrap_seed=1,
        only_scenario_ids=frozenset({"S3_chars_180"}),
    )
    ids = {r["scenario_id"] for r in res["scenarios"]}
    assert ids == {"baseline", "S3_chars_180"}
    assert "S4_neg_9" not in res["paired_vs_baseline"]


def test_strategy_matrix_runner_only_cod_123_input_flag(tmp_path: Path) -> None:
    spec = {
        "matrix_spec": {"baseline_scenario_id": "baseline"},
        "scenarios": [{"id": "baseline", "strategies": [], "config_patch": {}}],
    }
    scen = tmp_path / "matrix_cod123.yaml"
    scen.write_text(yaml.safe_dump(spec, allow_unicode=True), encoding="utf-8")
    out_dir = tmp_path / "out_cod123"
    res = run_matrix(
        input_csv=_IN,
        legacy_csv=_EXP,
        base_config_yaml=_BASE,
        scenarios_yaml=scen,
        out_dir=out_dir,
        max_rows=10,
        bootstrap_n=20,
        bootstrap_seed=1,
        only_cod_123=True,
    )
    assert res["inputs"]["only_cod_123"] is True


def test_resolve_inputs_explicit_paths_win(tmp_path: Path) -> None:
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("x\n", encoding="utf-8")
    b.write_text("y\n", encoding="utf-8")
    i, l = _resolve_inputs(
        from_query_validate=False,
        input_csv=a,
        legacy_csv=b,
    )
    assert i == a and l == b


def _row(sid: str, mr: float, fp: int, fn: int) -> dict:
    return {
        "scenario_id": sid,
        "match_rate": mr,
        "compare": {
            "confusion_matrix_legacy_vs_motor": {
                "legacy_N_motor_S": fp,
                "legacy_S_motor_N": fn,
            }
        },
    }


def test_pick_winner_fp_ceiling_prefers_lower_fp_at_same_mr() -> None:
    rows = [
        _row("baseline", 0.5, 228, 742),
        _row("A", 0.5, 220, 800),
        _row("B", 0.52, 250, 700),
    ]
    w = _pick_winner(rows, baseline_id="baseline", profile="fp_ceiling", promotion_block={})
    assert w is not None
    assert w["scenario_id"] == "A"


def test_recompute_promotion_from_report_uses_repo_yaml(tmp_path: Path) -> None:
    yaml_p = _ROOT / "configs" / "hepatologia" / "scenarios" / "strategy_matrix.yaml"
    report = {
        "matrix_spec": {"baseline_scenario_id": "baseline"},
        "inputs": {},
        "scenarios": [
            _row("baseline", 0.515, 228, 742),
            _row("S5_hybrid_calibrated", 0.741, 424, 94),
        ],
    }
    out = recompute_promotion_from_report(
        report,
        profile="fn_priority",
        scenarios_yaml=yaml_p,
    )
    assert out["promotion_profile"] == "fn_priority"
    assert out["winner_under_promotion_rules"]["scenario_id"] == "S5_hybrid_calibrated"


def test_pick_winner_fn_priority_min_fn_within_fp_budget() -> None:
    rows = [
        _row("baseline", 0.515, 228, 742),
        _row("S5", 0.741, 424, 94),
        _row("S5b", 0.742, 404, 112),
    ]
    prom = {"fn_priority": {"fp_ratio_max": 2.0, "require_match_rate_gte_baseline": True, "require_fn_lt_baseline": True}}
    w = _pick_winner(rows, baseline_id="baseline", profile="fn_priority", promotion_block=prom)
    assert w is not None
    assert w["scenario_id"] == "S5"
    assert w["fn"] == 94
    assert w["fp_cap_applied"] == 456


def test_resolve_inputs_from_query_validate_flag(tmp_path: Path, monkeypatch) -> None:
    q = tmp_path / "query_hepato_validate.csv"
    q.write_text("h\n", encoding="utf-8")
    monkeypatch.setenv("NLP_HEPATO_QUERY_VALIDATE", str(q))
    i, l = _resolve_inputs(
        from_query_validate=True,
        input_csv=_DEFAULT_IN,
        legacy_csv=_DEFAULT_LEGACY,
    )
    assert i == l == q
