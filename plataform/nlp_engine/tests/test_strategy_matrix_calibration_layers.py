"""Valida YAML da submatriz de calibração em camadas (sem PHI)."""

from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[1]
_CAL = _ROOT / "configs" / "hepatologia" / "scenarios" / "strategy_matrix_calibration_layers.yaml"


def test_calibration_layers_yaml_unique_ids_and_baseline() -> None:
    raw = yaml.safe_load(_CAL.read_text(encoding="utf-8-sig"))
    assert isinstance(raw, dict)
    sc = raw.get("scenarios")
    assert isinstance(sc, list) and sc
    ids = [str(s.get("id", "")) for s in sc if isinstance(s, dict)]
    assert all(ids)
    assert len(ids) == len(set(ids))
    assert "baseline" in ids
    assert "S5_hybrid_calibrated" in ids
    assert "S5_hybrid_calibrated_llm_pilot" in ids
    assert any(i.startswith("S5_hybrid_sim0") for i in ids)
    assert raw.get("matrix_spec", {}).get("baseline_scenario_id") == "baseline"
