"""E2E: input CSV -> audit (motor + YAML hepatologia) -> compare vs expected (sem PHI).

Alinha com a cadeia manual: `audit_engine_from_csv.py` + `compare_hepatologia_audit_vs_legacy.py`.
Textos e gold em `tests/fixtures/hepatologia_e2e_*.csv` calibrados com o motor (ver nota S06).
"""

from __future__ import annotations

from pathlib import Path

from scripts.audit_engine_from_csv import run_audit
from scripts.audit_legacy_compare import run_compare_audit_vs_legacy

_ROOT = Path(__file__).resolve().parents[2]
_CFG = _ROOT / "configs" / "hepatologia" / "config.yaml"
_IN = _ROOT / "tests" / "fixtures" / "hepatologia_e2e_input.csv"
_EXP = _ROOT / "tests" / "fixtures" / "hepatologia_e2e_expected.csv"


def test_hepatologia_e2e_audit_matches_expected_fixture(tmp_path: Path) -> None:
    out = tmp_path / "hepatologia_e2e_audit.csv"
    n = run_audit(
        _IN,
        out,
        max_rows=100,
        config_yaml=_CFG,
        engine_version="e2e-hepatologia",
        validate_output_invariants=True,
    )
    assert n == 5

    res = run_compare_audit_vs_legacy(audit_csv=out, legacy_csv=_EXP, out_json=None)
    assert res["status"] == "ok"
    assert res["n_joined"] == 5
    assert res["n_mismatch"] == 0, res.get("mismatch_examples", [])
    assert res["n_match"] == 5
