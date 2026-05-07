"""E2E sintetico: CSV estilo gold Hive (sem PHI) -> build -> audit -> compare -> mismatch 0."""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from scripts.audit_engine_from_csv import run_audit
from scripts.audit_legacy_compare import legacy_s_n_from_row, run_compare_audit_vs_legacy
from scripts.build_hepatologia_standard_sample import run_build

_ROOT = Path(__file__).resolve().parents[2]
_HCFG = _ROOT / "configs" / "hepatologia" / "config.yaml"

_TEXTO_S = "Fígado com nódulo hipodense de 15 mm no segmento VI."
_TEXTO_N = "Fígado homogeneo, sem nódulos ou lesoes focais."


def _write_hive_synth(path: Path) -> None:
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=[
            "dataExecucaoModelo",
            "idPredicao",
            "idExame",
            "idPaciente",
            "laudoExame",
            "laudoTokens",
            "analiseBlocos",
            "flgRelevante",
        ],
    )
    w.writeheader()
    w.writerow(
        {
            "dataExecucaoModelo": "2026-01-01",
            "idPredicao": "p1",
            "idExame": "EX-S1",
            "idPaciente": "P-S1",
            "laudoExame": _TEXTO_S,
            "laudoTokens": "[]",
            "analiseBlocos": "[]",
            "flgRelevante": "TRUE",
        }
    )
    w.writerow(
        {
            "dataExecucaoModelo": "2026-01-01",
            "idPredicao": "p2",
            "idExame": "EX-S2",
            "idPaciente": "P-S2",
            "laudoExame": _TEXTO_N,
            "laudoTokens": "[]",
            "analiseBlocos": "[]",
            "flgRelevante": "FALSE",
        }
    )
    path.write_text(buf.getvalue(), encoding="utf-8-sig")


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"cod_achado_relevante": "1 - Tem Doença Fígado"}, "S"),
        ({"cod_achado_relevante": "2 - Sim (Mas Não Tem Doença Fígado)"}, "S"),
        ({"cod_achado_relevante": "3 - Não"}, "N"),
        ({"expected_encaminhamento": "S"}, "S"),
        ({"flgRelevante": "TRUE"}, "S"),
        ({"flgRelevante": "FALSE"}, "N"),
        ({"exm_encaminhamento_nlp": "0"}, "N"),
        ({"fl_relevante": "1"}, "S"),
        ({"fl_relevante": "0"}, "N"),
    ],
)
def test_legacy_s_n_from_row_gold_hive(
    row: dict[str, str], expected: str
) -> None:
    assert legacy_s_n_from_row(row) == expected


def test_hive_synth_e2e_build_audit_compare_match(tmp_path: Path) -> None:
    if not _HCFG.is_file():
        pytest.skip("configs/hepatologia/config.yaml ausente")

    hive = tmp_path / "synth_hive_gold.csv"
    _write_hive_synth(hive)

    out_dir = tmp_path / "std"
    summary = run_build(
        source_legacy_csv=hive, out_dir=out_dir, max_rows=None, source="gold_hive"
    )
    assert summary["source"] == "gold_hive"
    assert summary["sample_rows"] == 2

    audit_p = tmp_path / "audit_out.csv"
    n = run_audit(
        out_dir / "hepatologia_standard_input.csv",
        audit_p,
        max_rows=100,
        config_yaml=_HCFG,
        engine_version="e2e-hive-synth",
        validate_output_invariants=True,
    )
    assert n == 2

    res = run_compare_audit_vs_legacy(
        audit_csv=audit_p,
        legacy_csv=out_dir / "hepatologia_standard_expected.csv",
        out_json=None,
    )
    assert res["status"] == "ok", res
    assert res["n_joined"] == 2
    assert res["n_mismatch"] == 0, res.get("mismatch_examples", [])
