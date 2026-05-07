"""Integracao rule-based: YAML minimo + invariantes (sem PHI)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from nlp_engine.config_loader import merge_with_shared_organs
from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.output_invariants import validate_engine_output_row

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "engine_integration_minimal.yaml"


def _merged_config() -> tuple[dict, str, str]:
    raw = yaml.safe_load(_FIXTURE.read_text(encoding="utf-8-sig"))
    merged = merge_with_shared_organs(None, raw)
    nlp = dict(merged["nlp"])
    return (
        nlp,
        str(merged["specialty_id"]),
        str(merged["config_version"]),
    )


def test_integration_headers_and_invariants() -> None:
    nlp, sid, cv = _merged_config()
    eng = ClinicalNlpEngine(engine_version="integ-test")
    row = {
        "id_exame": "e1",
        "exm_laudo_texto": "FIGADO:\nFigado com lesao focal.\nBEXIGA:\nBexiga sem achados.",
    }
    out = eng.process([row], nlp, specialty_id=sid, config_version=cv)
    assert len(out) == 1
    assert validate_engine_output_row(out[0]) == []
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload.get("segmentation_strategy") == "headers"
    assert payload.get("n_positive_spans", 0) >= 1


def test_integration_negation_and_proximity() -> None:
    nlp, sid, cv = _merged_config()
    nlp = dict(nlp)
    nlp["finding_organ_max_chars"] = 5
    eng = ClinicalNlpEngine(engine_version="integ-test")
    neg = {"id_exame": "e2", "exm_laudo_texto": "Figado sem lesao patologica."}
    out_neg = eng.process([neg], nlp, specialty_id=sid, config_version=cv)
    assert validate_engine_output_row(out_neg[0]) == []
    p_neg = json.loads(out_neg[0]["exm_laudo_resultado"])
    assert p_neg["n_positive_spans"] == 0
    assert p_neg["n_negated_spans"] >= 1

    distant = "Figado homogeneo " + ("x " * 50) + "nodulo periferico"
    out_dist = eng.process(
        [{"id_exame": "e3", "exm_laudo_texto": distant}],
        nlp,
        specialty_id=sid,
        config_version=cv,
    )
    assert validate_engine_output_row(out_dist[0]) == []
    p_dist = json.loads(out_dist[0]["exm_laudo_resultado"])
    assert p_dist["n_positive_spans"] == 0
