"""Invariantes locais da saida do motor (Gate D leve)."""

from __future__ import annotations

import json

from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.output_invariants import (
    validate_engine_output_row,
    validate_exm_laudo_resultado_json,
)


def test_validate_engine_output_row_accepts_full_process_row() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    out = eng.process(
        [
            {
                "id_exame": "e",
                "id_paciente": "p",
                "id_unidade": "u",
                "exm_laudo_texto": "ok",
                "exm_mod": "m",
                "exm_tipo": "t",
                "dt_exame": "2024-01-01",
            }
        ],
        {},
        specialty_id="hepato",
        config_version="1.0.0",
    )
    assert validate_engine_output_row(out[0]) == []


def test_validate_exm_laudo_resultado_json_accepts_minimal() -> None:
    raw = json.dumps(
        {
            "summary_compact": [],
            "n_positive_spans": 0,
            "n_negated_spans": 0,
            "rule_engine_version": "",
            "score_policy_version": "v1_bins_legacy",
            "decision_source": "rule",
            "uncertainty_band_hit": False,
            "semantic_score": 0.0,
            "semantic_matched_term": "",
            "semantic_backend": "",
            "embedding_model": "",
            "llm_router_mode": "deterministic",
            "llm_called": False,
            "llm_model": "",
            "llm_error": "",
        }
    )
    assert validate_exm_laudo_resultado_json(raw) == []


def test_validate_exm_laudo_resultado_json_rejects_bad_segmentation() -> None:
    raw = json.dumps(
        {
            "summary_compact": [],
            "n_positive_spans": 0,
            "n_negated_spans": 0,
            "rule_engine_version": "x",
            "score_policy_version": "v1_bins_legacy",
            "segmentation_strategy": "invalid",
            "decision_source": "rule",
            "uncertainty_band_hit": False,
            "semantic_score": 0.0,
            "semantic_matched_term": "",
            "semantic_backend": "",
            "embedding_model": "",
            "llm_router_mode": "deterministic",
            "llm_called": False,
            "llm_model": "",
            "llm_error": "",
        }
    )
    errs = validate_exm_laudo_resultado_json(raw)
    assert any("segmentation_strategy" in e for e in errs)


def test_validate_engine_output_row_detects_bad_score() -> None:
    row = {
        "id_predicao": "x",
        "dt_execucao": "t",
        "specialty_id": "",
        "config_version": "",
        "engine_version": "1",
        "fl_relevante": 0,
        "confidence_score": 1.5,
        "exm_laudo_resultado": json.dumps({}),
        "exm_laudo_texto_tratado": "",
    }
    assert any("confidence" in e for e in validate_engine_output_row(row))


def test_validate_exm_laudo_resultado_json_rejects_invalid_decision_source() -> None:
    raw = json.dumps(
        {
            "summary_compact": [],
            "n_positive_spans": 0,
            "n_negated_spans": 0,
            "rule_engine_version": "",
            "score_policy_version": "v1_bins_legacy",
            "decision_source": "unknown",
            "uncertainty_band_hit": False,
            "semantic_score": 0.0,
            "semantic_matched_term": "",
            "semantic_backend": "",
            "embedding_model": "",
            "llm_router_mode": "deterministic",
            "llm_called": False,
            "llm_model": "",
            "llm_error": "",
        }
    )
    errs = validate_exm_laudo_resultado_json(raw)
    assert "decision_source_invalid" in errs
