"""S02 — ClinicalNlpEngine.process, to_plain e rule-based."""

from __future__ import annotations

import json

from nlp_engine import engine as engine_module
from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.text_pipeline import to_plain


def _full_input_row(exm_laudo_texto: str) -> dict[str, str]:
    return {
        "id_exame": "e1",
        "id_paciente": "p1",
        "id_unidade": "u1",
        "exm_laudo_texto": exm_laudo_texto,
        "exm_mod": "CT",
        "exm_tipo": "t1",
        "dt_exame": "2024-06-01",
    }


def test_process_sets_output_contract_stubs() -> None:
    eng = ClinicalNlpEngine(engine_version="9.8.7")
    rows = [_full_input_row(" texto simples ")]
    out = eng.process(rows, {}, specialty_id="colon", config_version="1.0.0")
    assert len(out) == 1
    r = out[0]
    assert r["specialty_id"] == "colon"
    assert r["config_version"] == "1.0.0"
    assert r["engine_version"] == "9.8.7"
    assert r["fl_relevante"] == 0
    assert r["confidence_score"] == 0.0
    payload = json.loads(r["exm_laudo_resultado"])
    assert payload["summary_compact"] == []
    assert payload["n_positive_spans"] == 0
    assert "uncertainty_band_hit" in payload
    assert "id_predicao" in r and r["id_predicao"]
    assert "dt_execucao" in r and r["dt_execucao"]
    assert r["id_exame"] == "e1"
    assert r["exm_laudo_texto_tratado"] == to_plain(" texto simples ")


def test_process_uses_laudo_when_exm_laudo_texto_blank() -> None:
    eng = ClinicalNlpEngine(engine_version="0.0.1")
    rows = [{"id_exame": "x", "exm_laudo_texto": "   ", "Laudo": "corpo"}]
    out = eng.process(rows, {})
    assert out[0]["exm_laudo_texto_tratado"] == to_plain("corpo")


def test_process_score_policy_v2_density() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "score_policy_version": "v2_density",
        "findings": {"a": ["um", "dois"]},
        "negation_phrases": [],
    }
    rows = [_full_input_row("um dois tres.")]
    out = eng.process(rows, cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload["score_policy_version"] == "v2_density"
    assert out[0]["confidence_score"] > 0.5


def test_process_positive_finding_updates_score_and_flag() -> None:
    eng = ClinicalNlpEngine(engine_version="2.0.0")
    cfg = {
        "findings": {"achado": ["lesao"]},
        "negation_phrases": ["sem"],
        "negation_window": 7,
    }
    rows = [_full_input_row("Exame com lesao expansiva.")]
    out = eng.process(rows, cfg)
    r = out[0]
    assert r["fl_relevante"] == 1
    assert r["confidence_score"] == 0.9
    payload = json.loads(r["exm_laudo_resultado"])
    assert payload["n_positive_spans"] >= 1


def test_process_applies_trailing_line_patterns_from_nlp_config() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    raw = "linha ok\nrodape x"
    rows = [_full_input_row(raw)]
    cfg = {"text_pipeline": {"trailing_line_patterns": [r"(?i)^rodape x$"]}}
    out = eng.process(rows, cfg)
    assert out[0]["exm_laudo_texto_tratado"] == to_plain(
        raw, trailing_line_patterns=[r"(?i)^rodape x$"]
    )


def test_process_segments_by_headers_first_when_available() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "findings": {"achado": ["lesao"]},
        "target_organs": ["figado"],
        "all_organs": {"figado": {"seeds": ["figado"]}},
        "header_aliases": {"figado": ["figado"]},
    }
    text = "FIGADO:\nFigado com lesao focal.\nBEXIGA:\nBexiga sem lesao."
    out = eng.process([_full_input_row(text)], cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload["segmentation_strategy"] == "headers"
    assert payload["n_positive_spans"] >= 1


def test_process_falls_back_to_anchors_when_no_header_found() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "findings": {"achado": ["lesao"]},
        "target_organs": ["figado"],
        "all_organs": {"figado": {"seeds": ["figado"]}},
        "header_aliases": {"figado": ["figado"]},
        "segmentation": {"mode": "auto"},
    }
    text = "Paciente com figado apresentando lesao focal."
    out = eng.process([_full_input_row(text)], cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload["segmentation_strategy"] == "anchors"
    assert payload["n_positive_spans"] >= 1


def test_process_embeddings_fallback_promotes_ambiguous_negative() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "findings": {"achado": ["tumor"]},
        "negation_phrases": [],
        "embeddings": {
            "use_embeddings": True,
            "decision_mode": "fallback",
            "embedding_backend": "token_overlap",
            "similarity_threshold": 0.30,
            "ambiguity_band": [0.0, 0.2],
            "semantic_terms": ["lesao focal hepatica"],
        },
    }
    rows = [_full_input_row("lesao focal hepatica em segmento vi")]
    out = eng.process(rows, cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert out[0]["fl_relevante"] == 1
    assert payload["decision_source"] == "embedding_fallback"
    assert payload["semantic_score"] >= 0.30


def test_process_embeddings_hybrid_sets_decision_source() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "findings": {"achado": ["nodulo hepatico"]},
        "negation_phrases": [],
        "embeddings": {
            "use_embeddings": True,
            "decision_mode": "hybrid",
            "embedding_backend": "token_overlap",
            "similarity_threshold": 0.25,
            "hybrid_weight_rule": 0.8,
            "hybrid_weight_semantic": 0.2,
            "semantic_terms": ["lesao focal hepatica"],
        },
    }
    rows = [_full_input_row("lesao focal hepatica observada no figado")]
    out = eng.process(rows, cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload["decision_source"] == "hybrid"
    assert 0.0 <= float(out[0]["confidence_score"]) <= 1.0


def test_process_embeddings_clips_semantic_score_to_unit_interval(monkeypatch) -> None:
    class _E:
        max_similarity = 1.7
        matched_term = "x"
        backend_used = "token_overlap"
        model_name = "mock"

    monkeypatch.setattr(engine_module, "semantic_evidence", lambda _t, _c: _E())
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "findings": {"achado": ["tumor"]},
        "embeddings": {
            "use_embeddings": True,
            "decision_mode": "fallback",
            "embedding_backend": "token_overlap",
            "similarity_threshold": 0.2,
            "ambiguity_band": [0.0, 0.4],
            "semantic_terms": ["lesao focal"],
        },
    }
    out = eng.process([_full_input_row("lesao focal hepatica")], cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload["semantic_score"] == 1.0
    assert 0.0 <= float(out[0]["confidence_score"]) <= 1.0


def test_process_llm_router_changes_decision_in_uncertain_band() -> None:
    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "findings": {"achado": ["hepatopatia cronica"]},
        "negation_phrases": [],
        "embeddings": {
            "use_embeddings": True,
            "decision_mode": "hybrid",
            "embedding_backend": "token_overlap",
            "semantic_terms": ["hepatopatia cronica"],
            "similarity_threshold": 0.2,
        },
        "llm_router": {
            "enabled": True,
                "uncertainty_band": [0.0, 1.0],
            "negative_context_patterns": [r"sem\\s+c[áa]lculos"],
        },
    }
    rows = [_full_input_row("vesicula biliar sem calculos. hepatopatia cronica.")]
    out = eng.process(rows, cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload["uncertainty_band_hit"] is True
    assert payload["decision_source"] in ("llm_router_block", "llm_router_no_change")
