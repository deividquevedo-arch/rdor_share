"""Testes do router LLM / deterministic (sem rede)."""

from __future__ import annotations

import json

import pytest

from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.llm_router_backend import decide_deterministic_regex, llm_router_mode


def test_llm_router_mode_default_deterministic() -> None:
    assert llm_router_mode({}) == "deterministic"
    assert llm_router_mode({"llm_router": {"mode": "LLM"}}) == "llm"


def test_parse_llm_json_relevante() -> None:
    from nlp_engine.llm_router_backend import _parse_llm_json

    assert _parse_llm_json('{"relevante": true}') == (1, "llm_relevante")
    assert _parse_llm_json('{"relevante": false}') == (0, "llm_relevante")


def test_llm_router_step_llm_mode_mocked(monkeypatch: pytest.MonkeyPatch) -> None:
    import nlp_engine.llm_router_backend as lb

    def fake_call(*, cfg: dict, messages: list) -> tuple[str, str | None]:
        return '{"relevante": true}', None

    monkeypatch.setattr(lb, "call_openai_compatible_chat", fake_call)

    cfg = {
        "findings": {"x": ["test"]},
        "embeddings": {
            "use_embeddings": True,
            "decision_mode": "hybrid",
            "embedding_backend": "token_overlap",
            "semantic_terms": ["test"],
            "similarity_threshold": 0.01,
        },
        "feature_flags": {"calibrated_hybrid": True},
        "llm_router": {
            "enabled": True,
            "mode": "llm",
            "uncertainty_band": [0.0, 1.0],
            "provider": "openai_compatible",
            "base_url": "https://example.invalid/v1",
            "model": "stub-model",
            "api_key_env": "NLP_ENGINE_LLM_API_KEY",
        },
    }
    fl, src, band, meta = lb.llm_router_step(
        treated="qualquer texto sintetico sem phi",
        current_fl=0,
        calibrated_score=0.5,
        nlp_config=cfg,
    )
    assert fl == 1
    assert src == "llm_router_llm_positive"
    assert band is True
    assert meta["llm_called"] is True
    assert meta["llm_router_mode"] == "llm"


def test_llm_router_step_llm_fallback_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import nlp_engine.llm_router_backend as lb

    def fake_call(**kwargs: object) -> tuple[str, str | None]:
        return "", "http_500"

    monkeypatch.setattr(lb, "call_openai_compatible_chat", fake_call)

    cfg = {
        "findings": {"x": ["test"]},
        "embeddings": {
            "use_embeddings": True,
            "decision_mode": "hybrid",
            "embedding_backend": "token_overlap",
            "semantic_terms": ["test"],
            "similarity_threshold": 0.01,
        },
        "feature_flags": {"calibrated_hybrid": True},
        "llm_router": {
            "enabled": True,
            "mode": "llm",
            "uncertainty_band": [0.0, 1.0],
            "base_url": "https://example.invalid/v1",
            "model": "x",
            "api_key_env": "NLP_ENGINE_LLM_API_KEY",
        },
    }
    fl, src, band, meta = lb.llm_router_step(
        treated="texto",
        current_fl=1,
        calibrated_score=0.5,
        nlp_config=cfg,
    )
    assert fl == 1
    assert src == "llm_router_llm_fallback"
    assert meta["llm_error"] == "http_500"


def test_engine_end_to_end_llm_mode_mocked(monkeypatch: pytest.MonkeyPatch) -> None:
    import nlp_engine.llm_router_backend as lb

    monkeypatch.setattr(
        lb,
        "call_openai_compatible_chat",
        lambda **kw: ('{"relevante": false}', None),
    )

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
        "feature_flags": {"calibrated_hybrid": True},
        "llm_router": {
            "enabled": True,
            "mode": "llm",
            "uncertainty_band": [0.0, 1.0],
            "base_url": "https://example.invalid/v1",
            "model": "stub",
            "api_key_env": "UNUSED",
        },
    }
    rows = [
        {
            "id_exame": "SYN-E2E-LLM",
            "id_paciente": "P-SYN",
            "id_unidade": "U-SYN",
            "exm_laudo_texto": "hepatopatia cronica.",
            "exm_mod": "SYN",
            "exm_tipo": "test",
            "dt_exame": "2026-05-05",
        }
    ]
    out = eng.process(rows, cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert payload.get("llm_called") is True
    assert payload["decision_source"] == "llm_router_llm_negative"
    assert out[0]["fl_relevante"] == 0


def test_build_messages_includes_specialty_placeholders() -> None:
    from nlp_engine.llm_router_backend import _build_messages

    cfg = {
        "specialty_context": "SYN_CTX",
        "prompt_user_template": "id={specialty_id}|ctx={specialty_context}|t={text}",
    }
    msgs = _build_messages(cfg, "SYN_BODY", specialty_id="syn_specialty")
    user = msgs[1]["content"]
    assert "syn_specialty" in user
    assert "SYN_CTX" in user
    assert "SYN_BODY" in user


def test_deterministic_unchanged_from_extract() -> None:
    cfg = {
        "llm_router": {
            "enabled": True,
            "mode": "deterministic",
            "uncertainty_band": [0.0, 1.0],
            "negative_context_patterns": [r"sem\s+c[áa]lculos"],
        }
    }
    fl, src, hit = decide_deterministic_regex(
        treated="vesicula sem calculos",
        current_fl=1,
        calibrated_score=0.5,
        nlp_config=cfg,
    )
    assert fl == 0
    assert src == "llm_router_block"
    assert hit is True
