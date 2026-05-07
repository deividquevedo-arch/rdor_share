from __future__ import annotations

from nlp_engine.semantic_expand import (
    effective_embeddings_ambiguity_band,
    effective_similarity_threshold,
    embeddings_ambiguity_band,
    embeddings_decision_mode,
    embeddings_enabled,
    embeddings_hybrid_weights,
    embeddings_similarity_threshold,
    semantic_evidence,
)


def test_semantic_evidence_token_overlap_uses_findings_when_no_terms() -> None:
    cfg = {
        "findings": {"lesao": ["lesao focal hepatica"]},
        "embeddings": {
            "use_embeddings": True,
            "embedding_backend": "token_overlap",
            "embedding_model": "dummy",
        },
    }
    ev = semantic_evidence("exame com lesao focal hepatica em segmento", cfg)
    assert ev.backend_used == "token_overlap"
    assert ev.max_similarity > 0.3
    assert ev.matched_term


def test_embeddings_helpers_apply_defaults_and_bounds() -> None:
    cfg = {
        "embeddings": {
            "use_embeddings": "true",
            "decision_mode": "hybrid",
            "similarity_threshold": 2.0,
            "ambiguity_band": [0.8, 0.2],
            "hybrid_weight_rule": 2,
            "hybrid_weight_semantic": 1,
        }
    }
    assert embeddings_enabled(cfg)
    assert embeddings_decision_mode(cfg) == "hybrid"
    assert embeddings_similarity_threshold(cfg) == 1.0
    assert embeddings_ambiguity_band(cfg) == (0.2, 0.8)
    wr, ws = embeddings_hybrid_weights(cfg)
    assert round(wr + ws, 6) == 1.0
    assert wr > ws


def test_effective_threshold_and_band_per_model() -> None:
    cfg = {
        "embeddings": {
            "embedding_model": "pucpr/biobertpt-all",
            "similarity_threshold": 0.78,
            "ambiguity_band": [0.3, 0.7],
            "similarity_threshold_by_model": {
                "pucpr/biobertpt-all": {
                    "similarity_threshold": 0.88,
                    "ambiguity_band": [0.4, 0.6],
                }
            },
        }
    }
    assert effective_similarity_threshold(cfg) == 0.88
    assert effective_embeddings_ambiguity_band(cfg) == (0.4, 0.6)


def test_effective_defaults_when_no_override_for_model() -> None:
    cfg = {
        "embeddings": {
            "embedding_model": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
            "similarity_threshold": 0.78,
            "ambiguity_band": [0.3, 0.7],
            "similarity_threshold_by_model": {
                "pucpr/biobertpt-all": {"similarity_threshold": 0.88}
            },
        }
    }
    assert effective_similarity_threshold(cfg) == 0.78
    assert effective_embeddings_ambiguity_band(cfg) == (0.3, 0.7)
