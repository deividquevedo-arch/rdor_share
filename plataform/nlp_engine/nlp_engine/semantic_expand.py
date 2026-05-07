"""Embeddings / expansao semantica (Fase 2, opcional via flag no config)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from math import isfinite
from typing import Any

from nlp_engine.text_pipeline.norm import norm


@dataclass(frozen=True)
class SemanticEvidence:
    max_similarity: float
    matched_term: str
    backend_used: str
    model_name: str


def _as_float(raw: Any, default: float) -> float:
    try:
        f = float(raw)
    except (TypeError, ValueError):
        return default
    if not isfinite(f):
        return default
    return f


def _sentence_chunks(text: str) -> list[str]:
    out = [p.strip() for p in re.split(r"[.!?\n;]+", text or "") if p.strip()]
    if out:
        return out
    t = (text or "").strip()
    return [t] if t else []


def _token_set(text: str) -> set[str]:
    return {t for t in norm(text).split() if len(t) >= 3}


def _jaccard_similarity(a: str, b: str) -> float:
    sa = _token_set(a)
    sb = _token_set(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    if union == 0:
        return 0.0
    return inter / union


def _semantic_terms(nlp_config: dict[str, Any]) -> list[str]:
    emb = nlp_config.get("embeddings")
    if isinstance(emb, dict):
        raw_terms = emb.get("semantic_terms")
        if isinstance(raw_terms, (list, tuple)):
            terms = [str(x).strip() for x in raw_terms if str(x).strip()]
            if terms:
                return terms
    findings = nlp_config.get("findings")
    if not isinstance(findings, dict):
        return []
    flat: list[str] = []
    for terms in findings.values():
        if not isinstance(terms, (list, tuple)):
            continue
        flat.extend(str(x).strip() for x in terms if str(x).strip())
    return flat


@lru_cache(maxsize=4)
def _load_sentence_model(model_name: str) -> Any:
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_name)


@lru_cache(maxsize=128)
def _cached_term_embeddings(model_name: str, terms_key: tuple[str, ...]) -> tuple[list[str], Any]:
    model = _load_sentence_model(model_name)
    vectors = model.encode(list(terms_key), convert_to_tensor=True, normalize_embeddings=True)
    return list(terms_key), vectors


def _evidence_with_sentence_transformers(
    text: str,
    *,
    model_name: str,
    terms: list[str],
) -> SemanticEvidence:
    from sentence_transformers import util

    chunks = _sentence_chunks(text)
    if not chunks or not terms:
        return SemanticEvidence(0.0, "", "sentence_transformers", model_name)
    model = _load_sentence_model(model_name)
    terms_key = tuple(terms)
    term_labels, term_vec = _cached_term_embeddings(model_name, terms_key)
    chunk_vec = model.encode(chunks, convert_to_tensor=True, normalize_embeddings=True)
    sim_m = util.cos_sim(chunk_vec, term_vec)
    best = sim_m.max()
    row, col = divmod(int(sim_m.argmax().item()), sim_m.shape[1])
    _ = row  # row currently not exposed; reserved for future audit by trecho
    return SemanticEvidence(float(best.item()), term_labels[int(col)], "sentence_transformers", model_name)


def _evidence_with_token_overlap(text: str, *, model_name: str, terms: list[str]) -> SemanticEvidence:
    best_score = 0.0
    best_term = ""
    for chunk in _sentence_chunks(text):
        for term in terms:
            s = _jaccard_similarity(chunk, term)
            if s > best_score:
                best_score = s
                best_term = term
    return SemanticEvidence(best_score, best_term, "token_overlap", model_name)


def semantic_evidence(text: str, nlp_config: dict[str, Any]) -> SemanticEvidence:
    emb = nlp_config.get("embeddings")
    emb_cfg = emb if isinstance(emb, dict) else {}
    model_name = str(
        emb_cfg.get("embedding_model", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    ).strip()
    backend = str(emb_cfg.get("embedding_backend", "auto")).strip().lower()
    terms = _semantic_terms(nlp_config)
    if not terms:
        return SemanticEvidence(0.0, "", "none", model_name)
    if backend == "token_overlap":
        return _evidence_with_token_overlap(text, model_name=model_name, terms=terms)
    try:
        return _evidence_with_sentence_transformers(text, model_name=model_name, terms=terms)
    except Exception:
        return _evidence_with_token_overlap(text, model_name=model_name, terms=terms)


def embeddings_enabled(nlp_config: dict[str, Any]) -> bool:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return False
    v = emb.get("use_embeddings", False)
    return str(v).strip().lower() in ("1", "true", "yes")


def embeddings_decision_mode(nlp_config: dict[str, Any]) -> str:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return "fallback"
    mode = str(emb.get("decision_mode", "fallback")).strip().lower()
    return mode if mode in {"fallback", "hybrid"} else "fallback"


def embeddings_similarity_threshold(nlp_config: dict[str, Any]) -> float:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return 0.78
    return max(0.0, min(1.0, _as_float(emb.get("similarity_threshold"), 0.78)))


def _embedding_model_id(nlp_config: dict[str, Any]) -> str:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return ""
    return str(emb.get("embedding_model", "")).strip()


def _per_model_override(nlp_config: dict[str, Any]) -> Mapping[str, Any] | None:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return None
    mid = _embedding_model_id(nlp_config)
    if not mid:
        return None
    by_m = emb.get("similarity_threshold_by_model")
    if not isinstance(by_m, dict) or mid not in by_m:
        return None
    raw = by_m.get(mid)
    return raw if isinstance(raw, Mapping) else None


def effective_similarity_threshold(nlp_config: dict[str, Any]) -> float:
    """Threshold base + refinamento opcional por ``embedding_model``."""
    base = embeddings_similarity_threshold(nlp_config)
    ov = _per_model_override(nlp_config)
    if ov is None or "similarity_threshold" not in ov:
        return base
    return max(0.0, min(1.0, _as_float(ov.get("similarity_threshold"), base)))


def embeddings_ambiguity_band(nlp_config: dict[str, Any]) -> tuple[float, float]:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return (0.3, 0.7)
    raw = emb.get("ambiguity_band")
    if not isinstance(raw, (list, tuple)) or len(raw) != 2:
        return (0.3, 0.7)
    lo = max(0.0, min(1.0, _as_float(raw[0], 0.3)))
    hi = max(0.0, min(1.0, _as_float(raw[1], 0.7)))
    return (min(lo, hi), max(lo, hi))


def effective_embeddings_ambiguity_band(nlp_config: dict[str, Any]) -> tuple[float, float]:
    """Faixa de ambiguidade base + refinamento opcional por modelo."""
    lo, hi = embeddings_ambiguity_band(nlp_config)
    ov = _per_model_override(nlp_config)
    if ov is None or "ambiguity_band" not in ov:
        return (lo, hi)
    raw = ov.get("ambiguity_band")
    if not isinstance(raw, (list, tuple)) or len(raw) != 2:
        return (lo, hi)
    lo2 = max(0.0, min(1.0, _as_float(raw[0], lo)))
    hi2 = max(0.0, min(1.0, _as_float(raw[1], hi)))
    return (min(lo2, hi2), max(lo2, hi2))


def embeddings_hybrid_weights(nlp_config: dict[str, Any]) -> tuple[float, float]:
    emb = nlp_config.get("embeddings")
    if not isinstance(emb, dict):
        return (0.7, 0.3)
    wr = max(0.0, _as_float(emb.get("hybrid_weight_rule"), 0.7))
    ws = max(0.0, _as_float(emb.get("hybrid_weight_semantic"), 0.3))
    if wr == 0 and ws == 0:
        return (0.7, 0.3)
    s = wr + ws
    return (wr / s, ws / s)
