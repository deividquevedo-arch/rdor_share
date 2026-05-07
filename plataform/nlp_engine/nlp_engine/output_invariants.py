"""Invariantes minimas da linha de saida do motor (T02.4 local; schema completo em monitoring)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

_JSON_REQUIRED = (
    "summary_compact",
    "n_positive_spans",
    "n_negated_spans",
    "rule_engine_version",
    "score_policy_version",
    "decision_source",
    "uncertainty_band_hit",
    "semantic_score",
    "semantic_matched_term",
    "semantic_backend",
    "embedding_model",
    "llm_router_mode",
    "llm_called",
    "llm_model",
    "llm_error",
)
_SEGMENTATION_STRATEGIES = frozenset({"headers", "anchors", "full_doc"})
_DECISION_SOURCES = frozenset(
    {
        "rule",
        "hybrid",
        "hybrid_calibrated",
        "embedding_fallback",
        "llm_router_block",
        "llm_router_promote",
        "llm_router_no_change",
        "llm_router_llm_fallback",
        "llm_router_llm_positive",
        "llm_router_llm_negative",
        "llm_router_llm_abstain_empty",
        "llm_router_llm_abstain_invalid_json",
        "llm_router_llm_abstain_not_object",
        "llm_router_llm_abstain_relevante",
        "llm_router_llm_abstain_unknown_keys",
        "llm_router_llm_llm_abstain",
        "disabled",
    }
)
_LLM_ROUTER_MODES = frozenset({"deterministic", "llm"})

_REQUIRED = (
    "id_predicao",
    "dt_execucao",
    "specialty_id",
    "config_version",
    "engine_version",
    "fl_relevante",
    "confidence_score",
    "exm_laudo_resultado",
    "exm_laudo_texto_tratado",
)


def validate_exm_laudo_resultado_json(raw: str) -> list[str]:
    """Estrutura minima do JSON serializado em ``exm_laudo_resultado`` (S02 contrato v0)."""
    errs: list[str] = []
    if not isinstance(raw, str) or not raw.strip():
        errs.append("exm_laudo_resultado_empty")
        return errs
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        errs.append("exm_laudo_resultado_invalid_json")
        return errs
    if not isinstance(payload, dict):
        errs.append("exm_laudo_resultado_not_object")
        return errs
    for key in _JSON_REQUIRED:
        if key not in payload:
            errs.append(f"exm_laudo_resultado_missing:{key}")
    sc = payload.get("summary_compact")
    if sc is not None and not isinstance(sc, list):
        errs.append("summary_compact_not_list")
    elif isinstance(sc, list) and any(not isinstance(x, str) for x in sc):
        errs.append("summary_compact_entries_not_str")
    for nk in ("n_positive_spans", "n_negated_spans"):
        if nk not in payload:
            continue
        v = payload[nk]
        if not isinstance(v, int) or v < 0:
            errs.append(f"{nk}_invalid")
    for sk in ("rule_engine_version", "score_policy_version"):
        if sk not in payload:
            continue
        val = payload[sk]
        if not isinstance(val, str):
            errs.append(f"{sk}_not_str")
    seg = payload.get("segmentation_strategy")
    if seg is not None:
        if not isinstance(seg, str) or seg not in _SEGMENTATION_STRATEGIES:
            errs.append("segmentation_strategy_invalid")
    ds = payload.get("decision_source")
    if ds is not None:
        if not isinstance(ds, str) or ds not in _DECISION_SOURCES:
            errs.append("decision_source_invalid")
    if "uncertainty_band_hit" in payload and not isinstance(payload["uncertainty_band_hit"], bool):
        errs.append("uncertainty_band_hit_not_bool")
    for sk in ("semantic_score",):
        if sk in payload:
            try:
                v = float(payload[sk])
                if not 0.0 <= v <= 1.0:
                    errs.append(f"{sk}_out_of_range")
            except (TypeError, ValueError):
                errs.append(f"{sk}_not_numeric")
    for kk in ("semantic_matched_term", "semantic_backend", "embedding_model"):
        if kk in payload and not isinstance(payload[kk], str):
            errs.append(f"{kk}_not_str")
    if "llm_router_mode" in payload:
        mode = payload["llm_router_mode"]
        if not isinstance(mode, str) or mode not in _LLM_ROUTER_MODES:
            errs.append("llm_router_mode_invalid")
    for lk in ("llm_router_mode", "llm_model", "llm_error"):
        if lk not in payload:
            continue
        lv = payload[lk]
        if lv is not None and not isinstance(lv, str):
            errs.append(f"{lk}_not_str")
    if "llm_called" in payload and not isinstance(payload["llm_called"], bool):
        errs.append("llm_called_not_bool")
    return errs


def validate_engine_output_row(row: Mapping[str, Any]) -> list[str]:
    """Lista vazia se OK; entradas descrevem violacoes (para testes e notebook)."""
    errs: list[str] = []
    for key in _REQUIRED:
        if key not in row:
            errs.append(f"missing_field:{key}")
    if "confidence_score" in row:
        try:
            v = float(row["confidence_score"])
            if not 0.0 <= v <= 1.0:
                errs.append("confidence_score_out_of_range")
        except (TypeError, ValueError):
            errs.append("confidence_score_not_numeric")
    if "fl_relevante" in row and row["fl_relevante"] not in (0, 1):
        errs.append("fl_relevante_not_binary")
    if "exm_laudo_resultado" in row:
        raw = row["exm_laudo_resultado"]
        if not isinstance(raw, str) or not raw.strip():
            errs.append("exm_laudo_resultado_empty")
        else:
            errs.extend(validate_exm_laudo_resultado_json(raw))
    return errs
