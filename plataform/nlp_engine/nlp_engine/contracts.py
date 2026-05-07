"""Tipos de contrato documentais (TypedDict) para o motor rule-based — T02.1.

Nao substituem validacao em runtime; ``ClinicalNlpEngine.process`` aceita ``Mapping`` por compatibilidade.
Ver ``docs/motor-nlp/doc-contrato-engine-rule-based-v0.md``.
"""

from __future__ import annotations

from typing import Literal, TypedDict

DecisionSource = Literal[
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
]


class EngineInputRow(TypedDict, total=False):
    """Linha de entrada tipica para ``process`` (campos repassados se presentes)."""

    id_exame: str
    id_paciente: str
    id_unidade: str
    exm_laudo_texto: str
    Laudo: str
    exm_mod: str
    exm_tipo: str
    dt_exame: str


class ExmLaudoResultadoPayload(TypedDict, total=False):
    """JSON minimo em ``exm_laudo_resultado`` (string JSON na linha de saida)."""

    summary_compact: list[str]
    n_positive_spans: int
    n_negated_spans: int
    rule_engine_version: str
    score_policy_version: str
    segmentation_strategy: str
    decision_source: DecisionSource
    uncertainty_band_hit: bool
    semantic_score: float
    semantic_matched_term: str
    semantic_backend: str
    embedding_model: str
    llm_router_mode: Literal["deterministic", "llm"]
    llm_called: bool
    llm_model: str
    llm_error: str


class EngineOutputRow(TypedDict, total=False):
    """Linha de saida do motor (apos ``process``)."""

    id_exame: str
    id_paciente: str
    id_unidade: str
    exm_laudo_texto: str
    exm_mod: str
    exm_tipo: str
    dt_exame: str
    id_predicao: str
    dt_execucao: str
    specialty_id: str
    config_version: str
    engine_version: str
    fl_relevante: int
    confidence_score: float
    exm_laudo_resultado: str
    exm_laudo_texto_tratado: str
