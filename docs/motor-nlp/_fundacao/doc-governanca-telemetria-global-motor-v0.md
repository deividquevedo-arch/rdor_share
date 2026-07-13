# Governanca e Telemetria Global do Motor (v0)

Objetivo: padronizar rastreabilidade no `nlp_engine` para qualquer especialidade sem lógica clínica hardcoded.

## Campos obrigatórios por linha
- `specialty_id`
- `config_version`
- `engine_version`
- `fl_relevante`
- `confidence_score`
- `exm_laudo_resultado` (JSON válido)

## Campos obrigatórios no JSON `exm_laudo_resultado`
- Núcleo: `summary_compact`, `n_positive_spans`, `n_negated_spans`, `rule_engine_version`, `score_policy_version`
- Decisão: `decision_source`, `uncertainty_band_hit`
- Semântico: `semantic_score`, `semantic_matched_term`, `semantic_backend`, `embedding_model`
- Router LLM: `llm_router_mode`, `llm_called`, `llm_model`, `llm_error`

## Taxonomia canônica de `decision_source`
- `rule`
- `hybrid`
- `hybrid_calibrated`
- `embedding_fallback`
- `llm_router_block`
- `llm_router_promote`
- `llm_router_no_change`
- `llm_router_llm_fallback`
- `llm_router_llm_positive`
- `llm_router_llm_negative`
- `llm_router_llm_abstain_empty`
- `llm_router_llm_abstain_invalid_json`
- `llm_router_llm_abstain_not_object`
- `llm_router_llm_abstain_relevante`
- `llm_router_llm_abstain_unknown_keys`
- `llm_router_llm_llm_abstain`
- `disabled`

## Regras de preenchimento estável
- Campos `llm_*` sempre presentes no JSON.
- Se o router estiver desligado: `llm_router_mode="deterministic"`, `llm_called=false`, `llm_model=""`, `llm_error=""`.
- Se o router estiver ligado e ocorrer erro externo: `llm_called=true` e `llm_error` preenchido.

## Validadores de referência
- Código: `plataform/nlp_engine/nlp_engine/output_invariants.py`
- Tipos: `plataform/nlp_engine/nlp_engine/contracts.py`

## Consumo padrão em A/B
- Usar `decision_source_distribution`, `llm_called_rate`, `llm_error_rate` na análise de cenário.
- Nunca aprovar promoção sem verificar `llm_error_rate` e matriz FP/FN.
