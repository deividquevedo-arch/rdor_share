# Governanca e Telemetria Global do Motor (v0)

Objetivo: padronizar rastreabilidade no `nlp_engine` para qualquer especialidade sem lógica clínica hardcoded.

## Campos obrigatórios por linha
- `specialty_id`
- `config_version`
- `engine_version`
- `fl_relevante`
- `confidence_score`
- `exm_laudo_resultado` (JSON válido)

## Campos obrigatórios no JSON `exm_laudo_resultado` — **atualizado na lib 0.9.0**

Princípio: o que sai em todo laudo e é lido em **agregação** fica no núcleo; o resto é
**condicional** — a presença é a informação.

- **Núcleo (sempre):** `summary_compact`, `n_positive_spans`, `n_negated_spans`,
  `score_policy_version`, `decision_source`, `llm_router_mode`, `llm_called`, `decision_trail`
- **Condicionais:** `segmentation_strategy`, `uncertainty_band_hit`, `semantic_score`,
  `semantic_matched_term`, `semantic_evidence`, `llm_model`, `llm_error`, `ordinal_*`,
  `quantitative`, e os sinais de perda (`n_organ_gate_spans`, `segmentation_coverage`)

⚠️ `semantic_score` **ausente ≠ `0.0`** — ausente significa que a semântica não rodou.

**Removidos:** `rule_engine_version` (literal `"t022_v1"` congelado; `engine_version` já é coluna
real) e `embedding_model` (eco da config). **`semantic_backend` foi realocado** para o passo
`semantic` da `decision_trail`, onde segue sendo o único sinal do fallback silencioso de
embeddings.

⚠️ **`emit_decision_trail` deixou de ser opt-in** — a trilha é sempre emitida.

## Taxonomia canônica de `decision_source`

⚠️ **Lib 0.9.0:** `rads_promotion` e `rads_only` passaram a `ordinal_promotion` e `ordinal_only`.
A escala não é só RADS (Bethesda, Bosniak, TNM). Rename limpo, sem valor legado: não havia
consumidor a jusante — medido em 2026-08-18.

- `rule`
- `hybrid`
- `hybrid_calibrated`
- `embedding_fallback`
- `ordinal_promotion`
- `ordinal_only`
- `quantitative_promote`
- `quantitative_gate`
- `document_vet`
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
- Código: `nlp_engine/output_invariants.py` (repo `nlp-engine-lib`)
- Tipos: `nlp_engine/contracts.py` (repo `nlp-engine-lib`)

## Consumo padrão em A/B
- Usar `decision_source_distribution`, `llm_called_rate`, `llm_error_rate` na análise de cenário.
- Nunca aprovar promoção sem verificar `llm_error_rate` e matriz FP/FN.
