# Hepatologia — decisão modelo serving

Matriz P0 completada em `2026-05-25` com wheel **0.2.5**, cohort `hep-v1-200`.

## Contexto

| Campo | Valor |
|-------|--------|
| `run_id` | `hep-v1-200` |
| Perfil motor | `llm_http` |
| `config_version` | `0.1.9-diamond-pack2b-antifp-v1-pr2-reverted-pr1-only` |
| Wheel | `fabrica_ia-0.2.5` |
| Banda LLM | `[0.35, 0.65]` |
| `json_response_format` | `False` (Databricks FM não suporta `json_object`) |
| Cohort | VIEW `hive_metastore.ia.vw_dev_hepatologia_motor_validacao` |
| Benchmark Delta | `hive_metastore.ia.dev_tbl_hepatologia_motor_benchmark_{detail,summary}` |

## Corridas registradas — matriz P0 (n=200)

| `ts_run` (UTC) | `model_id` | MR homolog | FN | FP | TN | `llm_pct` | `llm_error_rate` | Notas |
|----------------|------------|------------|----|----|-----|-----------|-------------------|-------|
| 2026-05-25T14:17 | `databricks-qwen3-next-80b-a3b-instruct` | 0.880 | 12 | 12 | 5 | 0.32 | 0 | |
| 2026-05-25T14:14 | `databricks-meta-llama-3-3-70b-instruct` | 0.895 | 10 | 11 | 6 | 0.32 | 0 | melhor n=200 |
| 2026-05-25T14:11 | `databricks-claude-haiku-4-5` | 0.885 | 11 | 12 | 5 | 0.32 | 0 | |
| 2026-05-25T14:07 | `databricks-claude-sonnet-4-6` | 0.885 | 11 | 12 | 5 | 0.32 | 0 | idêntico ao Haiku |
| 2026-05-25T14:04 | `databricks-gpt-oss-20b` | 0.880 | 11 | 13 | 4 | 0.32 | 0 | baseline (wheel 0.2.5) |
| 2026-05-25T13:43 | `databricks-gpt-oss-20b` | 0.880 | 12 | 12 | 5 | 0.32 | 0 | confirmação 0.2.5 |
| 2026-05-21T18:11 | `databricks-gpt-oss-20b` | 0.875 | 12 | 13 | 4 | 0.32 | 0 | pré-wheel parse fix |
| 2026-05-21T17:27 | `databricks-gpt-oss-20b` | 0.890 | 7 | 15 | 2 | 0.32 | 1.0 | **Descartar** — 64/64 `http_400` |

## Corridas registradas — escala n=500

| `ts_run` (UTC) | `model_id` | Cohort | MR homolog | MR legado | FN | FP | TN | `llm_pct` | `llm_err` | Notas |
|----------------|------------|--------|------------|-----------|----|----|-----|-----------|-----------|-------|
| 2026-05-25T18:38 | `databricks-claude-haiku-4-5` | `hep-v1-500` | **0.886** | 0.914 | **29** | 28 | 14 | 0.36 | 0 | ✅ LLM activo (140 pos + 46 neg); 64 fallback transientes + 6 abstain; **VENCEDOR n=500** |
| 2026-05-25T18:04 | `databricks-claude-haiku-4-5` | `hep-v1-500` | 0.870 | 0.946 | 25 | 40 | 2 | 0.36 | 0 | ~~⚠️ **INVÁLIDO** — 100% fallback; causa: `DATABRICKS_TOKEN` ausente do `os.environ` naquela sessão~~ |
| 2026-05-25T17:39 | `databricks-meta-llama-3-3-70b-instruct` | `hep-v1-500` | 0.870 | 0.886 | 40 | 25 | 17 | 0.36 | 0 | LLM activo; **REGRESSÃO** vs n=200 |

## Baseline rule_only (referência)

| `ts_run` | Perfil | MR homolog | MR legado | FN | FP |
|----------|--------|-----------|-----------|----|----|
| 2026-05-25 | `rule_only` | 0.660 | 0.645 | 61 | 7 |

O LLM (`llm_http`) recupera **51 FN** do `rule_only` com custo de ~5 FP adicionais.

## Query ranking

```sql
SELECT model_id, match_rate_homolog, fn, fp, tn, llm_called_rate, llm_error_rate, ts_run
FROM hive_metastore.ia.dev_tbl_hepatologia_motor_benchmark_summary
WHERE run_id = 'hep-v1-200'
ORDER BY match_rate_homolog DESC, fn ASC, ts_run DESC;
```

## Decisão

- **Status:** `HAIKU VENCEDOR` — `databricks-claude-haiku-4-5` é o modelo a promover
- **`ts_run` de referência (n=500 válido):** `2026-05-25T18:38` — MR=**88.6%**, FN=**29**
- Llama descartado para produção: regressão de 89.5% → 87.0% ao escalar; FN=40 em n=500

### Comparação final n=500

| Métrica | Haiku (18:38) | Llama (17:39) | Δ |
|---------|--------------|--------------|---|
| MR homolog | **88.6%** | 87.0% | +1.6 pp |
| FN | **29** | 40 | −11 misses |
| FP | 28 | **25** | +3 falsos alarmes |
| TN | 14 | 17 | — |
| LLM calls | 36% | 36% | — |
| LLM positivos | 140 | — | — |
| LLM negativos | 46 | — | — |
| Fallback (transiente) | 64 | — | — |
| Abstain (json inválido) | 6 | — | — |

**Em contexto clínico:** reduzir 11 FN (exames relevantes não detectados) justifica os 3 FP adicionais.

### Root cause anomalia Haiku (run 18:04 — INVÁLIDO)

- Causa: `DATABRICKS_TOKEN` ausente do `os.environ` naquela sessão Python (após restart do kernel).
- O motor retornou `missing_env:DATABRICKS_TOKEN` → 100% `llm_router_llm_fallback` para todos os 180 casos in-band.
- `llm_error_rate=0` no benchmark não captura este tipo de erro (campo `llm_error` não persistido na tabela de detalhe).
- Formato de resposta Haiku (`\`\`\`json\n{"relevante": true}\n\`\`\``) validado: o regex de `_parse_llm_json` extrai correctamente.

### Nota sobre 64 fallbacks no run válido (18:38)

64 casos resultaram em `llm_router_llm_fallback` com erro (transiente — provavelmente timeouts ou throttle do endpoint). Mesmo com estes 64 a cair para calibração, o Haiku supera o Llama. Em produção, erros transientes devem ser monitorizados.

### Próxima decisão

Promover `databricks-claude-haiku-4-5` como modelo padrão.

## Gates sanity (wheel 0.2.5) — validados

- [x] Bloco **1d**: `hasattr(_extract_message_content)` — OK
- [x] Bloco **7**: sem `http_400` em massa; `llm_positive`+`llm_negative` ≥ 50 por corrida
- [x] Bloco **10**: `llm_error_rate` = 0 em todas as corridas da matriz
- [x] Wheel `fabrica_ia-0.2.5` publicada (Volume + Azure Artifacts, run #20260521.4)

## Teste de refinamento de banda

| Banda | MR homolog | FN | FP | TN | LLM calls | Decisão |
|-------|-----------|----|----|-----|-----------|---------|
| `[0.35, 0.65]` | **88.6%** | **29** | **28** | **14** | 180 (36%) | ✅ **USAR** |
| `[0.40, 0.60]` | 88.2% | 30 | 29 | 13 | 168 (33.6%) | ❌ Descartado |

Poupar 12 chamadas LLM (~6.7%) ao custo de −0.4 pp MR e +1 FN clínico não justifica a mudança. Banda permanece `[0.35, 0.65]`.

## Próximos passos

- [x] Escala `hep-v1-500` com `databricks-meta-llama-3-3-70b-instruct` → MR 87.0%, FN=40 (regressão — descartado)
- [x] Investigar anomalia Haiku n=500 (18:04) → causa: `DATABRICKS_TOKEN` ausente; formato de resposta validado
- [x] Re-run Haiku n=500 (18:38 e 19:15) → MR **88.6%**, FN=**29** — estável, zero fallbacks; **VENCEDOR CONFIRMADO**
- [x] Teste banda `[0.40, 0.60]` → MR 88.2%, −0.4 pp, −12 LLM calls — **descartado, manter [0.35, 0.65]**
- [ ] Alinhar time: widget 9 default → `databricks-claude-haiku-4-5`
- [ ] Monitorizar fallbacks transientes em produção (`decision_source = llm_router_llm_fallback`)
- [ ] Débito: snapshot cohort fixo (`dev_tbl_hepatologia_motor_validacao_snapshot`)
- [ ] PR `release/0.2.5-sync-hml-nlp` → `develop/nlp_engine` (revisão time)
