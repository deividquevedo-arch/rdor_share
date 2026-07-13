# Benchmark Hepatologia no lake — output por modelo

**Débito técnico (após E2E):** materializar `dev_tbl_hepatologia_motor_validacao_snapshot` e trocar fonte do notebook de `gold_view_validacao` → `gold_snapshot` para cohort fixo por `run_id`.

**Objetivo:** rodar o motor no notebook (Databricks), comparar com homolog + legado, cruzar corridas por **`model_id`** e escolher o melhor modelo tecnicamente.

**Perfil motor (validado):** equivalente a `S5_hybrid_calibrated_llm_pilot` → notebook `llm_http` + `calibrated_hybrid` + `llm_router` banda `[0.35, 0.65]`.

**Primeiro modelo:** `databricks-gpt-oss-20b` (serving). Depois repetir o **mesmo `run_id` de cohort** com outro `model_id`.

**Wheel mínimo:** `fabrica_ia>=0.2.1` (`_extract_message_content`). Notebook: blocos **1d** (smoke) e **11** (matriz P0).

**Wheel mínimo:** `fabrica_ia>=0.2.1` (`_extract_message_content` para OSS/reasoning). Notebook: bloco **11** (matriz P0) e **1d** (smoke wheel).

---

## Cohorts (fixos)

| `run_id` | N | Uso |
|----------|---|-----|
| `hep-v1-200` | 200 | Gate |
| `hep-v1-500` | 500 | Escala |
| `hep-v1-1000` | 1000 | Opcional |
| `hep-v1-5000` | 5000 | Master blaster (1x) |

Filtro: `saida` ⨝ `retorno`, `cod_achado_relevante` nas 3 categorias homologadas, laudo válido.

---

## Tabelas Delta (schema `hive_metastore.ia`)

### Detalhe — uma linha por exame por corrida

`dev_tbl_hepatologia_motor_benchmark_detail`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `run_id` | string | Cohort fixo (`hep-v1-200`) |
| `model_id` | string | Ex.: `databricks-gpt-oss-20b` |
| `model_provider` | string | `databricks_serving` |
| `llm_base_url` | string | Endpoint serving |
| `perfil_motor` | string | `llm_http` |
| `config_version` | string | YAML |
| `engine_version` | string | Wheel |
| `ts_run` | timestamp | Início da corrida |
| `id_exame` | string | |
| `id_paciente` | string | |
| `fl_motor` | int | Saída motor |
| `confidence_score` | double | |
| `fl_legado` | int | `flgRelevante` gold saida |
| `fl_homolog` | int | Derivado retorno 1/2 vs 3 |
| `cod_achado_relevante` | string | Rótulo clínico |
| `decision_source` | string | Payload motor |
| `llm_called` | boolean | |
| `llm_model` | string | Modelo efetivo na chamada LLM |
| `llm_error` | string | Curto, se houver |
| `latency_ms` | double | Tempo por exame |

### Resumo — uma linha por (`run_id`, `model_id`)

`dev_tbl_hepatologia_motor_benchmark_summary`

| Coluna | Descrição |
|--------|-----------|
| `run_id`, `model_id`, `n` | |
| `match_rate_homolog`, `match_rate_legado` | |
| `tp`, `fp`, `fn`, `tn` | vs homolog |
| `llm_called_rate`, `llm_error_rate` | |
| `latency_p50_ms`, `latency_p95_ms`, `total_sec` | |

**Regra de comparação entre modelos:** só comparar métricas com o **mesmo `run_id`** (mesmos `id_exame`).

---

## Critério técnico para “melhor modelo” (ordem)

1. Maior `match_rate_homolog` (primário)
2. Menor `fn` vs homolog (não perder positivos clínicos)
3. `fp` aceitável (teto acordado com clínica)
4. `llm_error_rate` ≈ 0
5. Menor `latency_p95_ms` e `total_sec` empatados em qualidade

Documentar decisão em nota com `run_id` + JSON de `summary`.

---

## Notebook

Ver células em `apps/databricks/hepatologia_motor/ntb_hepatologia_motor_sandbox.py` — Blocos **9** (persist) e **10** (métricas + cruzamento modelos).
