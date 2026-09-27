# SPEC — `llm_router` no motor (v0)

Contrato para segunda passagem opcional após score híbrido calibrado. Implementação em [`plataform/nlp_engine/nlp_engine/llm_router_backend.py`](../../plataform/nlp_engine/nlp_engine/llm_router_backend.py) e integração em [`engine.py`](../../plataform/nlp_engine/nlp_engine/engine.py).

## Modos (`nlp.llm_router.mode`)

| Valor | Comportamento |
|-------|----------------|
| `deterministic` (default) | Regex na banda `uncertainty_band` sobre texto tratado — sem rede. |
| `llm` | Chamada HTTP OpenAI-compatible quando a banda activa; falha → mantém decisão pré-LLM. |

## Gatilho

- Invocação só quando `enabled: true` **e** `calibrated_score` ∈ `uncertainty_band` (mesmo critério que o modo determinístico).

## Entrada ao modelo

- Texto já passado por `to_plain` (motor); truncado a `max_input_chars` (default 8000).
- **Não** incluir ids de paciente/exame no prompt — o composition root (ex. Databricks) deve garantir política de dados.

## Prompt — placeholders (YAML-only, generalista)

Os templates `prompt_system` e `prompt_user_template` podem usar estes placeholders (substituição segura; chaves desconhecidas não quebram o fluxo):

| Placeholder | Origem |
|-------------|--------|
| `{text}` | Excerto truncado do laudo tratado. |
| `{specialty_context}` | Campo `nlp.llm_router.specialty_context` no YAML da especialidade. |
| `{specialty_id}` | Valor de `specialty_id` passado a `ClinicalNlpEngine.process` (identificador da especialidade, não conteúdo clínico). |

Exemplo mínimo (apenas contexto por especialidade):

```yaml
nlp:
  llm_router:
    specialty_context: |
      Decidir relevância para encaminhamento: relevante=true só se houver achado clinicamente relevante;
      ausência de doença, anatomia normal ou negação clara → relevante=false.
    prompt_user_template: |
      Contexto:
      {specialty_context}
      Excerto:
      {text}
```

## Saída esperada (JSON)

O modelo deve responder com JSON parseável contendo **um** dos esquemas:

1. `{"relevante": true|false}` — `true` força `fl_relevante=1`, `false` força `0`.
2. `{"action": "promote"|"block"|"abstain"}` — `abstain` mantém `current_fl`.

## Config YAML genérica (`nlp.llm_router`)

| Chave | Tipo | Descrição |
|-------|------|-----------|
| `enabled` | bool | Liga o gancho do router. |
| `mode` | string | `deterministic` \| `llm`. |
| `uncertainty_band` | `[lo, hi]` | Banda em [0,1] sobre `calibrated_score`. |
| `max_input_chars` | int | Truncagem do texto enviado ao LLM. |
| `timeout_s` | float | Timeout HTTP. |
| `provider` | string | `openai_compatible` (único suportado em v0). |
| `base_url` | string | Ex.: `https://api.openai.com/v1` ou endpoint Azure OpenAI. |
| `model` | string | Nome do modelo no endpoint. |
| `api_key_env` | string | Nome da variável de ambiente com o segredo (ex.: `NLP_ENGINE_LLM_API_KEY`). |
| `temperature` | float | Opcional; default 0. |
| `prompt_system` | string | Mensagem system (pode referenciar tarefa sem PHI). |
| `prompt_user_template` | string | Template com placeholders (ver secção **Prompt — placeholders**). |
| `specialty_context` | string | Texto curto **só via YAML** (contexto clínico genérico da especialidade); injetado em `{specialty_context}`. |
| `negative_context_patterns` | list[str] | Só modo `deterministic`: regex. |
| `positive_context_patterns` | list[str] | Só modo `deterministic`: regex. |
| `json_response_format` | bool | Se `true`, envia `response_format: json_object` no POST (OpenAI/Azure compatível). |
| `chat_completions_path` | string | Default `v1/chat/completions` (Databricks serving-openai-compat). |

### Databricks Foundation Model / serving (obrigatório no piloto)

- **`json_response_format: false`** no notebook/perfil `llm_http`. Com `true`, o serving exige a palavra `json` em `messages` e modelos como `databricks-gpt-oss-20b` devolvem **HTTP 400**.
- **`api_key_env`:** usar `DATABRICKS_TOKEN` no composition root (widget 11).
- **`base_url`:** `https://<workspace>/serving-endpoints` (sem `/v1` no widget).
- Respostas com `content` em **lista** (reasoning): a lib `fabrica_ia>=0.2.1` normaliza via `_extract_message_content` antes do parse JSON.

## Exemplo (fragmento — merge em `nlp:` da especialidade)

**Modo determinístico** (default; sem rede):

```yaml
nlp:
  llm_router:
    enabled: false
    mode: deterministic
    uncertainty_band: [0.35, 0.65]
```

**Piloto LLM** (`pip install -e ".[llm]"`; segredo só via env referenciada):

```yaml
nlp:
  llm_router:
    enabled: true
    mode: llm
    uncertainty_band: [0.35, 0.65]
    max_input_chars: 8000
    timeout_s: 30
    provider: openai_compatible
    base_url: "${AZURE_OPENAI_ENDPOINT}/openai/deployments/${DEPLOYMENT_NAME}"
    model: "${AZURE_OPENAI_DEPLOYMENT_ID}"
    api_key_env: NLP_ENGINE_LLM_API_KEY
    temperature: 0
    json_response_format: false
    prompt_system: |
      Respond only with a single JSON object. Output json only.
      Schema: {"relevante": boolean}
```

*(Substituir placeholders pelo endpoint real; não versionar chaves no YAML. Em Databricks FM manter `json_response_format: false`.)*

## Observabilidade

- Campos opcionais em `exm_laudo_resultado`: `llm_router_mode`, `llm_called`, `llm_model`, `llm_error` (curto, sem payload completo).
- **Não** logar prompt/resposta completa em ambientes com PHI.

## Dependência opcional

- Extra `llm` em `pyproject.toml` inclui `httpx` para chamadas HTTP.

## Validação (bancada)

### Métricas por `cod_achado_relevante` (1 / 2 / 3)

No relatório JSON da matriz (`run_hepatologia_strategy_matrix` / `run_hepatologia_diamond_bench`), cada cenário pode incluir **`metrics_by_cod_123`**: contagens e acertos por classe de gold (primeiro carácter de `cod_achado_relevante`), apenas para linhas com `1`, `2` ou `3`, no pareamento audit↔legacy (mesmas chaves que o compare).

- **`by_cod["1"|"2"|"3"]`**: `n`, `motor_S`, `motor_N`, `correct`, `wrong`, `accuracy_class` (= `correct/n`).
- **`aggregate_sn_on_labeled`**: `tp`, `fn`, `fp`, `tn`, `accuracy`, `recall_positive`, `recall_negative` com gold binário S/N (`1` e `2` → S, `3` → N).

Útil para gates por classe (ex.: melhorar classe `3` sem degradar `1`) além de MR/FP/FN globais.

Comparar com gold estrito S/N: usar `--only-cod-123` no bench (subset de avaliação alinhado a rotulados 1/2/3).

Gate A/B/C (rule vs hybrid calibrado vs router determinístico):

```powershell
cd plataform/nlp_engine
.\.venv\Scripts\python.exe scripts\run_hepatologia_abc_gate.py --help
```

Matriz rápida **baseline + campeão** (mesma amostra `max_rows`):

```powershell
.\.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 `
  --promotion-profile fn_priority --print-table `
  --scenarios-yaml configs\hepatologia\scenarios\strategy_matrix_calibration_layers.yaml `
  --only-scenarios baseline,S5_hybrid_calibrated
```

Para piloto **LLM real**: `mode: llm` + `api_key_env` + `base_url`/`model`, instalar `.[llm]`, repetir MR/FP/FN e medir **% de linhas com `llm_called: true`** no audit.

Cenário já preparado na submatriz (preencher `base_url` e `model` no YAML ou overlay local):

```powershell
.\.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 `
  --promotion-profile fn_priority --print-table `
  --scenarios-yaml configs\hepatologia\scenarios\strategy_matrix_calibration_layers.yaml `
  --only-scenarios baseline,S5_hybrid_calibrated,S5_hybrid_calibrated_llm_pilot
```

Se `base_url`/`model` estiverem vazios, todas as chamadas falham cedo → `llm_router_llm_fallback` (útil só para smoke sem API).

## Governação

- Implementação alinhada a backlog acordado (história/task no board). PHI proibido em testes e logs.
- **História de referência:** **S12b — LLM fallback seletivo** (`T12b.x` em [`anexo03-historias-e-tasks-v0.md`](../anexos/anexo03-historias-e-tasks-v0.md)): router opcional, gatilho por incerteza, fallback seguro, observabilidade e validação em bancada.
