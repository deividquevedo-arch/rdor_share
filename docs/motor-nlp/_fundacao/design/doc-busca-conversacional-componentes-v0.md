# Busca conversacional — componentes e escopo MVP (v0)

Produto: **busca conversacional no Clinical Data Hub** (distinto da Central de Captação programada).  
Reutiliza `fabrica_ia.data_manager` + wheel **`nlp_engine`** (`nlp-engine-lib`) sem acoplar lógica de app dentro das libs.

**Diagrama:** `.alt.doc/Mapa do Sistema Rede D'Or NLP Base.drawio`  
- Aba *Busca Conversacional — Componentes* (arquitetura)  
- Aba *Busca Conversacional — Histórias* (S20–S27, tasks, gate, AC)  
**Backlog:** histórias **S20–S27** no drawio; registo formal no `anexo03` pendente (D1).

**Fase atual:** **planejamento** — componentes especificados; **plano ainda não fechado** para implementação. Ver [§ Gate de fechamento do planejamento](#gate-de-fechamento-do-planejamento).

---

## Macro (uma frase)

Interpretador monta plano → `data_manager` estreita cohort → `nlp_engine` (stack completo, config **query-native**) tria → row-LLM refina extração estruturada (se preciso) → Resultado empacota para UI.

---

## Princípio de produto — busca query-native

A busca conversacional **não é** uma especialidade clínica (hepatologia, BI-RADS, etc.). É um produto de **perguntas ad-hoc personalizadas** sobre o hub.

| | Central de Captação (especialidade) | Busca conversacional |
|--|-------------------------------------|----------------------|
| Config | YAML calibrado por especialidade | `QueryPlan` → `RuntimeBundle` **por pergunta** |
| `findings` | Léxico clínico homologado | Termos extraídos da pergunta do utilizador |
| Motor | Perfil da especialidade (`rule_only` … `llm_http`) | **`query_full`** — todas as capacidades da lib disponíveis |
| Objetivo | Captação programada com FP/FN calibrados | Máximo desempenho em buscas gerais variadas |

**Hepatologia / runner E2E:** referência de **como compor** as libs (`get_data`, `rows_for_motor`, `ClinicalNlpEngine.process`) — **não** template de config, prompts nem léxico a copiar.

**Stack motor alvo (C4 → `nlp_cfg`):** regra + negação → embeddings (sinónimos dos termos da pergunta) → score calibrado → `llm_router` (desambiguação na banda de incerteza) → xxRADS **se** a pergunta envolver categorização RADS. Tudo ligável via `nlp_config`; C4 monta o dict a partir do `QueryPlan`, não de YAML de especialidade.

**Row-LLM (C5):** camada **adicional** da app para extração estruturada (medidas, operadores, campos livres) — **complementa** o motor; não substitui `llm_router` nem embeddings.

**Perfis de runtime (C4):**

| Perfil | Uso |
|--------|-----|
| `query_full` | **Alvo produção** — motor completo + row-LLM opcional |
| `query_standard` | POC sem endpoint LLM do router (regra + embeddings) |
| `query_minimal` | Dev/local — só regra; sem dependência de serving |

---

## Roadmap de fechamento de escopo

Fechar **um componente por vez** (entrada, saída, funções, não-faz, edge cases).  
**Especificado v0** ≠ **plano fechado** — itens em aberto no [gate de planejamento](#gate-de-fechamento-do-planejamento).

| Ordem | ID | Componente | Escopo v0 | Especificação | Plano |
|------:|----|------------|-----------|---------------|-------|
| 1 | C2 | Interpretador LLM | [§ C2](#c2--interpretador-llm) | v0 | pendente gate |
| 2 | C3 | Validador QueryPlan | [§ C3](#c3--validador-queryplan) | v0 | pendente gate |
| 3 | C4 | Montador de configs | [§ C4](#c4--montador-de-configs) | v0 | pendente gate |
| 4 | — | `data_manager` | [§ data_manager](#data_manager) | v0 | pendente gate |
| 5 | — | `nlp_engine` | [§ nlp_engine](#nlp_engine) | v0 (`query_full`) | pendente gate |
| 6 | C5 | Plugin row-LLM | [§ C5](#c5--plugin-row-llm) | v0 | pendente gate |
| 7 | C6 | Empacotador resultado | [§ C6](#c6--empacotador-resultado) | v0 | pendente gate |
| 8 | C1 | Orquestrador | [§ C1](#c1--orquestrador) | v0 | pendente gate |
| 9 | C7 | UI | [§ C7](#c7--ui) | v0 | pendente gate |

---

## Template (cada seção fechada deve ter)

1. **Papel** — uma frase  
2. **Entrada / Saída** — tipos ou contrato  
3. **Funções internas (MVP)** — lista curta  
4. **O que não faz** — limites explícitos  
5. **Edge cases MVP** — tabela ou bullets  
6. **Critério “escopo fechado”** — checklist sim/não  

---

## C2 — Interpretador LLM

**Status:** Fechado v0  
**Papel:** Traduzir a pergunta em linguagem natural num documento estruturado `QueryPlan` (uma chamada LLM por pergunta).

### Entrada

| Campo | Tipo | Obrigatório |
|-------|------|-------------|
| `pergunta` | `str` | sim |
| `contexto` | `dict` opcional | não — defaults de produto (ex.: `limit` máximo sugerido) |

### Saída

`QueryPlan` — objeto JSON (dict Python após parse) conforme schema abaixo.  
Dois desfechos:

| `status` | Próximo passo |
|----------|----------------|
| `ok` | Encaminha para **C3** |
| `needs_clarification` | Retorna `clarification_question` à **UI** (sem lake, sem motor) |

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `build_system_prompt()` | Prompt fixo + descrição do schema `QueryPlan` |
| `load_few_shots()` | 3–4 exemplos piloto (sem PHI) |
| `call_llm()` | Uma completion com saída JSON |
| `parse_query_plan()` | Parse + normalização mínima de chaves |
| `route_by_status()` | `ok` → C3; `needs_clarification` → UI |

### Schema `QueryPlan` (v0)

```json
{
  "status": "ok | needs_clarification",
  "clarification_question": "string | null",
  "data_spec": {
    "modalidade": ["string"],
    "procedimento_contains": ["string"],
    "dt_exame_from": "YYYY-MM-DD | null",
    "dt_exame_to": "YYYY-MM-DD | null",
    "limit": 5000
  },
  "nlp_spec": {
    "findings": ["string"],
    "match_mode": "any | all"
  },
  "row_llm_spec": {
    "enabled": false,
    "field": "string | null",
    "operator": "< | <= | > | >= | = | null",
    "value": "number | string | null",
    "unit": "string | null"
  },
  "output_spec": {
    "columns": ["id_exame", "dt_exame", "trecho_evidencia", "match_source"]
  }
}
```

**Regras de preenchimento (v0):**

- `findings` = termos **léxicos** para o motor (ex.: `PMAP`, `RVP`, `nódulo`). **Não** colocar medidas numéricas aqui.
- Medidas, campos clínicos e regras numéricas/estruturadas vão em `row_llm_spec` com `enabled: true` (não só VEF1 — qualquer campo que a pergunta explicitar).
- `limit` sugerido pelo LLM; **validação forte** fica no **C3** (teto 5000 no MVP).

### Few-shots piloto (v0)

| Pergunta (sintética) | `row_llm` | Notas |
|----------------------|-----------|--------|
| VEF1 menor que 30% | `enabled: true`, campo VEF1, op `<`, valor 30 | Espirometria / laudo pulmonar |
| PMAP e RVP no mesmo laudo | `enabled: false`, `match_mode: all` | Dois termos léxicos |
| Nódulo pulmonar suspeito | `enabled: false` | Só triagem NLP |
| “Exames de ontem” (sem modalidade) | `needs_clarification` | Falta cohort |

### O que não faz

- Não consulta Clinical Data Hub nem Delta.
- Não chama `data_manager`, `nlp_engine` nem row-LLM.
- Não valida `limit`, operadores nem schema (isso é **C3**).
- Não monta `dict` final das libs (isso é **C4**).
- Não usa `llm_router` do motor (contrato diferente: `fl_relevante`).

### Edge cases MVP

| Caso | Comportamento esperado |
|------|------------------------|
| JSON inválido do LLM | Tratar como erro recuperável → `needs_clarification` genérico |
| Pergunta fora do domínio clínico | `needs_clarification` |
| Só filtros de data/modalidade, sem achado | `nlp_spec.findings` vazio permitido; C3 decide se executável |
| Pergunta pede “todos os laudos” sem limite | LLM preenche `limit`; C3 aplica teto |

### Critério — escopo fechado v0

- [x] Papel e limites explícitos  
- [x] Schema `QueryPlan` mínimo documentado  
- [x] Funções internas nomeadas (diagrama + doc)  
- [x] Few-shots piloto listados  
- [x] Ramo `needs_clarification` definido  
- [x] Revisão do time — refinável; escopo base aprovado para seguir

---

## C3 — Validador QueryPlan

**Status:** Fechado v0  
**Papel:** Código Python (sem LLM) que confere se o `QueryPlan` é executável e seguro antes de buscar dados no hub.

### Entrada

| Campo | Tipo | Obrigatório |
|-------|------|-------------|
| `query_plan` | `dict` | sim — saída do C2 com `status: ok` |
| `policy` | `dict` | não — defaults MVP (ver constantes) |

**Pré-condição:** orquestrador **não** chama C3 se C2 retornou `needs_clarification`.

### Saída

`ValidationResult` (dict):

```json
{
  "status": "ok | reject",
  "errors": ["string"],
  "clarification_question": "string | null",
  "query_plan": {}
}
```

| `status` | Próximo passo |
|----------|----------------|
| `ok` | `query_plan` sanitizado → **C4** |
| `reject` | `clarification_question` ou lista `errors` → **UI** (sem lake) |

### Constantes MVP (`policy` default)

| Chave | Valor | Uso |
|-------|-------|-----|
| `max_limit` | `5000` | Teto de linhas no cohort |
| `min_limit` | `1` | Piso |
| `allowed_operators` | `<`, `<=`, `>`, `>=`, `=` | `row_llm_spec.operator` |
| `allowed_match_modes` | `any`, `all` | `nlp_spec.match_mode` |
| `max_findings` | `10` | Tamanho de `nlp_spec.findings` |

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `validate_schema()` | Chaves obrigatórias e tipos do `QueryPlan` v0 |
| `validate_data_spec()` | `limit` dentro do intervalo; datas coerentes (`from` ≤ `to`) |
| `validate_nlp_spec()` | `match_mode` permitido; `findings` lista de strings não vazias (se presentes) |
| `validate_row_llm_spec()` | Se `enabled`: `field`, `operator`, `value` obrigatórios; operador na whitelist |
| `validate_executability()` | Plano tem intenção mínima: cohort **ou** achado léxico **ou** row-LLM |
| `sanitize_plan()` | Aplica `clamp(limit)`; remove strings vazias em listas |
| `build_validation_result()` | Monta `ok` + plano sanitizado ou `reject` + mensagem |

### Regras de validação (v0)

**Schema**

- `data_spec`, `nlp_spec`, `row_llm_spec`, `output_spec` devem existir.
- `data_spec.limit` é inteiro.

**Cohort (`data_spec`)**

- `limit` ∈ [`min_limit`, `max_limit`]; se acima do teto → **sanitizar** para `max_limit` (não rejeitar só por isso).
- Se `dt_exame_from` e `dt_exame_to` preenchidos → `from` ≤ `to`.
- Janela máxima opcional MVP: 365 dias (rejeitar se exceder).

**NLP (`nlp_spec`)**

- `findings` vazio **permitido** se `row_llm_spec.enabled` ou filtros de cohort forem suficientes para executabilidade.
- Termos em `findings` não podem ser só números/medidas (heurística: rejeitar se parecer valor numérico puro).

**Row-LLM (`row_llm_spec`)**

- `enabled: false` → ignorar demais campos.
- `enabled: true` → exige `field`, `operator`, `value`; `operator` ∈ `allowed_operators`.

**Executabilidade mínima**

Rejeitar se **todos** forem verdadeiros:

- cohort vazio (sem modalidade, procedimento nem janela de data), **e**
- `findings` vazio, **e**
- `row_llm_spec.enabled` é `false`.

### O que não faz

- Não chama LLM.
- Não consulta hub nem executa SQL/Spark.
- Não monta dicts das libs (isso é **C4**).
- Não altera texto da pergunta original.
- Não implementa regras clínicas finas (só guardrails estruturais).

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `limit` ausente | Default `max_limit` no `sanitize_plan()` |
| `limit` > 5000 | Clamp para 5000 + segue |
| `row_llm` enabled sem `value` | `reject` com mensagem clara |
| Operador inválido (`!=`) | `reject` |
| Só `output_spec` preenchido | `reject` — nada a executar |
| Plano válido mas cohort muito amplo | `ok` com `limit` clamped (MVP não bloqueia por “amplitude”) |

### Critério — escopo fechado v0

- [x] Contrato `ValidationResult` definido  
- [x] Constantes e whitelist documentadas  
- [x] Funções internas nomeadas  
- [x] Regra de executabilidade mínima  
- [x] Sanitização vs rejeição explícita  
- [ ] Revisão do time — refinável

---

## Auditoria — repos, runner E2E e `data_manager` (v0)

Revisão contra o **uso real** documentado em `fabrica-ia-plataforma` (runner) + contratos em `fabrica-ia-lib` (branch `hml`) + motor em `nlp-engine-lib`.

> **Nota:** hepatologia e demais especialidades aparecem aqui só como **exemplo de composition root** — padrão de chamada às libs. A busca conversacional **não reutiliza** o conteúdo clínico desses YAMLs.

### Onde cada peça mora

| Repo / artefacto | Branch local (workspace) | Responsabilidade |
|------------------|--------------------------|------------------|
| **`fabrica-ia-lib`** | `hml` | `fabrica_ia.data_manager` (`GoldDataManager`, `SpecialtyConfig`, `LocationResolver`), `fabrica_ia.nlp_platform.batch` (`run_entrada`, `run_process`, `build_gold_query_filters`, `rows_for_motor`) |
| **`nlp-engine-lib`** | `release/v0.1.1` (wheel no Volume) | `nlp_engine.nlp_engine` — `ClinicalNlpEngine.process()`, `merge_with_shared_organs`, scoring, LLM router |
| **`fabrica-ia-plataforma`** | `branch-from-versao-alpha` (referência runner) | Composition root: `ntb_ia_motor_e2e.py` + configs `ntb_ia_{specialty}_config.py` |

**Dois wheels no cluster** (ver `apps/databricks/nlp_engine/README.md`):

- `fabrica_ia` → data + helpers batch (`entrada.py`)  
- `nlp_engine` → motor NLP (`ClinicalNlpEngine`)

**Fonte de verdade para APIs de coleta:** `fabrica-ia-lib/src/fabrica_ia/data_manager/gold.py` + `nlp_platform/batch/entrada.py` (branch `hml` sincronizado).

### Rastreio real — hepatologia (`ntb_ia_hepatologia_config.py` → `ntb_ia_motor_e2e.py`)

```
ntb_ia_hepatologia_config.py          ntb_ia_motor_e2e.py
────────────────────────────          ───────────────────
CONFIG["data"]                   →    SpecialtyConfig.data
  gold_domains [4 domínios]             (+ tables derivadas por catalog/specialty/env)
  filters.gold_filter only              filters repassados tal qual
  column_map                            column_map repassado
  legacy.*                              legacy.* repassado

CONFIG["nlp"]                    →    merge_with_shared_organs(ORGANS_SHARED, CONFIG)
                                      → apply_runtime_profile → nlp_cfg
                                      → run_process(..., nlp_cfg)

(widgets data_inicio/data_fim)   →    run_entrada(..., data_inicio, data_fim, limit_rows)
                                      → build_gold_query_filters(cfg, ...)
                                      → GoldDataManager.get_data(...)
                                      → rows_for_motor(..., gold_filter, column_map)
                                      → write_staging (Delta)
```

**Config de dados em produção (hepatologia / birads / template):**

```python
"data": {
    "gold_domains": ["exame.identificacao", "exame.procedimento", "exame.datas", "exame.laudos"],
    "filters": {
        "gold_filter": {"keywords": ["figado", "fígado", "abd", "bdo"], "mode": "any"},
        # gold_query: AUSENTE nos configs de especialidade hoje
    },
    "column_map": { ... },  # mesmo padrão em hepatologia_config
}
```

**Datas de cohort:** não ficam no `CONFIG` — vêm dos **widgets** `data_inicio` / `data_fim` do runner quando `fonte_staging=motor_gold`.

### O que o `data_manager` aceita (contrato lib)

| Camada | API / campo | Quando | Formato |
|--------|-------------|--------|---------|
| SQL | `GoldDataManager.get_data(domains, filters, limit)` | Coleta | `filters`: `data_inicio`, `data_fim` + `gold_query` (coluna → valor/`IN`/`{value,expr}`) |
| Config | `CONFIG["data"]` / `SpecialtyConfig.data` | Runner valida | `gold_domains`, `filters`, `column_map` |
| Montagem | `build_gold_query_filters(cfg, data_inicio, data_fim)` | Pré-`get_data` | Datas dos widgets/plano + merge `filters.gold_query` |
| Pós-fetch | `rows_for_motor(pdf, …, gold_filter, column_map)` | Python | `gold_filter` = substring no **texto do laudo** (`proced_lista_exames`); `column_map` → contrato motor |

**Referências canónicas:**  
`fabrica-ia-plataforma/apps/databricks/nlp_engine/ntb_ia_hepatologia_config.py`,  
`ntb_ia_motor_e2e.py`, `ntb_ia_template_config.py`,  
`fabrica-ia-lib/src/fabrica_ia/data_manager/models.py`,  
`fabrica-ia-lib/src/fabrica_ia/nlp_platform/batch/entrada.py`.

### Desalinhamentos do plano anterior (C4 / busca)

| Tópico | Plano anterior | Uso real (runner + hepatologia) |
|--------|----------------|----------------------------------|
| Forma da config | `RuntimeBundle` genérico `get_data` | Dict no **formato `CONFIG`** (`data` + `nlp` + metadados); runner fatia em `SpecialtyConfig` + `nlp_cfg` |
| Filtro de cohort | `gold_query` para procedimento/modalidade | Especialidades usam só **`gold_filter`** (keywords no laudo); **`gold_query` existe na lib mas não está nos configs `.py`** |
| Datas | dentro de `data_spec` no bundle | Paralelo aos **widgets** do runner: parâmetros de execução, não persistidos no CONFIG estático |
| Motor NLP | `fabrica_ia.nlp_engine` implícito | Wheel **`nlp_engine`**; `run_process` chama `ClinicalNlpEngine` de `nlp_engine.nlp_engine` |
| `findings` | lista flat no plano | `CONFIG["nlp"]["findings"]` = **`dict[str, list[str]]`** com categorias clínicas |
| Fluxo batch | `get_data` → motor direto | Produção: `run_entrada` → **staging Delta** → `run_process`; busca MVP pode saltar staging mas deve reutilizar as **mesmas funções** |

### Implicação para busca conversacional (C4 + data_manager)

**Princípio:** consumir **APIs das libs**, não replicar o runner. A plataforma (`run_entrada`) é **referência**, não limite.

| Camada | API canónica (lib) | Obrigatório? |
|--------|-------------------|--------------|
| Coleta SQL | `GoldDataManager.get_data(domains, filters, limit)` | sim |
| Montagem de filtros SQL | `build_gold_query_filters(cfg, …)` **ou** dict `filters` montado pelo app | um dos dois |
| Pós-fetch → motor | `rows_for_motor(pdf, …, column_map, gold_filter)` | sim |
| Config tipada | `SpecialtyConfig` | **opcional** — conveniente para C4 validar `data.*` |
| Batch plataforma | `run_entrada`, `write_staging` | **não** na busca MVP |

**Caminhos lib não usados pela plataforma hoje, mas válidos na busca:**

- `gold_query` em `get_data` (procedimento/modalidade no SQL) — suportado em `gold.py`; ausente nos `ntb_ia_*_config.py` de especialidade.
- `filters` dict **direto** em `get_data` sem `SpecialtyConfig`.
- `GoldDataManager.list_domains` / `list_fields` — introspecção para validador/UI (futuro).
- Passthrough de **qualquer coluna exame** via expr (`proced_descricao_ajustado`, `nme_procedimento`, …) — docstring de `get_data`.

**Campo morto (não usar):** `DataFilters.modality` — declarado em `models.py`, **não ligado** a `build_gold_query_filters` nem a `GoldDataManager`. C4 mapeia modalidade → chave explícita em `filters` / `gold_query`.

---

## C4 — Montador de configs

**Status:** Fechado v0  
**Papel:** Traduzir `QueryPlan` validado (C3) num **`CONFIG` parcial** (formato compatível com `SpecialtyConfig`) + **`run_params`**, consumível pelas APIs da lib.

### Entrada

| Campo | Tipo | Obrigatório |
|-------|------|-------------|
| `query_plan` | `dict` | sim — saída C3 com `ValidationResult.status == ok` |
| `defaults` | `dict` | não — domínios Gold default, `specialty_id` query |

### Saída

`RuntimeBundle` (dict) — **espelha o contrato do runner**, não um formato novo:

```json
{
  "specialty_id": "query_mvp",
  "config_version": "query-v0",
  "catalog": "<catalog_clinical_data_hub>",
  "data": {
    "gold_domains": [
      "exame.identificacao",
      "exame.procedimento",
      "exame.datas",
      "exame.laudos"
    ],
    "filters": {
      "gold_filter": { "keywords": [], "mode": "any" },
      "gold_query": {}
    },
    "column_map": {
      "id_exame": "id_exame",
      "id_paciente": ["id_paciente", "id_patient"],
      "id_unidade": "id_unidade",
      "exm_laudo_texto": ["proced_laudo_exame_original", "proced_laudo_exame"],
      "exm_mod": ["cod_procedimento", "tp_codigo_procedimento"],
      "exm_tipo": "proced_nome_exame",
      "dt_exame": "dt_exame"
    }
  },
  "nlp": {
    "findings": {},
    "negation_phrases": ["sem", "nao ha", "não há", "ausencia de", "ausência de"],
    "negation_window": 5,
    "feature_flags": { "rule_engine": true, "calibrated_hybrid": true },
    "score_policy_version": "v1_bins_legacy",
    "embeddings": {
      "use_embeddings": true,
      "decision_mode": "fallback",
      "semantic_terms": []
    },
    "llm_router": {
      "enabled": true,
      "mode": "llm",
      "specialty_context": "",
      "uncertainty_band": [0.35, 0.65],
      "fallback_policy": "keep_current"
    },
    "segmentation": { "mode": "full_doc" }
  },
  "runtime": { "profile": "query_full" },
  "run_params": {
    "data_inicio": "YYYY-MM-DD",
    "data_fim": "YYYY-MM-DD",
    "limit": 5000
  },
  "match_mode": "any",
  "row_llm_spec": {},
  "output_spec": {}
}
```

**Consumo via lib (busca — sem staging):**

```python
from datetime import date

from fabrica_ia.data_manager.gold import GoldDataManager
from fabrica_ia.data_manager.models import GoldFilterConfig, SpecialtyConfig
from fabrica_ia.nlp_platform.batch.entrada import build_gold_query_filters, rows_for_motor

# Caminho A — typed (C4 emite CONFIG parcial)
cfg = SpecialtyConfig.model_validate(
    {"specialty_id": bundle["specialty_id"], "config_version": bundle["config_version"], "data": bundle["data"]}
)
filters = build_gold_query_filters(
    cfg, bundle["run_params"]["data_inicio"], bundle["run_params"]["data_fim"]
)
pdf = GoldDataManager(spark).get_data(
    domains=cfg.data.gold_domains, filters=filters, limit=bundle["run_params"]["limit"]
).toPandas()
rows = rows_for_motor(pdf, date.today(), cfg.data.column_map, cfg.data.filters.gold_filter)

# Caminho B — filters dict directo (sem SpecialtyConfig; C4 monta o dict)
filters = {
    "data_inicio": bundle["run_params"]["data_inicio"],
    "data_fim": bundle["run_params"]["data_fim"],
    **bundle["data"]["filters"].get("gold_query", {}),
}
pdf = GoldDataManager(spark).get_data(
    domains=bundle["data"]["gold_domains"], filters=filters, limit=bundle["run_params"]["limit"]
).toPandas()
rows = rows_for_motor(
    pdf, date.today(), bundle["data"]["column_map"],
    GoldFilterConfig.model_validate(bundle["data"]["filters"].get("gold_filter", {})),
)
```

Motor NLP: `ClinicalNlpEngine().process(rows, nlp_cfg, …)` — wheel **`nlp_engine`**, fora deste passo.

**Não chamar na busca:** `run_entrada`, `write_staging`, `run_process` (exigem Delta staging).

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `map_config_data()` | `data_spec` → `data.gold_domains`, `data.filters`, `data.column_map` (defaults = contrato hub exame) |
| `map_config_nlp()` | `nlp_spec` + `QueryPlan` → `nlp` completo (`findings`, embeddings, `llm_router`, flags) |
| `map_runtime_profile()` | `query_full` \| `query_standard` \| `query_minimal` → liga/desliga camadas do motor |
| `map_llm_router_context()` | Intenção da pergunta → `llm_router.specialty_context` (query-native, não YAML hepato) |
| `map_semantic_terms()` | `findings` flat → `embeddings.semantic_terms` |
| `map_rads_block()` | Se pergunta menciona xxRADS → bloco `rads` mínimo; senão off / `relevance_mode: normal` |
| `map_run_params()` | datas + `limit` → `run_params` (equivalente widgets E2E) |
| `map_gold_query()` | procedimento/modalidade → `data.filters.gold_query` (**extensão busca**; vazio se só datas) |
| `pass_match_mode()` | `nlp_spec.match_mode` → top-level (orquestrador pós-motor) |
| `build_runtime_bundle()` | Monta `CONFIG` parcial + `run_params` |

### Mapeamento `data_spec` → `CONFIG["data"]` (v0)

| `QueryPlan.data_spec` | Destino | Nota |
|-----------------------|---------|------|
| `dt_exame_from` / `to` | `run_params.data_inicio` / `data_fim` | Igual widgets `ntb_ia_motor_e2e` |
| `procedimento_contains[]` | `gold_query.proced_descricao_ajustado` (expr `rlike`) | Passthrough suportado por `get_data`; preferir coluna ajustada |
| `modalidade[]` | `gold_query.cod_procedimento` (lista → `IN`) ou reservada `tp_codigo` | Códigos TUSS; **não** usar `filters.modality` |
| (opcional) termos de domínio amplos | `data.filters.gold_filter.keywords` | Filtro pós-fetch no texto do laudo (ex.: anatomia ampla) |
| `limit` | `run_params.limit` | |

### Mapeamento `nlp_spec` → `CONFIG["nlp"]` (v0)

- `findings[]` → `nlp.findings`: cada termo vira categoria, ex. `{"q0": ["PMAP"], "q1": ["RVP"]}`.
- `findings[]` (flat) → `embeddings.semantic_terms` — sinónimos / variantes para expansão semântica.
- `runtime.profile = query_full` (alvo): regra + `calibrated_hybrid` + embeddings `fallback` + `llm_router.mode: llm`.
- `map_llm_router_context()`: texto curto derivado da pergunta (critério de relevância **ad-hoc**, não `specialty_context` de hepatologia).
- Conexão LLM do router: `llm_runtime` do orquestrador injeta `base_url`, `model`, `api_key_env` em `nlp.llm_router`.
- `segmentation.mode = full_doc` — busca geral sem `target_organs` fixos; segmentação por órgão só se o plano explicitar.
- xxRADS: ligar bloco `rads` / `llm_fallback` **somente** quando a pergunta pedir categorização RADS; caso contrário omitir ou `relevance_mode: normal`.
- `match_mode: all` → orquestrador filtra linhas com hit em **todas** as categorias de `findings` após `process`.

**Perfis `query_standard` / `query_minimal`:** C4 desliga camadas progressivamente (`llm_router.enabled=false`; embeddings off no minimal) — ver § [Princípio query-native](#princípio-de-produto--busca-query-native).

### O que não faz

- Não executa `get_data()` nem `process()`.
- Não lê YAML de especialidade (configs montadas em dict Python no app).
- Não valida plano (C3 já validou).
- Não chama LLM.

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `procedimento_contains` vazio | `filters` só com datas/modalidade |
| `findings` vazio + row-LLM on | `nlp_config` mínimo; triagem pode ser só row-LLM |
| Modalidade como texto livre (ex. "tomografia") | C3/C2 devem normalizar para código; C4 só aceita códigos em `gold_query` |
| `procedimento_contains` com vários termos | Lista de expr no mesmo campo Gold (OR no SQL) |

### Critério — escopo fechado v0

- [x] Contrato `RuntimeBundle` definido  
- [x] Mapeamento data_manager alinhado às APIs da lib (`gold.py`, `entrada.py`)  
- [x] Perfil `query_full` e mapeamento motor completo documentado  
- [x] Funções internas nomeadas  
- [ ] Revisão do time após fechar § data_manager

---

## data_manager

**Status:** Fechado v0  
**Papel:** Estreitar cohort no Clinical Data Hub (SQL) e entregar **`rows_motor`** — linhas no contrato de entrada do motor NLP.

> Escopo deste componente na busca: **fatia de coleta + normalização pós-fetch**. Não inclui NLP, row-LLM, Delta staging nem distribuição.

### Pacotes e exports relevantes

| Símbolo | Import | Papel |
|---------|--------|-------|
| `GoldDataManager` | `fabrica_ia.data_manager.gold` | API principal — `get_data`, introspecção de domínios/campos |
| `SpecialtyConfig`, `GoldFilterConfig`, `DataConfig` | `fabrica_ia.data_manager.models` | Validação tipada de `data.*` (opcional na busca) |
| `build_gold_query_filters` | `fabrica_ia.nlp_platform.batch.entrada` | Mescla datas + `gold_query` → dict para `get_data` |
| `rows_for_motor` | idem | DataFrame pandas → `list[dict]` motor |
| `extract_laudo`, `apply_gold_filter` | idem | Composição fina (testes / pipeline custom) |

`GoldDataManager` **não** está em `data_manager.__all__` — import explícito do submódulo `gold`.

### Entrada

| Campo | Tipo | Origem |
|-------|------|--------|
| `spark` | `SparkSession` | runtime Databricks |
| `domains` | `list[str]` | C4 default: 4 domínios exame (ou `cfg.data.gold_domains`) |
| `filters` | `dict` | C4 / `build_gold_query_filters` — ver § filtros |
| `limit` | `int` | C4 `run_params.limit` |
| `column_map` | `dict` | C4 defaults (espelho hepatologia) |
| `gold_filter` | `GoldFilterConfig` | C4 — substring pós-fetch no laudo |

### Saída

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `rows_motor` | `list[dict]` | Linhas prontas para `ClinicalNlpEngine.process` |
| Chaves por linha | | `id_exame`, `id_paciente`, `id_unidade`, `exm_laudo_texto`, `exm_mod`, `exm_tipo`, `dt_exame` (+ `dt_execucao`, `etl_data_carga` internos) |

**Laudo:** `exm_laudo_texto` vem de `extract_laudo` sobre struct `proced_lista_exames` — **`column_map["exm_laudo_texto"]` não é usado** por `rows_for_motor`.

### Duas camadas de filtro (obrigatório entender)

| Camada | Onde | Chaves / mecanismo | Exemplo busca |
|--------|------|-------------------|---------------|
| **SQL** (`get_data`) | Spark / Gold | Reservadas exame: `data_inicio`, `data_fim`, `cod_procedimento`, `tp_codigo`, `id_unidade`, … + **passthrough** de coluna real com expr | datas + `proced_descricao_ajustado` rlike |
| **Python** (`gold_filter`) | pós-fetch | `keywords` + `mode` (`any`/`all`/`regex`) sobre texto do laudo | opcional — domínio amplo (“abd”, “mama”) |

`DataFilters.gold_query` no CONFIG alimenta a camada SQL via `build_gold_query_filters`.  
`DataFilters.gold_filter` alimenta a camada Python em `rows_for_motor`.

### Semântica de `get_data(filters=…)` (exame — MVP)

Reservadas documentadas em `gold.py`:

- `data_inicio` / `data_fim` → filtram `dt_dia_exame`
- `cod_procedimento`, `tp_codigo` → `IN(...)`
- Qualquer coluna exame existente → escalar/lista (`IN`) ou `{"value", "expr"}` (OR entre itens)

Formatos de valor:

1. escalar ou lista → `IN(...)`
2. `{"value": "x", "expr": "rlike '(?i)%value%'"}` → predicado SQL

### Funções internas do orquestrador busca (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `resolve_domains()` | Retorna `gold_domains` default ou do bundle |
| `build_sql_filters()` | Caminho A: `build_gold_query_filters`; caminho B: merge manual datas + `gold_query` |
| `fetch_gold_df()` | `GoldDataManager.get_data` + `limit` |
| `to_motor_rows()` | `df.toPandas()` → `rows_for_motor` |
| `handle_empty_cohort()` | `rows_motor` vazio → resposta UI sem chamar motor |

### O que não faz (MVP)

- Não chama `run_entrada`, `write_staging`, `run_process`, `LocationResolver` (só batch Delta).
- Não usa `load_specialty_config` / YAML de especialidade clínica.
- Não aplica `filters.modality` nem `custom_sql_predicate` (campos não ligados na lib v0.5.8).
- Não decripta PII (`decrypt_pii=False` default).
- Não executa NLP, row-LLM nem monta resultado para UI.

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `get_data` retorna 0 linhas | `rows_motor=[]` → UI “sem exames”; skip motor |
| Laudo ausente em `proced_lista_exames` | Linha descartada em `rows_for_motor` |
| `gold_filter.keywords` vazio | Pass-through (todas as linhas com laudo seguem) |
| `limit` atingido no SQL | Cohort truncado; UI indica truncamento |
| Domínio paciente + exame no futuro | `get_data` faz join por `id_paciente`; MVP busca só domínios exame |
| Filtro em coluna inexistente | Erro Spark em runtime — C3/C4 devem usar chaves válidas |

### Critério — escopo fechado v0

- [x] APIs lib identificadas (`GoldDataManager`, `entrada.*`)  
- [x] Duas camadas SQL vs Python documentadas  
- [x] Caminhos com e sem `SpecialtyConfig`  
- [x] Contrato `rows_motor` e origem do laudo  
- [x] Distinção lib vs batch plataforma (`run_entrada`)  
- [ ] Validação DS: `cod_procedimento` vs `tp_codigo` para modalidade (refinável)

---

## nlp_engine

**Status:** Fechado v0  
**Papel:** Triagem clínica **com stack completo** sobre `rows_motor` — regra, expansão semântica, desambiguação LLM e (quando aplicável) xxRADS — entregando **`rows_candidatos`** para row-LLM opcional e empacotador.

> Pacote: wheel **`nlp_engine`** (`nlp-engine-lib`), import `from nlp_engine.nlp_engine.engine import ClinicalNlpEngine`. **Não** usar `fabrica_ia.nlp_engine` (removido do `fabrica-ia-lib` hml). Config **query-native** montada pelo C4 — **não** YAML de especialidade.

### Pacotes e API

| Símbolo | Import | Papel na busca |
|---------|--------|----------------|
| `ClinicalNlpEngine` | `nlp_engine.nlp_engine.engine` | `process(rows, nlp_config, …)` — pipeline completo |
| `validate_engine_output_row` | `nlp_engine.nlp_engine.output_invariants` | Validação opcional pós-process |
| `merge_with_shared_organs` | `nlp_engine.nlp_engine.config_loader` | Opcional — só se plano pedir segmentação por órgão |
| `extract_rads_summary` / promoção RADS | via `engine.process` | Se C4 ligar bloco `rads` |

### Entrada

| Campo | Tipo | Origem |
|-------|------|--------|
| `rows_motor` | `list[dict]` | § data_manager — contrato motor |
| `nlp_cfg` | `dict` | C4 `bundle["nlp"]` — perfil **`query_full`** (ou degradado) |
| `specialty_id` | `str` | C4 — ex.: `query` (identificador técnico, não especialidade clínica) |
| `config_version` | `str` | C4 `bundle["config_version"]` |
| `match_mode` | `any \| all` | C4 top-level — aplicado pelo **orquestrador** pós-motor |
| `llm_runtime` | `dict` | Orquestrador — conexão serving para `llm_router` (e RADS `llm_fallback` se on) |

### `nlp_cfg` alvo — perfil `query_full`

```python
{
    "findings": {"q0": ["nódulo"], "q1": ["consolidação"]},  # dict[str, list[str]] — da pergunta
    "negation_phrases": ["sem", "nao ha", "não há", "ausencia de", "ausência de"],
    "negation_window": 5,
    "score_policy_version": "v1_bins_legacy",
    "feature_flags": {"rule_engine": true, "calibrated_hybrid": true},
    "segmentation": {"mode": "full_doc"},
    "embeddings": {
        "use_embeddings": true,
        "decision_mode": "fallback",
        "semantic_terms": ["nódulo", "nodulo", "lesão nodular"],  # de map_semantic_terms()
        "similarity_threshold": 0.35,
        "ambiguity_band": [0.35, 0.65],
    },
    "llm_router": {
        "enabled": true,
        "mode": "llm",
        "specialty_context": "<derivado da pergunta — relevância ad-hoc>",
        "uncertainty_band": [0.35, 0.65],
        "fallback_policy": "keep_current",
        "base_url": "...",  # de llm_runtime
        "model": "...",
        "api_key_env": "DATABRICKS_TOKEN",
    },
    # rads: { ... } — só se QueryPlan pedir categorização BI/PI/TI-RADS
}
```

**Pipeline interno (`process`) — todas as camadas disponíveis:**

```
to_plain → segmentação (full_doc default)
  → rule_engine (findings da pergunta)
  → embeddings (sinónimos / fallback na banda)
  → calibrated_hybrid
  → llm_router (só linhas na uncertainty_band)
  → xxRADS (se configurado)
  → fl_relevante + confidence_score + summary_compact
```

**Degradar perfil (POC/dev):** `query_standard` (sem `llm_router`) ou `query_minimal` (só regra) — C4 `map_runtime_profile()`.

### Saída

| Campo | Tipo | Uso busca |
|-------|------|-----------|
| `rows_out` | `list[dict]` | Saída bruta de `process()` — uma linha por laudo |
| `rows_candidatos` | `list[dict]` | Subset com `fl_relevante == 1` + filtro `match_mode: all` (orquestrador) |

**Campos por linha (`process`):**

| Campo | Descrição |
|-------|-----------|
| `fl_relevante` | `0 \| 1` — gate principal de candidato |
| `confidence_score` | `0.0–1.0` — score após regra + embeddings + calibração |
| `exm_laudo_texto_tratado` | laudo após `to_plain` |
| `exm_laudo_resultado` | JSON: `summary_compact`, `decision_source`, `semantic_score`, `llm_called`, … |
| `decision_source` | `rule`, `hybrid`, `embedding_fallback`, `llm_router_llm_*`, `rads_promotion`, … |
| Pass-through | `id_exame`, `id_paciente`, `exm_mod`, `exm_tipo`, `dt_exame`, … |

**Evidência para UI (C6):** `summary_compact`, `row_llm_evidence_snippet` (C5), ou recorte de `exm_laudo_texto_tratado`.

### Funções internas do orquestrador busca (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `build_nlp_cfg()` | C4 `nlp` + injeção `llm_runtime` no `llm_router` |
| `run_nlp_engine()` | `ClinicalNlpEngine().process(rows_motor, nlp_cfg, specialty_id=…, config_version=…)` |
| `filter_candidates()` | Mantém `fl_relevante == 1` |
| `apply_match_mode_all()` | Se `match_mode: all`: hit em **todas** as categorias de `findings` |
| `skip_if_empty()` | Sem candidatos → resposta UI; row-LLM só se plano pedir |

### Consumo (lib)

```python
from nlp_engine.nlp_engine.engine import ClinicalNlpEngine

nlp_cfg = build_nlp_cfg(bundle, llm_runtime)
engine = ClinicalNlpEngine()
rows_out = engine.process(
    rows_motor,
    nlp_cfg,
    specialty_id=bundle["specialty_id"],
    config_version=bundle["config_version"],
)
rows_candidatos = [r for r in rows_out if r.get("fl_relevante") == 1]
# + apply_match_mode_all se bundle["match_mode"] == "all"
```

### O que não faz (MVP)

- Não lê YAML de hepatologia / BI-RADS como config base.
- Não usa `run_process` / staging Delta.
- Não extrai medidas estruturadas com operador (VEF1 &lt; 30%, creatinina &gt; 2) — camada **row-LLM (C5)**.
- Não empacota `resultado_rows` nem define `match_source` final (C6).
- Não implementa `match_mode: all` **dentro** do motor — orquestrador.

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `rows_motor` vazio | Skip `process`; UI sem resultados |
| `findings` vazio | Motor com regra mínima; candidatos podem vir só do row-LLM ou cohort |
| `llm_router` falha na banda | `fallback_policy: keep_current` — decisão pré-LLM mantida |
| Pergunta só numérica (row-LLM) | Motor pode passar cohort; C1 envia `rows_motor` ao C5 |
| Pergunta com xxRADS | C4 liga `rads`; motor promove conforme política |
| `nlp_cfg` inválido | Erro em runtime — C4 valida forma |

### Critério — escopo fechado v0

- [x] Wheel e import canónico (`nlp_engine.nlp_engine`)  
- [x] Perfil **`query_full`** como alvo (stack completo documentado)  
- [x] Distinção query-native vs especialidade  
- [x] Gate `fl_relevante` + `match_mode` no orquestrador  
- [x] Motor (triagem) vs row-LLM (extração estruturada) — camadas complementares  
- [ ] Calibração de prompts `llm_router` query-native na bancada  
- [ ] Heurística exata de `match_mode: all` refinável na impl.

---

## C5 — Plugin row-LLM

**Status:** Fechado v0  
**Papel:** Refino **por linha** em `rows_candidatos` quando o plano exige **extração estruturada** (medida, campo clínico, operador) — **complementa** o stack do motor (`llm_router` tria relevância; row-LLM extrai valor e aplica regra).

> Componente **NOVO (app)** — não mora em `fabrica_ia` nem em `nlp_engine` no MVP. O orquestrador importa um módulo `row_llm` (ou pacote interno da busca).

### Entrada

| Campo | Tipo | Origem |
|-------|------|--------|
| `rows_candidatos` | `list[dict]` | § nlp_engine — gate `fl_relevante` (+ `match_mode`) |
| `row_llm_spec` | `dict` | C4 / `QueryPlan` validado |
| `llm_runtime` | `dict` | app — `base_url`, `model`, `api_key_env` (mesmo padrão cluster Databricks que o runner; **prompts próprios**) |

`row_llm_spec` (v0):

```json
{
  "enabled": true,
  "field": "VEF1",
  "operator": "<",
  "value": 30,
  "unit": "% predito"
}
```

### Saída

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `rows_refined` | `list[dict]` | Subset que passou na regra row-LLM (ou pass-through se `enabled: false`) |

**Campos adicionados por linha (quando `enabled: true`):**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `row_llm_passed` | `bool` | `true` se valor extraído satisfaz `operator` vs `value` |
| `row_llm_extracted_value` | `number \| null` | Valor numérico normalizado |
| `row_llm_extracted_unit` | `str \| null` | Unidade reportada pelo LLM |
| `row_llm_evidence_snippet` | `str` | Trecho do laudo usado na extração |
| `row_llm_error` | `str \| null` | Erro parse/LLM — linha descartada se não recuperável |

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `should_run_row_llm(spec)` | `spec.get("enabled") is True` |
| `build_row_llm_prompt(field, unit, laudo_excerpt)` | Prompt fixo + schema JSON de saída |
| `call_row_llm(prompt, llm_runtime)` | 1 completion HTTP (OpenAI-compatible / Databricks serving) |
| `parse_row_llm_response(raw)` | JSON → `{value, unit, snippet}` |
| `apply_operator(extracted, operator, threshold)` | Comparação em Python (`<`, `<=`, `>`, `>=`, `=`) |
| `process_row(row, spec, llm_runtime)` | Pipeline por linha |
| `run_row_llm_plugin(rows, spec, llm_runtime)` | Facade: pass-through ou map+filter |

### Schema JSON esperado do LLM (v0)

```json
{
  "field": "VEF1",
  "value": 24.5,
  "unit": "% predito",
  "snippet": "VEF1 = 24,5% do previsto"
}
```

Prompt system (resumo): extrair **apenas** o campo pedido; `value` numérico; `snippet` curto do laudo; JSON only.

### Fluxo

```
enabled=false → rows_refined = rows_candidatos (cópia)

enabled=true  → para cada row em rows_candidatos:
                  excerpt = laudo tratado (max_chars configurável, ex. 4000)
                  LLM → parse → apply_operator
                  se passed → append com campos row_llm_*
```

### O que não faz (MVP)

- Não reconsulta o hub — só `rows_candidatos` (ou cohort se motor não filtrou).
- Não substitui `llm_router` / embeddings / regra do motor — resolve outro problema (extração + operador).
- Não altera `fl_relevante` / `confidence_score` do motor (campos separados).
- Não empacota resposta UI (C6).
- Não faz batch massivo paralelo obrigatório — MVP pode ser sequencial com teto de linhas (= `limit` já aplicado).

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `enabled: false` | Pass-through imediato |
| `rows_candidatos` vazio | `rows_refined=[]` |
| LLM não acha o campo | `value=null` → `row_llm_passed=false` → linha descartada |
| JSON inválido | `row_llm_error` → linha descartada (log) |
| Valor com vírgula (`24,5`) | Normalizar para float em `parse_row_llm_response` |
| Operador `=` com float | Tolerância MVP: igualdade exata após round 1 casa decimal |
| Só row-LLM (findings vazios) | Candidatos podem vir do cohort inteiro se motor não filtrou — orquestrador pode passar `rows_motor` quando `findings` vazio e row_llm on (decisão documentada no C1) |

### Critério — escopo fechado v0

- [x] Contrato entrada/saída e campos `row_llm_*`  
- [x] Complementar ao motor (não substituto do `llm_router`)  
- [x] Schema LLM + operadores whitelist (C3)  
- [x] Funções internas nomeadas  
- [x] Pass-through quando `enabled: false`  
- [ ] Prompt few-shot por campo clínico refinável (VEF1 piloto)

---

## C6 — Empacotador resultado

**Status:** Fechado v0  
**Papel:** Consolidar linhas aprovadas em **`resultado_rows`** (tabela para UI/export) com evidência, metadados de auditoria e `match_source`.

> Componente **NOVO (app)** — funções puras de projeção; sem lib obrigatória no MVP.

### Entrada

| Campo | Tipo | Origem |
|-------|------|--------|
| `rows_refined` | `list[dict]` | C5 (ou `rows_candidatos` se C5 skipped) |
| `row_llm_spec` | `dict` | C4 — define se houve passo LLM |
| `output_spec` | `dict` | C4 — colunas desejadas |
| `query_meta` | `dict` | `pergunta`, `specialty_id`, `config_version`, `run_id` (opcional) |

### Saída

`resultado_rows` — `list[dict]` (UI pode materializar como `df.resultado` / export CSV).

**Colunas canónicas MVP** (default `output_spec.columns`):

| Coluna | Origem |
|--------|--------|
| `id_exame` | pass-through motor |
| `dt_exame` | pass-through motor |
| `id_paciente` | pass-through (se presente) |
| `exm_tipo` | procedimento / tipo exame |
| `trecho_evidencia` | `row_llm_evidence_snippet` ou 1º item de `summary_compact` ou recorte do laudo tratado |
| `confidence_score` | motor |
| `match_source` | `nlp` se `row_llm_spec.enabled=false`; `nlp+llm` se `enabled=true` e `row_llm_passed` |
| `valor_extraido` | `row_llm_extracted_value` (+ `unit` opcional concatenada) — null se só NLP |
| `fl_relevante` | motor |

**Metadados de auditoria (opcional MVP, recomendado):**

| Coluna | Descrição |
|--------|-----------|
| `query_id` / `run_id` | correlação da pergunta |
| `engine_version` | versão wheel `nlp_engine` |
| `config_version` | bundle C4 |

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `resolve_match_source(row, row_llm_spec)` | `nlp` \| `nlp+llm` |
| `pick_evidence_snippet(row)` | Prioridade: row_llm → summary_compact → laudo tratado (max ~300 chars) |
| `project_row(row, output_spec)` | Seleciona/renomeia colunas |
| `build_resultado_row(row, meta, spec)` | Monta dict final |
| `pack_resultado(rows, spec, meta)` | Facade → `resultado_rows` |

### Exemplo de linha resultado

```json
{
  "id_exame": "E123",
  "dt_exame": "2024-06-01",
  "exm_tipo": "Espirometria",
  "trecho_evidencia": "VEF1 = 24,5% do previsto",
  "confidence_score": 0.9,
  "match_source": "nlp+llm",
  "valor_extraido": "24.5 % predito",
  "fl_relevante": 1
}
```

### O que não faz (MVP)

- Não renderiza UI (C7) nem exporta arquivo (C7 pode chamar export).
- Não reexecuta motor nem row-LLM.
- Não deduplica pacientes nem agrega estatísticas globais.
- Não persiste em Delta (busca ad-hoc).

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `rows_refined` vazio | `resultado_rows=[]` → UI mensagem vazia |
| Coluna em `output_spec` ausente na linha | `null` na projeção |
| `summary_compact` lista vazia | Fallback para recorte de `exm_laudo_texto_tratado` |
| Múltiplos trechos em `summary_compact` | MVP: primeiro item; futuro: join com ` \| ` |
| Truncamento global | Respeitar `limit` já aplicado no hub — C6 não re-limita |

### Critério — escopo fechado v0

- [x] Contrato `resultado_rows` / colunas MVP  
- [x] `match_source` definido  
- [x] Regra de evidência documentada  
- [x] Funções internas nomeadas  
- [x] Separação empacotamento vs UI  
- [ ] Schema export CSV/XLSX delegado a C7

---

## C1 — Orquestrador

**Status:** Fechado v0  
**Papel:** Coordenar o fluxo C2→C6 por pergunta; expor **uma API de app** (`run_busca`) que injeta Spark/LLM e devolve resposta para a UI.

> MVP: **funções sequenciais** (sem LangGraph). LangGraph é evolução v1 quando houver multi-turn e persistência de estado.

### Entrada

| Campo | Tipo | Origem |
|-------|------|--------|
| `pergunta` | `str` | C7 / API |
| `spark` | `SparkSession` | runtime Databricks |
| `llm_runtime` | `dict` | app — endpoint interpretador + row-LLM |
| `defaults` | `dict` | opcional — limites, `specialty_id` query, domínios default |

### Saída

`BuscaResponse` (dict):

```json
{
  "status": "ok | needs_clarification | reject | empty | error",
  "clarification_question": "string | null",
  "errors": ["string"],
  "resultado_rows": [],
  "meta": {
    "query_plan": {},
    "n_cohort": 0,
    "n_candidatos": 0,
    "n_resultado": 0,
    "truncated": false
  }
}
```

### Estado interno (`SearchState` — v0)

| Campo | Preenchido em |
|-------|----------------|
| `pergunta` | entrada |
| `query_plan` | nó `interpret` (C2) |
| `validation` | nó `validate` (C3) |
| `bundle` | nó `build_bundle` (C4) |
| `rows_motor` | nó `fetch_data` |
| `rows_candidatos` | nó `run_nlp` + `filter_candidates` |
| `rows_refined` | nó `row_llm` (C5) ou cópia de candidatos |
| `resultado_rows` | nó `pack` (C6) |

### Grafo sequencial MVP

```
interpret (C2)
  → needs_clarification? → return BuscaResponse
validate (C3)
  → reject? → return BuscaResponse
build_bundle (C4)
fetch_data (data_manager: get_data + rows_for_motor)
  → vazio? → status=empty
run_nlp (nlp_engine.process)
filter_candidates (fl_relevante + match_mode)
row_llm (C5) — skip se enabled=false
pack (C6)
  → status=ok + resultado_rows
```

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `run_busca(pergunta, spark, llm_runtime, defaults)` | Facade pública |
| `node_interpret(state)` | C2 |
| `node_validate(state)` | C3 |
| `node_build_bundle(state)` | C4 |
| `node_fetch_data(state, spark)` | data_manager |
| `node_run_nlp(state)` | nlp_engine |
| `node_filter_candidates(state)` | gate + `match_mode: all` |
| `node_row_llm(state, llm_runtime)` | C5 ou no-op |
| `node_pack(state)` | C6 |
| `to_response(state)` | Monta `BuscaResponse` |

**Regra `findings` vazio + `row_llm` on:** `node_filter_candidates` pode receber `rows_motor` inteiro em vez de saída filtrada do motor (cohort → row-LLM direto).

### O que não faz (MVP)

- Não implementa LangGraph / persistência multi-turn (v1).
- Não contém lógica de negócio das libs (só orquestra chamadas).
- Não renderiza UI (C7).
- Não grava Delta nem MLflow.
- Não re-tenta LLM automaticamente (erro → `status=error` ou linha descartada no C5).

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| C2 `needs_clarification` | Para antes do hub; UI mostra pergunta |
| C3 `reject` | Para; `errors` ou `clarification_question` |
| Cohort vazio | `status=empty`; sem motor/row-LLM |
| Cohort ok, 0 candidatos após motor/row-LLM | `status=ok`, `resultado_rows=[]`, `meta.n_resultado=0` — UI “nenhum achado” (distinto de cohort vazio) |
| Exceção Spark | `status=error`; mensagem sanitizada |
| `row_llm` off | `node_row_llm` copia `rows_candidatos` → `rows_refined` |

### Critério — escopo fechado v0

- [x] Grafo sequencial documentado  
- [x] `SearchState` + `BuscaResponse`  
- [x] Facade `run_busca`  
- [x] Ramos clarification / reject / empty  
- [x] Skip C5 documentado  
- [ ] LangGraph — fora do MVP

---

## C7 — UI

**Status:** Fechado v0  
**Papel:** Interface conversacional mínima — captura pergunta NL, exibe esclarecimentos/erros e tabela de `resultado_rows`; export opcional.

> MVP: **Databricks App**, Streamlit ou painel interno equivalente — decisão de stack fica com o time; contrato é com o orquestrador (`run_busca`), não com as libs.

### Entrada (usuário)

| Ação | Descrição |
|------|-----------|
| Texto NL | Campo chat — envia `pergunta` ao C1 |
| Resposta a esclarecimento | Nova `pergunta` (turno seguinte) com contexto na sessão UI (v0: usuário reescreve; v1: histórico) |

### Saída (usuário)

| `BuscaResponse.status` | UI |
|------------------------|-----|
| `ok` | Tabela com `resultado_rows` |
| `needs_clarification` / `reject` | Balão com `clarification_question` ou `errors` |
| `empty` | Mensagem “nenhum exame no período/filtro” (cohort vazio no hub) |
| `ok` + `n_resultado=0` | Mensagem “nenhum laudo com o achado pedido” |
| `error` | Mensagem genérica + log técnico (sem PHI) |

### Colunas da tabela (default)

`id_exame`, `dt_exame`, `exm_tipo`, `trecho_evidencia`, `valor_extraido`, `match_source`, `confidence_score`

Respeitar `output_spec.columns` quando presente no plano.

### Funções internas (MVP)

| Função | Responsabilidade |
|--------|------------------|
| `on_submit(pergunta)` | Chama `run_busca` |
| `render_clarification(msg)` | Exibe pergunta de volta |
| `render_result_table(rows)` | DataFrame / grid |
| `render_empty()` / `render_error()` | Estados vazios |
| `export_resultado_csv(rows)` | Download opcional MVP |
| `show_meta(meta)` | Rodapé: `n_cohort`, truncamento |

### O que não faz (MVP)

- Não chama `GoldDataManager` nem `ClinicalNlpEngine` diretamente.
- Não edita `QueryPlan` manualmente (só NL).
- Não autentica/governa ACL do hub (assume sessão Databricks já autorizada).
- Não substitui Central de Captação programada.

### Edge cases MVP

| Caso | Comportamento |
|------|----------------|
| `resultado_rows` grande | Paginação ou scroll; aviso se `meta.truncated` |
| Export CSV | Colunas = tabela visível; sem PHI extra |
| Loading | Spinner entre submit e resposta |
| Duplo submit | Desabilitar botão enquanto `run_busca` corre |

### Critério — escopo fechado v0

- [x] Contrato com `BuscaResponse`  
- [x] Estados UI mapeados  
- [x] Tabela + esclarecimento  
- [x] Export CSV opcional documentado  
- [x] Sem acoplamento às libs  
- [ ] Stack UI (Streamlit vs Databricks App) — decisão time

---

## Escopo MVP — fechamento

Componentes **especificados em v0** neste documento. **Implementação bloqueada** até o [gate de planejamento](#gate-de-fechamento-do-planejamento) estar assinado pelo time.

**Visão de produto (acordada):** buscas gerais personalizadas com **stack motor completo** (`query_full`) + row-LLM quando a pergunta exigir extração estruturada. Especialidades existentes = exemplo de composição das libs, não modelo clínico a copiar.

---

## Gate de fechamento do planejamento

Checklist único antes de qualquer código. Cada linha deve estar **Fechado** ou explicitamente **Adiado v1** com dono.

### A — Produto e limites

| # | Item | Estado | Nota |
|---|------|--------|------|
| A1 | Produto distinto da Central de Captação | **Fechado** | Doc + princípio query-native |
| A2 | Perguntas ad-hoc; não copiar YAML de especialidade | **Fechado** | § Princípio query-native |
| A3 | Stack motor `query_full` como alvo | **Fechado** | Regra + embeddings + `llm_router` + RADS condicional |
| A4 | Row-LLM como camada extra (não substituto do motor) | **Fechado** | § C5 |
| A5 | Fora do MVP: LangGraph, multi-turn, ACL própria, Delta persist | **Fechado** | Explícito em C1/C7 |

### B — Decisões de desenho (fechar no doc)

| # | Item | Estado | Resolução proposta (v0) |
|---|------|--------|-------------------------|
| B1 | `BuscaResponse.status` — cohort vazio vs zero achados | **Fechado** | `empty` = 0 linhas do hub; `ok` + `n_resultado=0` = cohort ok mas nenhum laudo passou motor/row-LLM; UI mensagens distintas |
| B2 | `match_mode: all` — algoritmo | **Fechado** | Após `process`, exige para cada chave `qk` em `findings` pelo menos um termo da lista presente em `summary_compact` **ou** laudo tratado (case-insensitive); se `summary_compact` vazio, fallback laudo tratado |
| B3 | `findings` vazio + `row_llm` on | **Fechado** | Orquestrador passa `rows_motor` ao C5; motor corre com `findings` mínimo ou skip se C3 permitir só row-LLM |
| B4 | `map_llm_router_context` query-native | **Fechado** | Template fixo + injeção da pergunta e termos — ver § abaixo |
| B5 | `specialty_id` técnico | **Fechado** | Valor fixo `query` (não especialidade clínica); versionamento via `config_version` |
| B6 | Perfis degradados POC | **Fechado** | `query_standard` / `query_minimal` só para dev; produção = `query_full` |
| B7 | Export resultado | **Fechado** | CSV no MVP; XLSX v1 |
| B8 | Coluna canónica modalidade no Gold | **Aberto** | `cod_procedimento` vs `tp_codigo` — **validação DS obrigatória** |
| B9 | Normalização NL → código TUSS / modalidade | **Aberto** | C2 few-shots + catálogo ou tabela de referência — definir fonte com DS |
| B10 | Onde mora o código da app | **Aberto** | Proposta: `fabrica-ia-plataforma/apps/busca_conversacional/` — confirmar com time |
| B11 | Stack UI MVP | **Aberto** | Proposta faseada: (1) notebook composition root + widgets; (2) Databricks App — confirmar |
| B12 | Endpoint LLM único vs dois (interpretador vs row-LLM) | **Aberto** | Proposta: mesmo `llm_runtime`; prompts distintos C2 vs C5 |

### C — Contratos entre componentes

| # | Item | Estado |
|---|------|--------|
| C1 | `QueryPlan` schema | Especificado |
| C2 | `ValidationResult` | Especificado |
| C3 | `RuntimeBundle` + `run_params` | Especificado |
| C4 | `rows_motor` / `rows_candidatos` / `rows_refined` | Especificado |
| C5 | `BuscaResponse` / `resultado_rows` | Especificado |
| C6 | Diagrama drawio alinhado ao doc | Especificado — rever após B8–B12 |

### D — Governança e backlog

| # | Item | Estado |
|---|------|--------|
| D1 | Histórias **S20–S27** no `anexo03` | **Aberto** — especificadas no drawio (aba Histórias); falta registo formal |
| D2 | Revisão clínica de prompts query-native | **Aberto** — pós-planejamento, pré-produção |
| D3 | Critérios de aceite MVP (N perguntas piloto) | **Aberto** — definir lista com produto/clínica |
| D4 | Sign-off explícito deste gate | **Aberto** |

### Critério — plano fechado para implementação

Todos os itens **B8–B12**, **D1**, **D4** em **Fechado** (ou adiado v1 assinado). Itens **D2–D3** podem correr em paralelo à POC se não forem bloqueantes de merge.

---

### Anexo planejamento — `map_llm_router_context` (query-native)

Texto injetado em `nlp.llm_router.specialty_context` — **não** copiar hepatologia.

```
Tarefa: decidir se o excerto de laudo responde à pergunta do utilizador.
Pergunta: {pergunta}
Termos de interesse: {findings_flat}
Critério relevante=true: o laudo menciona ou implica claramente o achado pedido, sem negação dominante.
Critério relevante=false: normalidade explícita, negação, achado não relacionado, ou texto insuficiente.
Responda apenas via contrato JSON do router (relevante boolean).
```

`findings_flat` = join dos termos de `nlp_spec.findings`. C4 monta; C2 pode pré-preencher sinónimos em `embeddings.semantic_terms`.

---

## Próximo passo (planejamento — sem implementação)

1. **Workshop de fechamento** — resolver B8–B12 com DS + engenharia + produto (30–45 min).  
2. **Registrar S20–S27** no `anexo03` (espelhar aba drawio *Histórias*).  
3. **Atualizar drawio** componentes se B10/B11 mudarem hosting/UI.  
4. **Sign-off** na tabela D4 — só então abrir pasta de código / POC.
