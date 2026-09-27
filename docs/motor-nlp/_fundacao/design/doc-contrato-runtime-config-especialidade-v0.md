# Contrato Runtime por Especialidade (v0)

**Status:** Draft v0 — alinhamento intermediario registrado para desbloquear trabalho em `nlp_engine`; formalizacao canonica e T03.x ficam para a etapa correspondente.  
**Escopo:** Fase 1 (rule-based), runtime Databricks

## 1) Objetivo

Definir o contrato do `config.yaml` de especialidade (runtime), separando responsabilidades de `data_manage`, `nlp_engine` e `monitoring`.

## 2) Principios

- Variacao por especialidade vem de config, nao de hardcode em Python.
- Notebook e Composition Root: le YAML e injeta dict nas libs.
- `nlp_engine` e agnostico de origem fisica (gold/diamond); recebe `df` no contrato.
- Filtros de linha (gold -> `diamond_{esp}_entrada`) pertencem a `data_manage`.

## 3) Estrutura canonica do YAML runtime

```yaml
specialty_id: "<especialidade>"
config_version: "1.0.0"

nlp: {}
data: {}
monitoring: {}
```

## 4) Especificacao da secao `nlp`

Campos esperados (v0):

- `shared_organs_path` (string)
- `target_organs` (list[string])
- `findings` (map[string, list[string]])
- `negation_window` (int, default conforme engine)
- `threshold` (float) ou `thresholds` (map)
- `text_pipeline` (map opcional; ex.: `trailing_line_patterns`)
- `feature_flags` (map opcional)

**Regra:** termos clinicos, orgaos, achados e limiares ficam aqui.

## 5) Especificacao da secao `data`

Campos esperados (v0):

- `io_schema_version` (string, ex.: `clinical_nlp_io_v1`)
- `sources` (map de tabelas de origem, ex.: gold exame/paciente)
- `staging_entrada` (string: `tb_diamond_mod_{esp}_entrada`)
- `output_saida` (string: `tb_diamond_mod_{esp}_saida`)
- `materialize_entrada` (bool)
- `motor_input_mode` (`from_staging_entrada` | `from_sources_filtered`)
- `filters` (map de filtros de linha — ver secao 10)
- `column_map` (map: origem -> campos canonicos)

**Regra:** selecao e transformacao de dados (incluindo gold -> diamond entrada) ficam aqui.

## 6) Especificacao da secao `monitoring`

Campos esperados (v0):

- `metrics_table` (string)
- `alert_threshold_*` (floats/ints conforme politica operacional)

## 7) Contrato canonico de entrada no `nlp_engine`

`data_manage` deve entregar ao motor um `df` com, no minimo:

- `id_exame`
- `id_paciente`
- `id_unidade`
- `exm_laudo_texto`
- `exm_mod`
- `exm_tipo`
- `dt_exame`

## 8) Contrato canonico de saida do `nlp_engine`

Saida minima esperada:

- `id_predicao`
- `dt_execucao`
- `specialty_id`
- `config_version`
- `engine_version`
- `fl_relevante`
- `confidence_score`
- `exm_laudo_resultado`
- `exm_laudo_texto_tratado`

## 9) Exemplo de YAML (v0)

```yaml
specialty_id: "colon"
config_version: "1.0.0"

nlp:
  shared_organs_path: "../../shared/organs.yaml"
  target_organs: ["colon", "reto"]
  negation_window: 7
  findings:
    lesao: ["lesao", "nodulo", "polipo", "massa"]
  threshold: 0.5
  text_pipeline:
    trailing_line_patterns:
      - "(?i)^\\s*medico\\s+responsavel\\s*$"

data:
  io_schema_version: "clinical_nlp_io_v1"
  sources:
    exame: "{catalog}.gold_corporativo_ia.corporativo.tb_gold_mov_exame"
    paciente: "{catalog}.gold_corporativo_ia.corporativo.tb_gold_mov_paciente"
  staging_entrada: "{catalog}.ia.tb_diamond_mod_colon_entrada"
  output_saida: "{catalog}.ia.tb_diamond_mod_colon_saida"
  materialize_entrada: true
  motor_input_mode: "from_staging_entrada"
  filters:
    modality:
      exclude: ["COL"]
  column_map:
    id_exame: id_exame
    id_paciente: id_paciente
    id_unidade: id_unidade
    exm_laudo_texto: exm_laudo_texto
    exm_mod: exm_mod
    exm_tipo: exm_tipo
    dt_exame: dt_exame

monitoring:
  metrics_table: "{catalog}.ia.tb_diamond_mod_metricas_qualidade"
  alert_threshold_relevance_drop: 0.15
```

## 10) Alinhamento intermediario (v0) — fechado para prosseguir

Decisoes adotadas nesta fase (foco em `nlp_engine`; `data_manage` e formalizacao corporativa evoluem quando necessario):

| Topico | Decisao |
|--------|---------|
| `filters` | **v0 declarativo** como padrao (listas include/exclude por coluna, janelas de data, ranges numericos, regex por coluna). **`custom_sql_predicate`** (string SQL) apenas como **escape** quando o caso nao couber no declarativo — revisao em PR. |
| Versao do YAML | Campo unico **`config_version`** (semver). Nao usar `version` em paralelo no mesmo arquivo. |
| `materialize_entrada` | **Default `true`** nesta fase (alinhado ao legado com tabelas intermediarias no lake). Especialidade pode definir `false` apenas com decisao explicita. |
| Naming Unity Catalog | **Manter padrao legado** inicialmente (ex.: `tb_diamond_mod_{esp}_entrada` / `_saida`). Renomeacoes ou padrao corporativo definitivo ficam para formalizacao futura. |
| Escopo de implementacao | **`nlp_engine`** em primeiro plano. Contrato `data` / `filters` aqui descreve **dependencias e alvo**; implementacao e validacao plenas da `data_manage` na etapa propria. |
| Fonte canonica / T03.x | **Adiado**: ao fechar a etapa de schema YAML e governanca, amarrar a task **T03.x** e promover este documento (ou successor) a fonte canonica. |

Exemplo minimalista de `filters` v0 (ilustrativo; operadores exatos na lib `data_manage`):

```yaml
data:
  filters:
    modality:
      exclude: ["COL"]
    # custom_sql_predicate: "..."  # somente se necessario
```

## 11) Pendencias para formalizacao futura

Itens que **nao** bloqueiam o trabalho atual em `nlp_engine`, mas exigem discussao/aprovacao com o time quando a etapa for alcancada:

- Fechar **schema operacional completo** de `filters` na `data_manage` (todos os operadores, testes, edge cases).
- **Aprovacao formal** de naming Unity Catalog (se divergir do legado).
- **Amarrar** este documento como **fonte canonica** apos revisao EngML/DS.
- Documentar **joins** multi-fonte e parametros de ambiente (`catalog`, `environment`) em spec da `data_manage` quando implementada.
