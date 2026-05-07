# Anexo -- Historias e Tasks de Desenvolvimento

**Vinculado a:** `07-relatorio-final-v0-plataforma-nlp-clinica.md`, `05-roadmap-entregas-sprint-v0.md`, `anexo02-arquitetura-motor-nlp-v0.md`
**Sprints:** 10 dias uteis cada. Sprint 1 (atual) = estudo/proposta. Desenvolvimento comeca Sprint 2.

**Nota sobre estimativas:** os prazos consideram que estamos **extraindo** codigo existente do Grupo 1b (ja em producao), nao escrevendo do zero. Testes sao escritos inline com cada historia.

---

## Convencoes

- **S##** = historia (story)
- **T##.#** = task dentro da historia
- **AC** = criterio de aceite (acceptance criteria)
- Estimativas em dias (d) de trabalho efetivo

**Nota sobre estimativas:** os campos de estimativa estao intencionalmente vazios. Serao definidos via planning poker com o time completo.

**Nota sobre responsaveis:** responsaveis por task (DS vs MLOps) serao definidos no planning da Sprint 2.

---

## FASE 1 -- MVP rule-based (Sprint 2)

### S00 -- Setup de infraestrutura (3 repos + CI)

> Criar os 3 repos independentes, configurar CI minimo e publicar primeiro wheel de cada lib.


| Task  | Descricao                                                                                              | Est. |
| ----- | ------------------------------------------------------------------------------------------------------ | ---- |
| T00.1 | Criar repo `nlp_engine` + estrutura de pacote Python + CI minimo (lint)                                |      |
| T00.2 | Criar repo `data_manage` + estrutura de pacote Python + CI minimo (lint)                               |      |
| T00.3 | Criar repo `monitoring` + estrutura de pacote Python + CI minimo (lint)                                |      |
| T00.4 | Criar repo `plataforma-nlp` com estrutura: `shared/`, `especialidades/_template/`, `tests/`, `deploy/` |      |
| T00.5 | Configurar publicacao de wheel no feed interno para as 3 libs                                          |      |
| T00.6 | Publicar primeiro wheel (v0.1.0) de cada lib com modulo vazio + testes placeholder                     |      |


**Est. total:** 
**AC:** 4 repos criados. 3 wheels publicados no feed. CI rodando lint em cada push.

**Nota:** S00 desbloqueia todas as demais historias. Pode ser executada em paralelo com extracao de codigo (S01/S02 em branch local).

---

### S01 -- Criar TextPipeline compartilhado

> Extrair e unificar a logica de preparacao de texto dos algoritmos existentes (Grupo 1b como base).


| Task  | Descricao                                                                                             | Est. |
| ----- | ----------------------------------------------------------------------------------------------------- | ---- |
| T01.1 | Extrair `to_plain()` do Grupo 1b: deteccao RTF/HTML/plain, pypandoc fallback, ftfy, normalizacao NFKC |      |
| T01.2 | Extrair segmentacao por secao/orgao (headers, ancoras)                                                |      |
| T01.3 | Extrair deteccao de negacao avancada (23 expressoes, janela 7 tokens, multi-token)                    |      |
| T01.4 | Extrair remocao de boilerplate (rodapes OCR, assinaturas)                                             |      |
| T01.5 | Testes unitarios com laudos sinteticos (sem PHI): plain, RTF, HTML, com/sem negacao                   |      |
| T01.6 | SPEC + decisao: segmentacao T01.2 no caminho do `ClinicalNlpEngine` vs pre-processamento apenas no Composition Root (notebook) |      |


**Est. total:** 
**AC:** TextPipeline transforma RTF/HTML/plain em texto limpo (`to_plain`). Testes unitarios (T01.5) passando na CI. Negacao **no motor** e configuravel via YAML (`negation_phrases`, `negation_window`); paridade com as listas do legado (ex.: ~23 expressoes Grupo 1b) exige **migracao para YAML** e evidencia em amostra (`NLP_HML_LAUDOS_CSV` / `_local_samples`), nao valores fixos em codigo.

**Estado na lib `plataform/nlp_engine`:** T01.1-T01.5 e **T01.2 ancoras** (`text_pipeline/anchors.py`, paridade DII/colon: `organ_anchors`, `segment_by_organs`) com extra opcional `[spacy]`; extra `[dev]` inclui spaCy; CI instala `[dev]` em Python 3.12. **T01.6** fechada na implementacao: segmentacao por cabecalhos / fallback ancora no `ClinicalNlpEngine.process` (estrategia `headers` / `anchors` / `full_doc` via `nlp.segmentation`); SPEC de contrato: `doc-contrato-engine-rule-based-v0.md` (entrada, `nlp`, saida).

**Paridade / evidencia (Fase A):** `notas/s01-s02-paridade-evidencia-v0.md`, matriz e gaps `notas/paridade-legado-matriz-gaps-v0.md`.

---

### S02 -- Criar ClinicalNlpEngine (interface + rule-based)

> Implementar a interface do motor e a primeira implementacao (rule-based com spaCy Matcher + regex).


| Task  | Descricao                                                                                             | Est. |
| ----- | ----------------------------------------------------------------------------------------------------- | ---- |
| T02.1 | Definir interface `ClinicalNlpEngine` (metodo `process()`, input/output tipados)                      |      |
| T02.2 | Extrair `RuleBasedEngine` do Grupo 1b: spaCy Matcher + regex accent-tolerant + filtros de proximidade |      |
| T02.3 | Implementar `scoring.py`: score continuo 0.0-1.0 baseado em match count, negacao, proximidade         |      |
| T02.4a | Invariantes de saida em `nlp_engine` (`output_invariants.validate_engine_output_row`) + testes (`test_output_invariants.py`) |      |
| T02.4b | Schema/contrato completo e `quality_guard` na lib **`monitoring`** (sem import cruzado); dono **EngML / monitoring** — ver `notas/t02-4-quality-guard-monitoring-v0.md` |      |
| T02.5 | Testes unitarios do engine com configs de exemplo                                                     |      |


**Est. total:** 
**AC:** Engine recebe texto + dict `nlp` -> saida com `fl_relevante`, `confidence_score`, `exm_laudo_resultado` (JSON com `summary_compact`, contagens, `rule_engine_version`, `score_policy_version`). **T02.4a:** invariantes locais verificaveis por testes. **T02.4b:** validacao de schema/contrato na lib adequada apos decisao formal (nao duplicar no motor).

**Nota:** S01 e S02 sao construidas juntas (pipeline alimenta engine). Paralelismo possivel com S03 a partir de T02.1 (interface definida).

**Estado T02.4:** **T02.4a** implementada na lib (`output_invariants` + JSON minimo em `exm_laudo_resultado`). **T02.4b** parcial: **composition root** documentado (`doc-quality-guard-t024b-composition-v0.md`) — validacao pos-`process` com `validate_engine_output_row`; gate opcional em `audit_engine_from_csv.py --validate-output-invariants` e `validate_local_samples.py --validate-output-invariants`. **Schema completo / lib `monitoring`** pendente de wheel/repo e alinhamento EngML (sem bloquear MVP rule-based).

**Check legado Grupo 1b (2026-04-20):** baseline interno verde (`pytest tests -q --ignore=tests/local` = 86 passed). Gate completo por especialidade depende de reposicao de amostras em `_local_samples/` (status consolidado em `notas/s01-s02-paridade-evidencia-v0.md`).

---

### S03 -- Externalizar configuracao em YAML

> Mover CONFIG clinico de dentro dos notebooks para YAML versionado.


| Task  | Descricao                                                                                       | Est. |
| ----- | ----------------------------------------------------------------------------------------------- | ---- |
| T03.1 | Definir schema YAML (estrutura, campos obrigatorios, validacao)                                 |      |
| T03.2 | Implementar `config_loader.py`: load + validate + merge de defaults                             |      |
| T03.3 | Extrair CONFIG do notebook hepato atual para `configs/hepatologia.yaml`                         |      |
| T03.4 | Testes: YAML valido, YAML invalido (campo faltante, tipo errado), merge de defaults             |      |
| T03.5 | Criar `shared/organs.yaml` com universo de orgaos extraido do Grupo 1b                          |      |
| T03.6 | Implementar merge automatico no config_loader do `nlp_engine`: shared organs + specialty config |      |
| T03.7 | Definir schema YAML canonico para Fase 1 (sem campos de encoder/head)                           |      |


**Est. total:** 
**AC:** Config em duas camadas (shared + specialty) carregada pelo motor. `shared/organs.yaml` com universo de orgaos. Config de hepatologia com secoes por lib (nlp, data, monitoring). Alteracao de keyword = alteracao de YAML, nao de codigo.

**Estado piloto Pulmao (S03/S06b):** execucao em microetapas com SPEC e gate funcional "mesmo input -> resultado semelhante ou superior ao legado". Decisoes e vinculo backlog registados em `notas/s03-piloto-pulmao-runtime-v0.md`.

---

### S04 -- Padronizar metricas e logging

> Criar camada de observabilidade no motor.


| Task  | Descricao                                                                                            | Est. |
| ----- | ---------------------------------------------------------------------------------------------------- | ---- |
| T04.1 | Implementar registro de metadados: `specialty_id`, `config_version`, `engine_version`, `dt_execucao` |      |
| T04.2 | Implementar `confidence_score` continuo (0.0-1.0) no output                                          |      |
| T04.3 | Implementar calculo de metricas (precision/recall/F1) quando amostra gold fornecida                  |      |
| T04.4 | Definir schema + gravacao da tabela `ia.tb_diamond_mod_metricas_qualidade`                           |      |


**Est. total:** 
**AC:** Cada execucao do motor registra metadados e metricas. Tabela de metricas populada.

---

### S05 -- Notebook fino + integracao Databricks

> Criar notebook Databricks minimo que consome a lib.


| Task  | Descricao                                                                                              | Est. |
| ----- | ------------------------------------------------------------------------------------------------------ | ---- |
| T05.1 | Criar notebook template `ntb_ia_motor.py` (~50 linhas): widgets, load config, call engine, write Delta |      |
| T05.2 | Instanciar para hepatologia + testar end-to-end em dev                                                 |      |
| T05.3 | Documentar: como rodar, como alterar config, como adicionar especialidade                              |      |


**Est. total:** 
**AC:** Notebook roda em Databricks, consome lib, processa laudos hepato, grava Delta. PRD inalterado.

---

### S06 -- Validacao paralela com hepato

> Comparar output do motor novo vs pipeline atual.


| Task  | Descricao                                                                | Est. |
| ----- | ------------------------------------------------------------------------ | ---- |
| T06.1 | Selecionar amostra de laudos hepato ja processados pelo pipeline atual   |      |
| T06.2 | Processar mesma amostra com motor novo + comparar output por output      |      |
| T06.3 | Categorizar divergencias: melhoria recall, melhoria precision, regressao |      |
| T06.4 | Gerar relatorio de validacao com metricas                                |      |


**Est. total:** 
**AC:** Relatorio de concordancia motor vs baseline. Paridade ou melhoria documentada.

---

### S06b -- Validacao paralela Grupo 1b (piloto ex.: colon)

> Mesmo rigor do S06 para especialidades **Grupo 1b** (rule-based evoluido + embeddings no legado): evidencia de paridade ou melhoria **sem regressao**, com classificacao de divergencias.

| Task  | Descricao                                                                | Est. |
| ----- | ------------------------------------------------------------------------ | ---- |
| T06.5 | Selecionar amostra com baseline ou gold do pipeline 1b (ex.: colon)       |      |
| T06.6 | Processar com motor novo + YAML flatten + comparar `exm_laudo_resultado` / `fl_relevante` / `confidence_score` |      |
| T06.7 | Categorizar divergencias: melhoria recall, melhoria precision, regressao, gap de escopo (ex.: embeddings **S09**) |      |
| T06.8 | Registar metricas ou taxa de concordancia no mesmo modelo do relatorio S06 |      |


**Est. total:** 
**AC:** Artefacto equivalente ao S06 (relatorio ou Delta de auditoria). **Sem regressao nao documentada.** Melhorias de precision/recall com **sign-off clinico** (**S08**). Divergencias por falta de embeddings marcadas como dependencia **S09**, nao como falha silenciosa da Fase 1.

---

### S07 -- Inventariar dados para treinamento futuro

> Catalogar todos os dados disponiveis para Fase 3. Roda em paralelo.


| Task  | Descricao                                                                                  | Est. |
| ----- | ------------------------------------------------------------------------------------------ | ---- |
| T07.1 | Levantar volumes por tipo (nao-rotulados, outputs atuais, gold standard) por especialidade |      |
| T07.2 | Documentar inventario: volume, formato, localizacao, nivel de validacao                    |      |


**Est. total:** 
**AC:** Catalogo de dados por especialidade.

---

### S08 -- Governanca clinica minima

> Estabelecer fluxo de aprovacao de mudanca clinica. Roda em paralelo.


| Task  | Descricao                                                                         | Est. |
| ----- | --------------------------------------------------------------------------------- | ---- |
| T08.1 | Documentar fluxo (DS propoe PR YAML -> medico valida -> Git tag) + template de PR |      |


**Est. total:** 
**AC:** Fluxo documentado. Primeira mudanca de config hepato segue o fluxo.

---

### Resumo Fase 1

**Definition of Done — paridade legado (MVP rule-based):** (1) `to_plain` com taxa de match acordada em amostra de referencia **ou** plano WIP documentado (causas de divergencia classificadas); (2) motor com YAML da especialidade e **auditoria de saida** (ex.: `scripts/audit_engine_from_csv.py` em [`plataform/nlp_engine`](../../plataform/nlp_engine/)) revisada; (3) relatorio **S06** e/ou **S06b** com divergencias classificadas; (4) sem PHI versionado; (5) mudancas clinicas via **S08** quando aplicavel. Ver `notas/s01-s02-paridade-evidencia-v0.md` e `notas/paridade-legado-matriz-gaps-v0.md`.


| Historia            | Est.  | Paralelismo                                       |
| ------------------- | ----- | ------------------------------------------------- |
| S00 Setup infra     |       | Primeira (desbloqueia todas)                      |
| S01 TextPipeline    |       | Stream principal (com S02)                        |
| S02 Engine          |       | Stream principal (com S01)                        |
| S03 Config YAML     |       | Inicia apos T02.1                                 |
| S04 Metricas        |       | Paralelo a S03                                    |
| S05 Notebook        |       | Apos S01-S04                                      |
| S06 Validacao       |       | Apos S05                                          |
| S06b Validacao 1b   |       | Paralelo ou apos S06; mesma disciplina de evidencia |
| S07 Inventario      |       | Lateral (qualquer momento)                        |
| S08 Governanca      |       | Lateral (qualquer momento)                        |
| **Total efetivo**   | ****  |                                                   |
| **Com paralelismo** | ** ** | S01/S02 em paralelo com S03/S04; S07/S08 laterais |


---

## FASE 2 -- NLP avancado com embeddings (Sprint 3)

### DoReady / CA / DoD da Fase 2 (S09-S12)

**Definition of Ready (DoR) — obrigatorio antes de iniciar qualquer historia da Fase 2**

- Escopo e objetivo da historia aprovados no board (sem ambiguidades de paridade vs superioridade).
- Baseline Fase 1 congelado por especialidade (artefatos de `build/audit/compare` e metricas agregadas).
- Amostras de validacao disponiveis por especialidade (sem PHI versionada no repositorio).
- Responsavel clinico definido para sign-off de divergencias relevantes.
- Dependencias tecnicas explicitadas (runtime, libs, CI e decisoes EngML aplicaveis).

**Acceptance Criteria (CA) transversal — Fase 2**

- Todo incremento da Fase 2 e ativado por configuracao (feature flag), com fallback para comportamento Fase 1.
- Cada historia entrega evidencia mensuravel de impacto (delta de metricas vs baseline).
- Nenhuma regressao nao documentada: toda queda relevante de metrica exige justificativa e decisao.
- Pipeline de validacao obrigatorio em cada iteracao: `build -> audit -> compare`.

**Definition of Done (DoD) transversal — Fase 2**

- Codigo e testes automatizados (unit/integration) passando local e CI.
- Configuracao versionada e rastreavel (`config_version`/`engine_version` nos outputs).
- Evidencia publicada com metricas antes/depois e matriz (TP/FN/FP/TN quando aplicavel).
- Documentacao atualizada (escopo, riscos, limites e proximos passos).

### S09 -- Componente de embeddings (MiniLM)

> Extrair SentenceTransformer do Grupo 1b e encapsular como componente opcional.


| Task  | Descricao                                                          | Est. |
| ----- | ------------------------------------------------------------------ | ---- |
| T09.1 | Extrair logica de embeddings do Grupo 1b para `semantic_expand.py` |      |
| T09.2 | Integrar no engine via flag YAML (`use_embeddings: true`)          |      |
| T09.3 | Calibrar thresholds de similaridade por especialidade              |      |
| T09.4 | Testes: com/sem embeddings, thresholds, edge cases                 |      |


**Est. total:** 
**AC:** Embeddings ativados por config. Impacto mensuravel em recall/precision vs Fase 1.

**DoReady (S09):**

- Baseline sem embeddings congelado para as especialidades alvo.
- Modelo base e runtime de inferencia definidos (CPU) e dependencias aprovadas.
- Criterio de ganho minimo definido por especialidade (recall/precision/F1/concordancia).

**DoD (S09):**

- `semantic_expand` integrado com flag YAML (`use_embeddings`) e fallback sem embeddings preservado.
- Testes com/sem embeddings e thresholds (incluindo edge cases) passando.
- Relatorio de impacto vs baseline com decisao explicita (`aceite`, `ajuste`, `rollback`).

**Nota:** Paridade completa com legado Grupo 1b que dependa de `semantic_expand` / embeddings **nao** e critico de fecho da Fase 1 pura; gaps marcados **F2** em `notas/paridade-legado-matriz-gaps-v0.md` (ex.: Gap-002) fecham preferencialmente com esta historia.

---

### S10 -- Configs multi-especialidade

> Criar YAMLs para demais especialidades a partir do template hepato.


| Task  | Descricao                                                                               | Est. |
| ----- | --------------------------------------------------------------------------------------- | ---- |
| T10.1 | Criar YAMLs: biliar, neuroimunologia, reumatologia, colon (extrair do notebook de cada) |      |
| T10.2 | Validar motor com cada config: output vs pipeline atual da especialidade                |      |


**Est. total:** 
**AC:** Motor processa 5+ especialidades com config-only. Paridade por especialidade.

**DoReady (S10):**

- Lista priorizada de especialidades e mapeamento minimo por notebook legado.
- Template YAML estabilizado (campos obrigatorios e validacoes).

**DoD (S10):**

- YAMLs das especialidades previstas criados e validados por loader/engine.
- Auditoria e compare executados por especialidade com evidencia agregada.
- Divergencias relevantes classificadas (gap config, gap semantico, possivel incerteza legado).

---

### S11 -- CI gates

> Configurar lint + testes como gate no CI.


| Task  | Descricao                                                   | Est. |
| ----- | ----------------------------------------------------------- | ---- |
| T11.1 | Completar gaps de testes (fixtures multi-especialidade)     |      |
| T11.2 | Configurar CI: lint (ruff) + pytest como gate antes de sync |      |


**Est. total:** 
**AC:** CI roda lint + testes em cada push. Gate bloqueia sync se teste falha.

**DoReady (S11):**

- Suite minima de testes estabilizada (sem flaky conhecidos) e comandos padronizados.
- Politica de gate acordada (quais jobs bloqueiam merge/sync).

**DoD (S11):**

- Workflow CI ativo com lint + testes da Fase 2.
- Falha em gate impede merge/sync conforme politica definida.
- Documentacao de execucao e troubleshooting publicada.

---

### S12 -- Baseline de metricas por especialidade

> Calcular metricas de referencia para cada especialidade.


| Task  | Descricao                                                            | Est. |
| ----- | -------------------------------------------------------------------- | ---- |
| T12.1 | Rodar motor em amostra de cada especialidade ativa                   |      |
| T12.2 | Calcular metricas (precision, recall, F1, concordancia) + documentar |      |


**Est. total:** 
**AC:** Tabela de baseline com metricas por especialidade. Referencia para medir evolucao.

**DoReady (S12):**

- Amostras por especialidade disponiveis e contrato de metrica fechado.
- Formato unico de saida para consolidacao de metricas definido.

**DoD (S12):**

- Tabela baseline consolidada por especialidade (precision/recall/F1/concordancia e volume).
- Artefato de referencia versionado para comparacoes de iteracoes futuras.
- Lacunas e riscos destacados para priorizacao da fase seguinte.

---

### Resumo Fase 2


| Historia              | Est. | Paralelismo                          |
| --------------------- | ---- | ------------------------------------ |
| S09 Embeddings        |      | Paralelo a S10                       |
| S10 Multi-config      |      | Paralelo a S09                       |
| S11 CI gates          |      | Apos S09/S10                         |
| S12 Baseline metricas |      | Paralelo a S11                       |
| **Total efetivo**     | **** |                                      |
| **Com paralelismo**   | **** | S09+S10 paralelos; S11+S12 paralelos |


---

## FASE 3 -- Encoder compartilhado (Sprints 4-5) -- objetivo de evolucao

> Condicional a validacao das Fases 1-2 e disponibilidade de GPU.

### S13 -- Continued Pre-Training (CPT)


| Task  | Descricao                                                                  | Est. |
| ----- | -------------------------------------------------------------------------- | ---- |
| T13.1 | Preparar corpus de laudos nao-rotulados (limpeza via TextPipeline, dedup)  |      |
| T13.2 | Configurar treinamento MLM com BERTimbau + executar em GPU (~4-8h maquina) |      |
| T13.3 | Registrar encoder no MLflow + avaliar perplexity                           |      |


**Est. total:** 
**AC:** Encoder adaptado ao dominio clinico PT-BR. Registrado no MLflow.

---

### S14 -- Weak Supervision (pseudo-labels)


| Task  | Descricao                                                                              | Est. |
| ----- | -------------------------------------------------------------------------------------- | ---- |
| T14.1 | Definir labeling functions a partir dos outputs atuais (1 LF por engine/especialidade) |      |
| T14.2 | Treinar label model + gerar dataset pseudo-rotulado com faixas de confianca            |      |
| T14.3 | Filtrar por confianca: alta (>0.85) -> treino, intermediaria -> future active learning |      |


**Est. total:** 
**AC:** Dataset pseudo-rotulado com volume e confianca documentados.

---

### S15 -- Fine-Tuning specialty head (piloto hepato)


| Task  | Descricao                                                                              | Est. |
| ----- | -------------------------------------------------------------------------------------- | ---- |
| T15.1 | Preparar dados: pseudo-labels + gold standard hepato, stratified split                 |      |
| T15.2 | Implementar head hepato (Linear + Sigmoid) + treinar com gradual unfreezing, k-fold CV |      |
| T15.3 | Registrar head + metricas por fold no MLflow                                           |      |


**Est. total:** 
**AC:** Head hepato treinado. Metricas por fold no MLflow.

---

### S16 -- Validacao comparativa (encoder vs baseline)


| Task  | Descricao                                                           | Est. |
| ----- | ------------------------------------------------------------------- | ---- |
| T16.1 | Comparar encoder+head vs motor rule-based vs medico em amostra gold |      |
| T16.2 | Testes estatisticos: McNemar (p < 0.05), Bootstrap CI, F-beta(2)    |      |
| T16.3 | Gerar relatorio de concordancia e decisao go/no-go                  |      |


**Est. total:** 
**AC:** Relatorio de validacao tripla. Decisao documentada: encoder promove ou nao.

---

### Resumo Fase 3


| Historia             | Est. | Paralelismo                             |
| -------------------- | ---- | --------------------------------------- |
| S13 CPT              |      | Sequencial (prerequisito)               |
| S14 Weak Supervision |      | Paralelo a S13 (nao depende do encoder) |
| S15 Fine-Tuning      |      | Apos S13+S14                            |
| S16 Validacao        |      | Apos S15                                |
| **Total efetivo**    | **** |                                         |
| **Com paralelismo**  | **** | S13+S14 paralelos                       |


---

## FASE 4 -- Excelencia (Sprint 6+) -- continuo

### S17 -- Heads multi-especialidade

- Treinar heads para cada especialidade restante (mesma logica S15)
- Validacao por especialidade (mesma logica S16)

### S18 -- Monitoramento de drift

- Comparar distribuicao de scores entre janelas temporais (KS test)
- Alertas automaticos se drift detectado

### S19 -- LLM fallback seletivo

- Para casos inconclusivos (score 0.35-0.65)
- Anonimizacao obrigatoria, schema de resposta, trilha de auditoria

### S20 -- NER clinico

- Extracao estruturada de entidades anatomicas, procedimentos
- Validacao contra anotacao medica

---

## Resumo geral

```
Sprint 1 (atual)     Sprint 2 (Fase 1)    Sprint 3 (Fase 2)    Sprint 4-5 (Fase 3)    Sprint 6+ (Fase 4)
Estudo + proposta    MVP rule-based       + embeddings          Encoder (obj. evol.)   Excelencia

                     S01 TextPipeline     S09 Embeddings        S13 CPT                S17 Heads multi-esp
                     S02 Engine           S10 Multi-config      S14 Weak Supervision   S18 Drift monitoring
                     S03 Config YAML      S11 CI gates          S15 Fine-tuning        S19 LLM fallback
                     S04 Metricas         S12 Baseline          S16 Validacao          S20 NER clinico
                     S05 Notebook
                     S06 Validacao
                     S07 Inventario
                     S08 Governanca
```


| Fase                | Est. efetivo | Com paralelismo | Sprints          |
| ------------------- | ------------ | --------------- | ---------------- |
| 1 -- MVP            |              |                 | **1**            |
| 2 -- Embeddings     |              |                 | **1**            |
| 3 -- Encoder        |              |                 | **1-1.5**        |
| **Total Fases 1-3** | ****         | ****            | **~3.5 sprints** |


**Fases 1-2 (valor imediato, CPU only):**  com paralelismo (~2 sprints).