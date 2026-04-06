# Anexo -- Historias e Tasks de Desenvolvimento

**Vinculado a:** `07-relatorio-final-v0-plataforma-nlp-clinica.md`, `05-roadmap-entregas-sprint-v0.md`, `anexo-arquitetura-motor-nlp-v0.md`
**Sprints:** 10 dias uteis cada. Sprint 1 (atual) = estudo/proposta. Desenvolvimento comeca Sprint 2.

**Nota sobre estimativas:** os prazos consideram que estamos **extraindo** codigo existente do Grupo 1b (ja em producao), nao escrevendo do zero. Testes sao escritos inline com cada historia.

---

## Convencoes

- **S##** = historia (story)
- **T##.#** = task dentro da historia
- **AC** = criterio de aceite (acceptance criteria)
- Estimativas em dias (d) de trabalho efetivo

---

## FASE 1 -- MVP rule-based (Sprint 2)

### S01 -- Criar TextPipeline compartilhado

> Extrair e unificar a logica de preparacao de texto dos algoritmos existentes (Grupo 1b como base).

| Task | Descricao | Est. |
|------|-----------|------|
| T01.1 | Extrair `to_plain()` do Grupo 1b: deteccao RTF/HTML/plain, pypandoc fallback, ftfy, normalizacao NFKC |  |
| T01.2 | Extrair segmentacao por secao/orgao (headers, ancoras) |  |
| T01.3 | Extrair deteccao de negacao avancada (23 expressoes, janela 7 tokens, multi-token) |  |
| T01.4 | Extrair remocao de boilerplate (rodapes OCR, assinaturas) |  |
| T01.5 | Testes unitarios com laudos sinteticos (sem PHI): plain, RTF, HTML, com/sem negacao |  |

**Est. total:** 
**AC:** TextPipeline processa laudo RTF/HTML -> texto limpo com negacoes detectadas. Testes passando.

---

### S02 -- Criar ClinicalNlpEngine (interface + rule-based)

> Implementar a interface do motor e a primeira implementacao (rule-based com spaCy Matcher + regex).

| Task | Descricao | Est. |
|------|-----------|------|
| T02.1 | Definir interface `ClinicalNlpEngine` (metodo `process()`, input/output tipados) |  |
| T02.2 | Extrair `RuleBasedEngine` do Grupo 1b: spaCy Matcher + regex accent-tolerant + filtros de proximidade |  |
| T02.3 | Implementar `scoring.py`: score continuo 0.0-1.0 baseado em match count, negacao, proximidade |  |
| T02.4 | Implementar `quality_guard.py`: validacao do output contra schema esperado |  |
| T02.5 | Testes unitarios do engine com configs de exemplo |  |

**Est. total:** 
**AC:** Engine recebe texto preparado + config -> produz output com `fl_relevante`, `confidence_score`, `exm_laudo_resultado`. Schema validado.

**Nota:** S01 e S02 sao construidas juntas (pipeline alimenta engine). Paralelismo possivel com S03 a partir de T02.1 (interface definida).

---

### S03 -- Externalizar configuracao em YAML

> Mover CONFIG clinico de dentro dos notebooks para YAML versionado.

| Task | Descricao | Est. |
|------|-----------|------|
| T03.1 | Definir schema YAML (estrutura, campos obrigatorios, validacao) |  |
| T03.2 | Implementar `config_loader.py`: load + validate + merge de defaults |  |
| T03.3 | Extrair CONFIG do notebook hepato atual para `configs/hepatologia.yaml` |  |
| T03.4 | Testes: YAML valido, YAML invalido (campo faltante, tipo errado), merge de defaults |  |

**Est. total:** 
**AC:** Config de hepatologia em YAML. Motor carrega, valida e usa. Alteracao de keyword = alteracao de YAML, nao de codigo.

---

### S04 -- Padronizar metricas e logging

> Criar camada de observabilidade no motor.

| Task | Descricao | Est. |
|------|-----------|------|
| T04.1 | Implementar registro de metadados: `specialty_id`, `config_version`, `engine_version`, `dt_execucao` |  |
| T04.2 | Implementar `confidence_score` continuo (0.0-1.0) no output |  |
| T04.3 | Implementar calculo de metricas (precision/recall/F1) quando amostra gold fornecida |  |
| T04.4 | Definir schema + gravacao da tabela `ia.tb_diamond_mod_metricas_qualidade` |  |

**Est. total:** 
**AC:** Cada execucao do motor registra metadados e metricas. Tabela de metricas populada.

---

### S05 -- Notebook fino + integracao Databricks

> Criar notebook Databricks minimo que consome a lib.

| Task | Descricao | Est. |
|------|-----------|------|
| T05.1 | Criar notebook template `ntb_ia_motor.py` (~50 linhas): widgets, load config, call engine, write Delta |  |
| T05.2 | Instanciar para hepatologia + testar end-to-end em dev |  |
| T05.3 | Documentar: como rodar, como alterar config, como adicionar especialidade |  |

**Est. total:** 
**AC:** Notebook roda em Databricks, consome lib, processa laudos hepato, grava Delta. PRD inalterado.

---

### S06 -- Validacao paralela com hepato

> Comparar output do motor novo vs pipeline atual.

| Task | Descricao | Est. |
|------|-----------|------|
| T06.1 | Selecionar amostra de laudos hepato ja processados pelo pipeline atual |  |
| T06.2 | Processar mesma amostra com motor novo + comparar output por output |  |
| T06.3 | Categorizar divergencias: melhoria recall, melhoria precision, regressao |  |
| T06.4 | Gerar relatorio de validacao com metricas |  |

**Est. total:** 
**AC:** Relatorio de concordancia motor vs baseline. Paridade ou melhoria documentada.

---

### S07 -- Inventariar dados para treinamento futuro

> Catalogar todos os dados disponiveis para Fase 3. Roda em paralelo.

| Task | Descricao | Est. |
|------|-----------|------|
| T07.1 | Levantar volumes por tipo (nao-rotulados, outputs atuais, gold standard) por especialidade |  |
| T07.2 | Documentar inventario: volume, formato, localizacao, nivel de validacao |  |

**Est. total:** 
**AC:** Catalogo de dados por especialidade.

---

### S08 -- Governanca clinica minima

> Estabelecer fluxo de aprovacao de mudanca clinica. Roda em paralelo.

| Task | Descricao | Est. |
|------|-----------|------|
| T08.1 | Documentar fluxo (DS propoe PR YAML -> medico valida -> Git tag) + template de PR |  |

**Est. total:** 
**AC:** Fluxo documentado. Primeira mudanca de config hepato segue o fluxo.

---

### Resumo Fase 1

| Historia | Est. | Paralelismo |
|----------|------|-------------|
| S01 TextPipeline |  | Stream principal (com S02) |
| S02 Engine |  | Stream principal (com S01) |
| S03 Config YAML |  | Inicia apos T02.1 |
| S04 Metricas |  | Paralelo a S03 |
| S05 Notebook |  | Apos S01-S04 |
| S06 Validacao |  | Apos S05 |
| S07 Inventario |  | Lateral (qualquer momento) |
| S08 Governanca |  | Lateral (qualquer momento) |
| **Total efetivo** | **** | |
| **Com paralelismo** | ** ** | S01/S02 em paralelo com S03/S04; S07/S08 laterais |

---

## FASE 2 -- NLP avancado com embeddings (Sprint 3)

### S09 -- Componente de embeddings (MiniLM)

> Extrair SentenceTransformer do Grupo 1b e encapsular como componente opcional.

| Task | Descricao | Est. |
|------|-----------|------|
| T09.1 | Extrair logica de embeddings do Grupo 1b para `semantic_expand.py` |  |
| T09.2 | Integrar no engine via flag YAML (`use_embeddings: true`) |  |
| T09.3 | Calibrar thresholds de similaridade por especialidade |  |
| T09.4 | Testes: com/sem embeddings, thresholds, edge cases |  |

**Est. total:** 
**AC:** Embeddings ativados por config. Impacto mensuravel em recall/precision vs Fase 1.

---

### S10 -- Configs multi-especialidade

> Criar YAMLs para demais especialidades a partir do template hepato.

| Task | Descricao | Est. |
|------|-----------|------|
| T10.1 | Criar YAMLs: biliar, neuroimunologia, reumatologia, colon (extrair do notebook de cada) |  |
| T10.2 | Validar motor com cada config: output vs pipeline atual da especialidade |  |

**Est. total:** 
**AC:** Motor processa 5+ especialidades com config-only. Paridade por especialidade.

---

### S11 -- CI gates

> Configurar lint + testes como gate no CI.

| Task | Descricao | Est. |
|------|-----------|------|
| T11.1 | Completar gaps de testes (fixtures multi-especialidade) |  |
| T11.2 | Configurar CI: lint (ruff) + pytest como gate antes de sync |  |

**Est. total:** 
**AC:** CI roda lint + testes em cada push. Gate bloqueia sync se teste falha.

---

### S12 -- Baseline de metricas por especialidade

> Calcular metricas de referencia para cada especialidade.

| Task | Descricao | Est. |
|------|-----------|------|
| T12.1 | Rodar motor em amostra de cada especialidade ativa |  |
| T12.2 | Calcular metricas (precision, recall, F1, concordancia) + documentar |  |

**Est. total:** 
**AC:** Tabela de baseline com metricas por especialidade. Referencia para medir evolucao.

---

### Resumo Fase 2

| Historia | Est. | Paralelismo |
|----------|------|-------------|
| S09 Embeddings |  | Paralelo a S10 |
| S10 Multi-config |  | Paralelo a S09 |
| S11 CI gates |  | Apos S09/S10 |
| S12 Baseline metricas |  | Paralelo a S11 |
| **Total efetivo** | **** | |
| **Com paralelismo** | **** | S09+S10 paralelos; S11+S12 paralelos |

---

## FASE 3 -- Encoder compartilhado (Sprints 4-5) -- objetivo de evolucao

> Condicional a validacao das Fases 1-2 e disponibilidade de GPU.

### S13 -- Continued Pre-Training (CPT)

| Task | Descricao | Est. |
|------|-----------|------|
| T13.1 | Preparar corpus de laudos nao-rotulados (limpeza via TextPipeline, dedup) |  |
| T13.2 | Configurar treinamento MLM com BERTimbau + executar em GPU (~4-8h maquina) |  |
| T13.3 | Registrar encoder no MLflow + avaliar perplexity |  |

**Est. total:** 
**AC:** Encoder adaptado ao dominio clinico PT-BR. Registrado no MLflow.

---

### S14 -- Weak Supervision (pseudo-labels)

| Task | Descricao | Est. |
|------|-----------|------|
| T14.1 | Definir labeling functions a partir dos outputs atuais (1 LF por engine/especialidade) |  |
| T14.2 | Treinar label model + gerar dataset pseudo-rotulado com faixas de confianca |  |
| T14.3 | Filtrar por confianca: alta (>0.85) -> treino, intermediaria -> future active learning |  |

**Est. total:** 
**AC:** Dataset pseudo-rotulado com volume e confianca documentados.

---

### S15 -- Fine-Tuning specialty head (piloto hepato)

| Task | Descricao | Est. |
|------|-----------|------|
| T15.1 | Preparar dados: pseudo-labels + gold standard hepato, stratified split |  |
| T15.2 | Implementar head hepato (Linear + Sigmoid) + treinar com gradual unfreezing, k-fold CV |  |
| T15.3 | Registrar head + metricas por fold no MLflow |  |

**Est. total:** 
**AC:** Head hepato treinado. Metricas por fold no MLflow.

---

### S16 -- Validacao comparativa (encoder vs baseline)

| Task | Descricao | Est. |
|------|-----------|------|
| T16.1 | Comparar encoder+head vs motor rule-based vs medico em amostra gold |  |
| T16.2 | Testes estatisticos: McNemar (p < 0.05), Bootstrap CI, F-beta(2) |  |
| T16.3 | Gerar relatorio de concordancia e decisao go/no-go |  |

**Est. total:** 
**AC:** Relatorio de validacao tripla. Decisao documentada: encoder promove ou nao.

---

### Resumo Fase 3

| Historia | Est. | Paralelismo |
|----------|------|-------------|
| S13 CPT |  | Sequencial (prerequisito) |
| S14 Weak Supervision |  | Paralelo a S13 (nao depende do encoder) |
| S15 Fine-Tuning |  | Apos S13+S14 |
| S16 Validacao |  | Apos S15 |
| **Total efetivo** | **** | |
| **Com paralelismo** | **** | S13+S14 paralelos |

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

| Fase | Est. efetivo | Com paralelismo | Sprints |
|------|-------------|-----------------|---------|
| 1 -- MVP |  |  | **1** |
| 2 -- Embeddings |  |  | **1** |
| 3 -- Encoder |  |  | **1-1.5** |
| **Total Fases 1-3** | **** | **** | **~3.5 sprints** |

**Fases 1-2 (valor imediato, CPU only):**  com paralelismo (~2 sprints).
