# Briefing Tecnico -- Alinhamento com Engenharia de ML (v0.2)

**Data:** 2026-03-31
**Publico:** Engenheiro(s) de ML + Tech Lead DS
**Formato:** resumo executivo-tecnico para alinhar decisoes de plataforma, runtime, MLOps e roadmap

---

## 1. Contexto

A area de DS opera 9 pipelines NLP em producao (Databricks + Delta) para captacao de pacientes a partir de laudos clinicos. Cada pipeline foi desenvolvido de forma independente, com stacks e runtimes divergentes. O objetivo e **unificar em um motor NLP parametrizavel**, escalavel e refinavel, evoluindo em etapas sem big bang. Piloto definido: **Hepatologia**. Endometriose foi descontinuado. Biliar e neuroimunologia estao entrando em PRD nesta sprint.

**Posicionamento:** evolucao controlada com coexistencia. Pipelines atuais permanecem em PRD. Motor novo roda em paralelo para validacao. Saida operacional (Excel/SharePoint) inalterada.

---

## 2. Estado atual -- mapa tecnico

| Especialidade | Engine | Stack principal | Python | CI/CD | Status |
|---------------|--------|----------------|--------|-------|--------|
| Hepatologia | Rule-based (spaCy Matcher) | spaCy, NLTK, regex | 3.12 | rdmlops sync | ativo PRD |
| Biliar | Rule-based + embeddings + negacao | spaCy, SentenceTransformer, regex | 3.9 | rdmlops sync | entrando PRD |
| Neuroimunologia | Rule-based + embeddings + negacao | spaCy, SentenceTransformer, regex | 3.9 | rdmlops sync | entrando PRD |
| Reumatologia | Rule-based + embeddings + negacao | spaCy, SentenceTransformer, regex | 3.9 | rdmlops sync | ativo PRD |
| Colon | Embeddings + regras | spaCy, SentenceTransformer | 3.12 | rdmlops sync | ativo PRD |
| DII | Embeddings + regras (SVM legado arquivado) | spaCy, SentenceTransformer | 3.9 | rdmlops sync | ativo PRD |
| Pulmao | Embeddings + regras | spaCy, SentenceTransformer | 3.12 | rdmlops sync | ativo PRD |
| Birads | Pattern extraction (regex + NLTK) | NLTK, regex | 3.12 | rdmlops sync | ativo PRD |
| Rim | Business rules (calculo numerico) | PySpark, SQL | 3.12 | rdmlops sync | ativo PRD |

### Problemas operacionais que impactam EngML

- **Runtime divergente:** Python 3.9 em 4 pipelines vs 3.12 em 5 -- impede biblioteca unica sem alinhamento
- **Sem testes de regressao:** CI faz apenas sync de codigo para workspace; nao ha gate de qualidade
- **Duplicacao de codigo:** funcoes de limpeza textual, negacao e scoring replicadas em 7+ notebooks
- **pip install ad hoc:** cada notebook instala dependencias no inicio; risco de deriva entre execucoes
- **Sem padrao de imagem/init script:** dependencias nao estao fixadas em ambiente controlado

---

## 3. Objetivo de evolucao arquitetural (por fase)

### Fase 1-2: Motor rule-based + embeddings existentes (CPU)

```
Laudo (raw) --> [TextPipeline] --> [RuleBasedEngine] --> [PostProcess] --> Delta --> Excel/SharePoint
                     |                    |                   |
              Limpeza/Negacao     spaCy Matcher +       Regras/Score
              (compartilhado)    regex + config YAML   (QualityGuard)
```

**Sem encoder, sem GPU, sem treinamento.** Na Fase 2, embeddings existentes (MiniLM) entram como componente opcional de `semantic_expand`.

### Fase 3-4: Encoder compartilhado + specialty heads (GPU) -- objetivo de evolucao

```
Laudo (raw) --> [TextPipeline] --> [SharedEncoder] --> [SpecialtyHead] --> [PostProcess] --> Delta
                     |                   |                   |                  |
              Limpeza/Neg          BERTimbau           Linear+Sigmoid     Regras/Score
              (compartilhado)    (fine-tuned)       (por especialidade)   (QualityGuard)
```

So avanca para esta fase com evidencia de ganho mensuravel sobre o baseline rule-based (Fases 1-2).

### Stack definida

| Componente | Tecnologia | Fase | Justificativa |
|-----------|-----------|------|---------------|
| TextPipeline | spaCy (pt_core_news_lg) + regex + NFKC | **1** | Codigo mais robusto ja existe (Grupo 1b) |
| Embeddings (opcional) | SentenceTransformer (MiniLM, 118M params) | **2** | Ja em uso nos Grupos 1b/2; zero friction |
| Encoder | BERTimbau (bert-base-portuguese-cased, ~110M params) | **3** | Melhor representacao PT-BR para fine-tuning |
| Specialty Heads | PyTorch (Linear + Sigmoid/Softmax) | **3** | 8 heads leves, um por especialidade |
| Treinamento | Hugging Face Transformers + MLflow | **3** | Padrao de mercado; MLflow ja em uso parcial |
| Persistencia | Delta Lake (Unity Catalog) | **1** | Ja em uso; contratos I/O versionados |
| Orquestracao | SQL + Databricks Jobs (notebooks) | **1** | Manter padrao existente |
| Config | YAML por especialidade (Git) | **1** | Externalizar regras clinicas do codigo |
| Canal de saida | Excel/SharePoint (serving existente) | **1** | Canal oficial atual, inalterado |

### Contratos I/O

**Entrada (comum):** `id_exame`, `id_paciente`, `id_unidade`, `exm_laudo_texto`, `exm_mod`, `exm_tipo`, `dt_exame`

**Saida (comum):** `id_predicao`, `dt_execucao`, `specialty_id`, `config_version`, `engine_version`, `fl_relevante`, `confidence_score`, `exm_laudo_resultado` (JSON achados), `exm_laudo_texto_tratado`

---

## 4. Pipeline de treinamento (a partir da Fase 3)

**Nota:** treinamento so se aplica a partir da Fase 3. Fases 1-2 nao requerem GPU nem MLflow para treinamento.

```
Etapa 1          Etapa 2             Etapa 3              Etapa 4
CPT          --> Weak Supervision --> Fine-Tuning       --> Validacao
(sem rotulo)    (pseudo-labels)     (dados medicos)       (new vs baseline)
```

| Etapa | Input | Tecnica | Infra necessaria | Output |
|-------|-------|---------|-----------------|--------|
| 1. Continued Pre-Training | Laudos nao-rotulados | MLM (masked language modeling) | GPU single node (A10/V100), ~4-8h | Encoder adaptado ao dominio clinico PT-BR |
| 2. Weak Supervision | Outputs do sistema atual (fl_relevante + achados) | Labeling functions (Snorkel) | CPU | Dataset pseudo-rotulado com confianca |
| 3. Fine-Tuning | Lotes validados pelo corpo medico + pseudo-labels | Gradual unfreezing, k-fold CV, early stopping | GPU single node, ~2-4h por specialty | Heads treinados por especialidade |
| 4. Validacao | Amostra gold (medica) + baseline atual | McNemar, Bootstrap CI, F-beta(2) | CPU | Relatorio de concordancia + gate go/no-go |

### MLflow -- requisitos minimos (Fase 3)

- **Experiment naming:** `nlp-motor/{specialty_id}/{etapa}` (ex.: `nlp-motor/hepatologia/fine-tuning`)
- **Tracking:** loss, F-beta(2), recall, precision, AUC-ROC por epoch/fold
- **Model Registry:** encoder versionado + heads por especialidade
- **Artifacts:** config YAML usada, dataset split info, metricas finais, confusion matrix

---

## 5. Decisoes que dependem de alinhamento com EngML

| # | Decisao | Opcoes | Recomendacao DS | Fase | Impacto |
|---|---------|--------|-----------------|------|---------|
| 1 | **Runtime Python** | (A) Convergir para 3.12 unico, (B) Dual temporario 3.9+3.12 | A: convergir para 3.12 | **1** | Bloqueia criacao da lib comum se nao alinhado |
| 2 | **Repositorio da lib NLP** | (A) Monorepo com pasta de lib, (B) Repo dedicado com wheel no feed, (C) Pacote em PyPI interno | B: repo dedicado com wheel | **1** | Define como notebooks consomem a lib |
| 3 | **Cluster GPU para treinamento** | Tipo de VM, policy, scheduling | Single node A10 ou V100; job cluster on-demand | **3** (nao antes) | CPT e fine-tuning exigem GPU; Fases 1-2 nao |
| 4 | **CI/CD gates** | (A) Manter sync only, (B) Adicionar lint+testes, (C) Gate completo com testes NLP | B: lint + testes unitarios da lib como pre-req | **2** | Reduz risco de regressao sem travar velocity |
| 5 | **Padrao de imagem/init script** | (A) Init script com pip, (B) Container image customizada, (C) Cluster policy com libs | B ou C: imagem com deps fixadas | **2** | Elimina pip ad hoc e deriva |
| 6 | **MLflow -- padrao de uso** | Experiment naming, model registry, retention | Conforme secao 4 acima | **3** | Garante rastreabilidade de artefatos |
| 7 | **Unity Catalog -- naming** | Padrao de catalog/schema/table para motor unificado | `{env}.ia.tb_diamond_mod_{specialty}_{stage}` | **2** | Consistencia entre especialidades |
| 8 | **Canal de saida** | Excel/SharePoint permanece; Delta como fonte intermediaria | Confirmar com operacao | **1** | Nao bloqueia motor; serving layer inalterado |

---

## 6. Roadmap -- visao EngML

| Fase | Prazo | Entregaveis | Infra | Dependencia EngML |
|------|-------|-------------|-------|-------------------|
| **1 -- MVP rule-based** | 2-4 sem | Lib Python com TextPipeline + ClinicalNlpEngine (rule-based); configs YAML hepato; metricas; validacao paralela | **CPU** | Decisao runtime + repo |
| **2 -- NLP avancado** | 3-5 sem | Embeddings existentes (MiniLM) como componente opcional; configs biliar/neuroimuno; testes pytest; baseline metricas | **CPU** | CI com gates; wheel publicado |
| **3 -- Encoder** | 4-6 sem | BERTimbau fine-tuned para hepatologia; validacao tripla (encoder vs regras vs medico); metricas no MLflow | **GPU** | GPU cluster disponivel; MLflow padronizado |
| **4 -- Excelencia** | Continuo | Heads por especialidade; calibracao; monitoramento drift; LLM fallback seletivo | GPU + API LLM | Model Registry ativo |

---

## 7. Acoes imediatas (Fase 1)

| # | Acao | Responsavel | Prazo |
|---|------|-------------|-------|
| 1 | Definir runtime Python unico (decisao #1) | EngML + DS | Semana 1 |
| 2 | Definir repositorio e formato da lib (decisao #2) | EngML + DS | Semana 1 |
| 3 | Definir padrao CI/CD minimo (decisao #4) | EngML + DS | Semana 1 |
| 4 | Catalogar dados rotulados e nao-rotulados por especialidade | DS | Semana 1 |
| 5 | Confirmar canal de saida oficial com operacao (decisao #8) | DS + Operacao | Semana 1 |
| 6 | Definir metricas de sucesso com equipe clinica | DS + Clinico | Semana 1-2 |
| 7 | Alinhar padrao Unity Catalog naming (decisao #7) | EngML + DS | Semana 2 |

**Nota:** provisionar cluster GPU (#3), padronizar MLflow (#6) e definir padrao de imagem (#5) sao acoes da **Fase 2-3**, nao bloqueiam Fase 1.

---

## 8. Referencias (detalhamento completo)

- `visao-refinada-motor-nlp-unificado-v0.md` -- arquitetura, treinamento, validacao, configs
- `analise-profunda-engines-nlp-v0.md` -- analise tecnica dos 5 grupos de engine
- `discovery-estado-atual-nlp-v0.md` -- status operacional, lineage, governanca
- `relatorio-final-v0-plataforma-nlp-clinica.md` -- relatorio consolidado v0
- `documento-auxiliar-brainstorm-motor-ds-nlp-llm-ml-v0.md` -- brainstorm e estrategia

---

*Documento preparado para reuniao de alinhamento Fase 1.*
