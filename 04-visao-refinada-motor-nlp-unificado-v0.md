# Visao Refinada do Motor NLP Unificado — Estrategia Tecnico-Cientifica (v0)

**Versao:** v0
**Base:** consolidacao dos documentos de discovery, analise de engines, brainstorm e alinhamento com lideranca
**Data:** 2026-03-31
**Contexto operacional atualizado:** endometriose descontinuado; biliar e neuroimunologia entrando em PRD nesta sprint

---

## 1. Objetivo

Definir, com fundamentacao na literatura cientifica validada, a arquitetura e estrategia de treinamento de um **motor NLP unificado, parametrizavel e refinavel** que:

- evolua e complemente (nao substitua) os engines atuais de forma controlada,
- melhore o que ja e feito hoje (baseline),
- seja escalavel para novas especialidades sem reescrita,
- evolua em etapas controladas com validacao clinica e estatistica.

**Nota v0.2 -- faseamento e escopo deste documento:** este documento detalha o **objetivo de evolucao** do motor NLP (Fases 3-4), incluindo encoder compartilhado e specialty heads. As **Fases 1-2 constroem a base** (rule-based + embeddings existentes, CPU, sem GPU) e estao detalhadas no `05-roadmap-entregas-sprint-v0.md`. As Fases 3-4 evoluem sobre essa base, com **gate de validacao por fase** -- so avancam com evidencia de ganho mensuravel sobre o baseline anterior.

**Premissas operacionais:** canal de saida atual (Excel/SharePoint) inalterado; orquestracao SQL + Jobs Databricks mantida; pipelines atuais coexistem com motor novo ate validacao.

---

## 2. Objetivo de evolucao: Shared Encoder + Multi-Task Learning (Fase 3)

### 2.1 Fundamentacao cientifica

A abordagem com maior validacao na literatura para NLP clinico com dados limitados e **Transfer Learning com encoder compartilhado e heads especializados por tarefa**:

| Referencia | Contribuicao | Relevancia para o contexto |
|------------|-------------|---------------------------|
| Peng et al. 2019, "Transfer Learning in Biomedical NLP" | Demonstrou que transfer learning supera abordagens treinadas from scratch em 15+ tasks biomedicas | Justifica uso de encoder pre-treinado ao inves de treinar do zero |
| Lee et al. 2020, "BioBERT" | Pre-training em PubMed melhora NER, RE, QA biomedicos em 0.6-9.6 F1 | Valida continued pre-training em dominio especifico |
| Alsentzer et al. 2019, "Publicly Available Clinical BERT Embeddings" | Adaptacao de BERT para notas clinicas (MIMIC-III) | Referencia direta para adaptacao ao nosso dominio |
| Gururangan et al. 2020, "Don't Stop Pretraining" (ACL) | Domain-adaptive pre-training (DAPT) melhora 2-5 F1 points em media | Fundamenta Etapa 1 do pipeline de treinamento |
| Howard & Ruder 2018, "ULMFiT" | Gradual unfreezing + discriminative fine-tuning para dados limitados | Tecnica de fine-tuning para nossos lotes pequenos |
| Caruana 1997, "Multitask Learning" | Compartilhar representacao entre tarefas relacionadas melhora generalizacao | Justifica arquitetura multi-task vs modelo monolitico |
| Ruder 2017, "An Overview of Multi-Task Learning" | Hard parameter sharing (encoder comum) e abordagem mais robusta para dados limitados | Valida a decisao de encoder compartilhado |
| Ratner et al. 2017/2020, "Snorkel" | Weak supervision com labeling functions gera training data em escala sem anotacao manual | Fundamenta uso dos outputs atuais como pseudo-labels |
| Savova et al. 2010, "cTAKES" | Sistemas rule-based clinicos sao frageis a variabilidade linguistica | Justifica evolucao de regras puras para encoder |

### 2.2 Por que esta arquitetura (e nao alternativas)

**Por que nao um unico modelo end-to-end monolitico:**

As tarefas clinicas sao heterogeneas — deteccao binaria de relevancia (hepato), extracao de categoria padronizada (BI-RADS), classificacao multi-label de achados (biliar). A literatura mostra que **compartilhar representacao e especializar por tarefa** (multi-task learning com hard parameter sharing) e superior a forcar um classificador unico para todas as tarefas (Caruana 1997; Ruder 2017).

**Limitacoes conhecidas de abordagens puramente rule-based:**

Regras sao deterministicas, auditaveis e constituem a **base operacional** do motor NLP. Porem, sao frageis a variabilidade linguistica (sinonimos, erros de digitacao, variantes regionais). O encoder e o caminho de evolucao para enderecar essas limitacoes — nao e substituicao. Regras permanecem como **pos-processamento** e **fallback** em todas as fases.

**Por que nao LLM-first (GPT-4o, etc.):**

LLMs generativos tem custo por token, latencia, e risco de alucinacao incompativeis com batch clinico diario de alto volume. A literatura recomenda LLM como **fallback seletivo** para casos inconclusivos, nao como motor primario (Nori et al. 2023 "Capabilities of GPT-4 on Medical Challenge Problems").

### 2.3 Diagramas da arquitetura

**Fase 1-2: Motor rule-based + embeddings existentes (CPU, sem GPU)**

```
[Laudo clinico raw]
        |
        v
+---------------------------------------+
|         TextPipeline (compartilhado)   |
| - Limpeza HTML/RTF/OCR                |
| - Normalizacao linguistica (NFKC)     |
| - Segmentacao por secao/orgao          |
| - Deteccao de negacao (23 expressoes)  |
+---------------------------------------+
        |
        v
+---------------------------------------+
|   RuleBasedEngine (config-driven)      |
| - spaCy Matcher + regex               |
| - Config YAML por especialidade        |
| - Embeddings MiniLM (Fase 2, opcional) |
+---------------------------------------+
        |
        v
+---------------------------------------+
|   Pos-processamento                    |
| - Scoring de priorizacao               |
| - Validacao de output (QualityGuard)   |
+---------------------------------------+
        |
        v
[Delta] --> [Serving layer existente] --> [Excel/SharePoint/API]
```

**Fase 3-4: Encoder compartilhado + specialty heads (GPU)**

```
[Laudo clinico raw]
        |
        v
+---------------------------------------+
|         TextPipeline (compartilhado)   |
| - Limpeza HTML/RTF/OCR                |
| - Normalizacao linguistica (NFKC,     |
|   acentos, boilerplate)               |
| - Segmentacao por secao/orgao          |
| - Deteccao de negacao (23 expressoes,  |
|   janela 7 tokens)                     |
+---------------------------------------+
        |
        v
+---------------------------------------+
|   Shared Encoder (fine-tuned)          |
| - Transformer multilingual clinico     |
| - Base: ~110M params (modelo base)     |
| - Continued pre-training em laudos     |
|   clinicos PT-BR                       |
+---------------------------------------+
        |
        v
+---------------------------------------+
|   Task Router (config-driven)          |
| - specialty_config.yaml                |
| - Seleciona head + pos-processamento   |
+---------------------------------------+
        |
    +---+---+---+---+---+---+---+---+
    v   v   v   v   v   v   v   v
  [H1] [H2] [H3] [H4] [H5] [H6] [H7] [H8]
  Hep  Col  Bil  Neu  Reu  Pul  DII  Bir

  Specialty Heads:
  - Linear + sigmoid/softmax por tarefa
  - Treinados com dados validados + pseudo-labels
        |
        v
+---------------------------------------+
|   Pos-processamento                    |
| - Regras deterministicas residuais     |
|   (ex.: BI-RADS categoria via regex)   |
| - Scoring de priorizacao               |
| - Validacao de output (QualityGuard)   |
+---------------------------------------+
        |
        v
[Schema unificado versionado -> Delta]
```

### 2.4 Detalhe por camada

**TextPipeline (compartilhado)**

Unifica toda limpeza/normalizacao que hoje esta duplicada em 7+ notebooks. A implementacao de referencia e a do Grupo 1b (biliar/neuroimuno/reumato), que possui:

- `to_plain()`: deteccao RTF/HTML/plain com pypandoc fallback, ftfy, normalizacao NFKC
- `_force_sentence_breaks` + sentencizer spaCy
- Segmentacao por headers/ancoras de orgao com expansao semantica
- Negacao avancada: 23 expressoes, janela de 7 tokens (vs 2 expressoes/3 tokens do hepato atual)
- Desambiguacao por orgao mais proximo (margem 25 chars)

**Shared Encoder**

Modelo pre-treinado multilingual, com continued pre-training em laudos clinicos PT-BR. Candidatos por ordem de recomendacao:

| Modelo | Params | Vantagem | Limitacao |
|--------|--------|----------|-----------|
| `neuralmind/bert-base-portuguese-cased` (BERTimbau) | 110M | Pre-treinado em PT-BR (brWaC), melhor representacao linguistica | Sem dominio clinico nativo |
| `xlm-roberta-base` | 125M | Multilingue robusto, boa baseline para fine-tuning | Maior que BERTimbau sem ganho claro para PT |
| `paraphrase-multilingual-MiniLM-L12-v2` (ja em uso) | 118M | Ja integrado nos pipelines atuais, zero friction de adocao | Otimizado para sentence similarity, nao classificacao |

**Recomendacao:** `BERTimbau` como encoder base, com continued pre-training (Etapa 1) em corpus de laudos clinicos nao-rotulados. Motivo: melhor representacao linguistica PT-BR como ponto de partida, e a continued pre-training adapta ao vocabulario clinico.

**Specialty Heads**

Camada linear + ativacao por especialidade:

| Especialidade | Tipo de tarefa | Ativacao | Labels |
|---------------|---------------|----------|--------|
| Hepatologia | Binary relevance | Sigmoid | relevante / nao_relevante |
| Colon | Multi-label (relevancia + achados) | Sigmoid por label | relevante + lista de achados |
| Biliar | Multi-label (relevancia + achados) | Sigmoid por label | relevante + achados biliares |
| Neuroimunologia | Multi-label (relevancia + achados) | Sigmoid por label | relevante + achados desmielinizantes |
| Reumatologia | Multi-label (relevancia + achados) | Sigmoid por label | relevante + achados reumatologicos |
| Pulmao | Multi-label (relevancia + achados) | Sigmoid por label | relevante + achados pulmonares |
| DII | Multi-label (relevancia + achados) | Sigmoid por label | relevante + achados inflamatorios |
| Birads | Category extraction | Softmax (0-6) | categoria BI-RADS (0-6) |

**Pos-processamento**

- Regras deterministicas residuais: para BI-RADS (regex de extracao de categoria permanece como validador), para rim (calculo numerico permanece inalterado)
- Scoring de priorizacao: combinacao de confianca do head + pesos configurados por especialidade
- QualityGuard: validacao estrutural do output contra schema versionado

### 2.5 Proporcionalidade tecnica

O principio e **nao usar canhao para matar formigas**:

| Decisao | Escolha | Justificativa |
|---------|---------|---------------|
| Tamanho do encoder | **base** (~110M params) | Suficiente para classificacao binaria/multi-label em textos curtos (laudos) |
| Abordagem de treino | Fine-tuning (nao pre-training from scratch) | 100x mais barato e eficiente com dados limitados |
| Infra | Single GPU node (A10/V100) no Databricks | Batch diario nao exige multi-GPU |
| Serving | Inferencia em lote (batch) | Nao ha requisito de real-time; lote diario e suficiente |
| LLM | Fallback seletivo (nao motor primario) | Custo/latencia incompativel com volume; so para ambiguos |
| Complexidade | Adicionada por fase | Cada camada so entra quando metricas justificarem |

---

## 3. Pipeline de treinamento (Fase 3 -- quando base rule-based estiver validada)

### 3.1 Inventario de dados disponiveis

| Tipo | Volume estimado | Uso |
|------|----------------|-----|
| Laudos nao-rotulados (corpus clinico) | Potencialmente grande (todas as tabelas de entrada) | Continued pre-training (Etapa 1) |
| Outputs do sistema atual (`fl_relevante` + achados) | Todo laudo ja processado | Weak supervision / pseudo-labels (Etapa 2) |
| Lotes validados pelo corpo medico | Centenas (amostras parciais) | Fine-tuning supervisionado (Etapa 3) |
| Baseline de metricas operacionais | Historico de monitoramento | Validacao comparativa (Etapa 4) |

**Acao requerida na Fase 0:** catalogar e quantificar cada tipo de dado por especialidade.

### 3.2 Etapa 1 — Continued Pre-Training (nao supervisionado)

**Objetivo:** adaptar o encoder ao vocabulario e sintaxe de laudos clinicos em PT-BR.

**Tecnica:** Masked Language Modeling (MLM) — mascarar 15% dos tokens de laudos e treinar o encoder a preve-los.

**Base cientifica:** Gururangan et al. 2020 "Don't Stop Pretraining" (ACL Best Paper Honorable Mention). Demonstrou que domain-adaptive pre-training (DAPT) melhora performance em 2-5 F1 points em tarefas downstream, mesmo com corpus de dominio pequeno (~25K docs ja e suficiente).

**Execucao pratica:**

- Input: laudos clinicos nao-rotulados (limpos via TextPipeline)
- Modelo base: BERTimbau (`neuralmind/bert-base-portuguese-cased`)
- Treinamento: MLM, ~3-5 epochs, learning rate 5e-5, batch size 16-32
- Infra: single GPU (A10/V100) no Databricks, ~4-8h de treinamento
- Tracking: MLflow (loss, perplexity por epoch)
- Output: `bert-clinical-ptbr-v1` (encoder adaptado ao dominio)

**Requisitos:**

- Acesso a volume de laudos (nao precisa de rotulo)
- LGPD: laudos processados apenas dentro do ambiente controlado Databricks
- Nao exportar texto de laudo para servicos externos

### 3.3 Etapa 2 — Weak Supervision (pseudo-labels)

**Objetivo:** gerar training data em escala sem custo de anotacao manual, usando os outputs do sistema atual como labeling functions.

**Base cientifica:** Ratner et al. 2017 "Data Programming" (NIPS) e Ratner et al. 2020 "Snorkel" (VLDB). Validado em dominio medico com ganhos de 5-15% vs regras puras. A ideia central: cada engine atual (regras, embeddings, regex) produz labels imperfeitas, e um **label model** estatistico estima a acuracia de cada fonte e agrega em labels mais confiaveis.

**Execucao pratica:**

1. **Definir labeling functions** (LFs): cada engine atual vira uma LF
   - LF_hepato_rules: output de `fl_relevante` do engine rule-based atual
   - LF_colon_embeddings: output do engine embeddings+regras
   - LF_birads_regex: output da extracao regex
   - LFs adicionais: heuristicas simples (ex.: laudo menciona termo critico? comprimento do laudo?)

2. **Treinar label model**: modelo estatistico que estima acuracia e correlacao entre LFs
   - Tecnica: modelo generativo de Snorkel (sem dados rotulados)
   - Output: probabilidade de label por exemplo, com estimativa de confianca

3. **Gerar dataset pseudo-rotulado**:
   - Filtrar exemplos com alta confianca (>0.85) para training set
   - Exemplos com confianca intermediaria (0.5-0.85) para active learning posterior
   - Exemplos com baixa confianca (<0.5) para revisao manual prioritaria

**Vantagem:** escala sem custo de anotacao — transforma o trabalho ja feito pelos engines atuais em dados de treinamento.

### 3.4 Etapa 3 — Fine-Tuning supervisionado (dados validados)

**Objetivo:** treinar specialty heads com os lotes validados pelo corpo medico, usando tecnicas comprovadas para datasets pequenos.

**Base cientifica:**

- Howard & Ruder 2018 "ULMFiT": gradual unfreezing e discriminative fine-tuning evitam overfitting em dados pequenos
- Sun et al. 2019 "How to Fine-Tune BERT for Text Classification": stratified k-fold e warmup para datasets clinicos
- Dodge et al. 2020 "Fine-Tuning Pretrained Language Models: Weight Initializations, Data Orders, and Early Stopping": early stopping como regularizacao

**Execucao pratica:**

1. **Preparacao dos dados:**
   - Unir pseudo-labels de alta confianca (Etapa 2) com lotes validados pelo corpo medico
   - Stratified split: manter proporcao de classes por fold
   - Data augmentation linguistica: sinonimos clinicos (ex.: "figado" <-> "hepatico"), reordenacao de sentencas

2. **Treinamento por specialty head:**
   - Encoder congelado nas primeiras 2-3 epochs (treina so o head)
   - Gradual unfreezing: liberar camadas do encoder progressivamente
   - Discriminative learning rates: camadas mais profundas com lr menor
   - Early stopping: monitorar validation loss com patience=3
   - Stratified k-fold cross-validation (k=5 para centenas de exemplos, k=10 se volume muito pequeno)

3. **Hiperparametros base:**
   - Learning rate: 2e-5 a 5e-5 (head), 1e-6 a 5e-6 (encoder layers)
   - Batch size: 16
   - Max epochs: 20 (com early stopping)
   - Dropout: 0.1-0.3
   - Weight decay: 0.01
   - Warmup: 10% dos steps

4. **Correcao do Grupo 4 atual (DII SVM legado):**
   - O treino anterior usava dataset inteiro como train=test (overfitting)
   - Nova abordagem: holdout real + cross-validation + metricas confiaveis

**Output:** specialty heads treinados, versionados no MLflow com metricas de cada fold.

### 3.5 Etapa 4 — Validacao comparativa (novo vs baseline)

**Objetivo:** estudo de concordancia sistematico para decidir se o novo motor promove ou nao.

**Base cientifica:**

- Dietterich 1998 "Approximate Statistical Tests for Comparing Supervised Classification Learning Algorithms": McNemar's test para comparacao pareada
- DeLong et al. 1988: comparacao de curvas ROC
- Efron & Tibshirani 1993: bootstrap confidence intervals

**Protocolo de validacao:**

1. **Tripla comparacao:**
   - `modelo_novo` (encoder + head)
   - `pipeline_atual` (engine rule-based/embeddings atual)
   - `validacao_medica` (lotes com rotulo do corpo medico = gold standard)

2. **Metricas primarias:**

   | Metrica | Formula | Prioridade | Justificativa |
   |---------|---------|-----------|---------------|
   | Recall | TP / (TP + FN) | **Critica** | Em captacao clinica, perder caso relevante e mais grave que falso positivo |
   | Precision | TP / (TP + FP) | Alta | Falso positivo gera custo operacional (navegacao desnecessaria) |
   | F-beta (beta=2) | (1+4) * P*R / (4P + R) | **Principal** | Pondera recall 2x mais que precision — adequado para triagem clinica |
   | AUC-ROC | Area sob curva ROC | Alta | Avalia performance em todos os thresholds |
   | Specificity | TN / (TN + FP) | Media | Complementa recall para visao completa |

3. **Testes estatisticos:**
   - **McNemar's test**: comparacao pareada entre modelo_novo e pipeline_atual (p < 0.05)
   - **Bootstrap CI (95%)**: intervalo de confianca para cada metrica via 1000+ amostras bootstrap
   - **DeLong test**: comparacao de AUC entre modelos

4. **Analise de discordancia:**
   - Categorizar casos onde `modelo_novo` e `pipeline_atual` divergem
   - Priorizar discordancias para revisao pelo corpo medico
   - Classificar em: (a) novo correto e velho errado, (b) velho correto e novo errado, (c) ambos errados, (d) ambiguo

5. **Gate de decisao:**
   - So promove encoder se: F-beta(2) >= baseline com p < 0.05
   - Se nao superar, manter engine deterministico e iterar no treinamento
   - Nenhuma evolucao sem paridade em amostra gold

---

## 4. Especificacao de configuracao por especialidade

### 4.1 Formato

Cada especialidade e definida por um arquivo `specialty_config.yaml` versionado no repositorio (Git como fonte da verdade).

### 4.2 Schema da configuracao

```yaml
# Schema: specialty_config_v1
specialty_id: string          # identificador unico
version: string               # semver (ex.: 1.0.0)
status: string                # ativo | descontinuado | piloto

engine:
  type: string                # shared_encoder | rule_based_fallback | pattern_extraction | business_rules
  encoder_model: string       # referencia ao modelo registrado no MLflow
  head_checkpoint: string     # referencia ao head treinado no MLflow

head_config:
  task_type: string           # binary_relevance | multi_label | category_extraction
  output_labels: list         # labels de saida
  threshold: float            # threshold de decisao (default: 0.5)
  confidence_levels:
    high: float               # >= este valor = alta confianca
    low: float                # <= este valor = baixa confianca / fallback

preprocessing:
  segmentation: string        # by_organ | by_header | full_doc
  target_organs: list         # orgaos-alvo para segmentacao
  negation_window: int        # tamanho da janela de negacao (tokens)
  negation_expressions: list  # lista de expressoes de negacao (herda default + custom)
  force_full_doc: bool        # processar doc inteiro como 1 bloco

clinical_taxonomy:
  keywords: list              # palavras-chave por dominio
  findings: dict              # achados clinicos agrupados por categoria
  exclusions: list            # termos de exclusao

postprocessing:
  deterministic_rules: bool   # aplicar regras deterministicas apos encoder
  scoring_weights:
    urgencia: float
    complexidade: float
  custom_rules: list          # regras especificas do dominio (opcionais)

metadata:
  owner_clinical: string      # dono clinico responsavel
  owner_technical: string     # dono tecnico
  last_review: date           # ultima revisao clinica
  review_cadence: string      # mensal | trimestral | semestral
```

### 4.3 Exemplos por especialidade

**Hepatologia** (piloto — Grupo 1 migrando para encoder):

```yaml
specialty_id: hepatologia
version: "1.0.0"
status: piloto

engine:
  type: shared_encoder
  encoder_model: "mlflow:/clinical-bert-ptbr/1"
  head_checkpoint: "mlflow:/head-hepato/1"

head_config:
  task_type: binary_relevance
  output_labels: [relevante, nao_relevante]
  threshold: 0.5
  confidence_levels:
    high: 0.85
    low: 0.35

preprocessing:
  segmentation: by_organ
  target_organs: [figado, vias_biliares, vesicula_biliar]
  negation_window: 7
  negation_expressions: []  # herda default completo (23 expressoes)
  force_full_doc: false

clinical_taxonomy:
  keywords: [figado, hepat, vias biliares, vesicula biliar]
  findings:
    cirrose: [cirrose, fibrose, hepatopatia cronica]
    hipertensao_portal: [hipertensao portal, trombose veia porta, varizes esofagianas]
    neoplasia: [lirads 4, lirads 5, nodulo hipervascular, hepatocarcinoma]
    inflamatorio: [hepatite, microlitiase, colangite]
  exclusions: []

postprocessing:
  deterministic_rules: false
  scoring_weights:
    urgencia: 0.6
    complexidade: 0.4

metadata:
  owner_clinical: "a definir (fase 0)"
  owner_technical: "equipe DS"
  last_review: "2026-03-31"
  review_cadence: trimestral
```

**Biliar** (Grupo 1b — entrando em PRD):

```yaml
specialty_id: biliar
version: "1.0.0"
status: ativo

engine:
  type: rule_based_fallback  # inicia com engine atual, migra para shared_encoder na Fase 3
  encoder_model: null
  head_checkpoint: null

head_config:
  task_type: multi_label
  output_labels: [relevante, colecistite, coledocolitiase, pancreatite_biliar, neoplasia_biliar]
  threshold: 0.5
  confidence_levels:
    high: 0.90
    low: 0.35

preprocessing:
  segmentation: by_header
  target_organs: [doencas_biliares]
  negation_window: 7
  negation_expressions: []
  force_full_doc: false

clinical_taxonomy:
  keywords: [vesicula, biliar, coldoco, pancreas]
  findings:
    colelitiase: [calculo, sinal WES, colelitiase]
    inflamatorio: [colecistite cronica, empiema, Murphy US, hidropsia]
    obstrutivo: [coledocolitiase, pancreatite biliar, ileo biliar]
    neoplasico: [polipo sessil, massa sugestiva, carcinoma vesicula]
  exclusions: []

postprocessing:
  deterministic_rules: true
  scoring_weights:
    urgencia: 0.7
    complexidade: 0.3

metadata:
  owner_clinical: "a definir (fase 0)"
  owner_technical: "equipe DS"
  last_review: "2026-03-31"
  review_cadence: trimestral
```

**Birads** (Grupo 3 — pattern extraction, hibrido futuro):

```yaml
specialty_id: birads
version: "1.0.0"
status: ativo

engine:
  type: pattern_extraction  # regex para extracao de categoria; encoder como complemento futuro
  encoder_model: null
  head_checkpoint: null

head_config:
  task_type: category_extraction
  output_labels: ["0", "1", "2", "3", "4", "5", "6"]
  threshold: null  # nao aplicavel (extracao por regex)
  confidence_levels:
    high: 1.0
    low: 0.0

preprocessing:
  segmentation: full_doc
  target_organs: [mama]
  negation_window: 7  # a implementar (hoje nao tem negacao)
  negation_expressions: []
  force_full_doc: true

clinical_taxonomy:
  keywords: [birads, bi_rads, bi-rads, categoria]
  findings:
    categorias: ["0", "1", "2", "3", "4", "4a", "4b", "4c", "5", "6"]
  exclusions: []

postprocessing:
  deterministic_rules: true  # regex permanece como regra primaria
  scoring_weights:
    urgencia: 1.0
    complexidade: 0.0
  custom_rules:
    - name: max_category
      description: "Em caso de multiplas categorias, prevalece a mais alta"

metadata:
  owner_clinical: "a definir (fase 0)"
  owner_technical: "equipe DS"
  last_review: "2026-03-31"
  review_cadence: trimestral
```

**Rim** (Grupo 5 — business rules, fora do escopo do encoder):

```yaml
specialty_id: rim
version: "1.0.0"
status: ativo

engine:
  type: business_rules  # calculo numerico, sem NLP textual
  encoder_model: null
  head_checkpoint: null

head_config:
  task_type: numeric_calculation
  output_labels: [clearance_value, faixa_risco]
  threshold: null

preprocessing:
  segmentation: null  # nao aplicavel
  target_organs: [rim]
  negation_window: null
  force_full_doc: false

clinical_taxonomy:
  keywords: []  # sem NLP textual
  findings: {}
  exclusions: []

postprocessing:
  deterministic_rules: true
  scoring_weights:
    urgencia: 1.0
    complexidade: 0.0
  custom_rules:
    - name: clearance_filter
      description: "0.1 <= clearance <= 60"

metadata:
  owner_clinical: "a definir (fase 0)"
  owner_technical: "equipe DS"
  last_review: "2026-03-31"
  review_cadence: trimestral
```

---

## 5. Matriz de convergencia por especialidade (atualizada)

### 5.1 Status e roadmap

| Especialidade | Status atual | Engine atual | Fase 1 (MVP rule-based, CPU) | Fase 2 (NLP avancado, CPU) | Fase 3 (encoder, GPU) | Fase 4 (excelencia) |
|---------------|-------------|-------------|------------------------------|----------------------------|-----------------------|---------------------|
| Hepatologia | ativo PRD | Grupo 1 (Matcher puro) | **Estender c/ negacao + CONFIG YAML** | + embeddings (MiniLM) | **Piloto encoder** (head hepato) | + active learning + NER |
| Biliar | **entrando PRD** | Grupo 1b | Unificar CONFIG | Manter embeddings | Head encoder | + NER + scoring |
| Neuroimunologia | **entrando PRD** | Grupo 1b | Unificar CONFIG | Manter embeddings | Head encoder | + LLM fallback + NER |
| Reumatologia | ativo PRD | Grupo 1b | Unificar CONFIG | Manter embeddings | Head encoder | + NER + scoring |
| Colon | ativo PRD | Grupo 2 | Unificar CONFIG | Manter embeddings | Head encoder | + scoring + calibracao |
| DII | ativo PRD | Grupo 2 (SVM legado arquivado) | Unificar CONFIG, arquivar SVM | Manter embeddings | Head encoder | + scoring + calibracao |
| Pulmao | ativo PRD | Grupo 2 | Unificar CONFIG | Manter embeddings | Head encoder | + scoring + calibracao |
| Birads | ativo PRD | Grupo 3 (regex) | **+ negacao basica** | Manter regex | Hibrido (encoder + regex) | + pattern engine robusto |
| Rim | ativo PRD | Grupo 5 (business rules) | Manter (padronizar config) | N/A (sem NLP) | N/A (sem NLP) | Manter |
| ~~Endometriose~~ | **descontinuado** | --- | --- | --- | --- | --- |

### 5.2 Fases detalhadas

**Fase 1: MVP rule-based (2-4 semanas) -- CPU**

- Extrair TextPipeline compartilhado do Grupo 1b
- Implementar interface ClinicalNlpEngine com engine rule-based como default
- Externalizar configs por especialidade em YAML (conforme schema da secao 4)
- Estender Hepatologia com negacao avancada (23 expressoes, janela 7 tokens) e limpeza RTF/HTML
- Adicionar negacao basica ao Birads
- Implementar metricas de qualidade (precision, recall, F1 quando gold disponivel)
- Validacao paralela com hepato (sem alterar PRD)
- Catalogar dados rotulados disponiveis por especialidade
- Definir metricas de sucesso com equipe clinica
- Governanca clinica pragmatica: DS propoe (PR no YAML) -> medico valida -> deploy versionado (Git tag)
- **Entregavel:** motor rule-based que replica ou melhora o baseline atual

**Fase 2: NLP avancado com embeddings existentes (3-5 semanas) -- CPU**

- Encapsular SentenceTransformer (MiniLM, ja em uso nos Grupos 1b/2) como componente opcional
- Configs YAML para biliar e neuroimunologia
- Testes automatizados (pytest) da lib
- Baseline de metricas por especialidade
- Calibracao de thresholds de similaridade
- **Entregavel:** motor com embeddings opcionais e paridade confirmada

**Fase 3: Encoder compartilhado + fine-tuning (4-6 semanas) -- GPU**

- Etapa 1: Continued pre-training do encoder em laudos clinicos PT-BR
- Etapa 2: Weak supervision -- converter outputs atuais em labeling functions
- Etapa 3: Fine-tuning do head hepatologia com dados validados
- Etapa 4: Validacao comparativa (encoder vs regras vs medico)
- Gate de decisao: so promove encoder se F-beta(2) >= baseline (p < 0.05)
- Expansao de heads para demais especialidades
- **Entregavel:** modelo fine-tuned com metricas documentadas no MLflow

**Fase 4: Excelencia e evolucao continua (ongoing) -- GPU + API LLM**

- LLM fallback para casos inconclusivos (score entre 0.35-0.65)
- Active learning: selecao automatica de casos para anotacao medica
- Retraining periodico com novos dados validados
- NER clinico para extracao estruturada (entidades anatomicas, procedimentos)
- Orquestracao avancada opcional (LangGraph, Prefect ou equivalente) quando houver necessidade de fluxos multi-etapas

---

## 6. Framework de validacao estatistica

### 6.1 Principios

- Toda evolucao de engine exige **evidencia estatistica** de melhoria ou paridade
- Nenhum modelo vai a PRD sem validacao contra gold standard (lotes medicos)
- Metricas devem ser **clinicamente relevantes**, nao apenas tecnicamente corretas
- Resultados devem ser **reprodutiveis** (seeds fixos, versoes controladas, logs completos)

### 6.2 Metricas por tipo de tarefa

**Binary relevance (hepato, biliar, neuroimuno, etc.):**

| Metrica | Prioridade | Justificativa |
|---------|-----------|---------------|
| F-beta (beta=2) | **Principal** | Pondera recall 2x — perder caso relevante e mais grave que falso positivo |
| Recall | Critica | Taxa de deteccao de casos relevantes |
| Precision | Alta | Controle de falso positivo operacional |
| AUC-ROC | Alta | Performance independente de threshold |
| Specificity | Media | Complemento para visao completa |
| Negative predictive value | Media | % de nao-relevantes corretos |

**Category extraction (BI-RADS):**

| Metrica | Prioridade | Justificativa |
|---------|-----------|---------------|
| Exact match accuracy | Principal | Categoria deve ser exata |
| Confusion matrix por categoria | Critica | Identificar confusao entre categorias adjacentes |
| Weighted kappa (Cohen) | Alta | Concordancia ajustada por acaso, considerando ordinalidade |

**Multi-label (achados clinicos):**

| Metrica | Prioridade | Justificativa |
|---------|-----------|---------------|
| Micro-F1 | Principal | Performance global sobre todos os labels |
| Macro-F1 | Alta | Performance media por label (sensivel a classes raras) |
| Per-label recall | Critica | Nenhum achado clinico critico pode ser sistematicamente perdido |
| Hamming loss | Media | Fracao de labels incorretos |

### 6.3 Testes estatisticos

| Teste | Quando usar | Hipotese |
|-------|-------------|----------|
| McNemar's test | Comparacao pareada de dois classificadores no mesmo dataset | H0: modelos tem taxa de erro igual |
| Bootstrap CI (95%, 1000+ amostras) | Intervalos de confianca para qualquer metrica | Metrica real esta dentro do intervalo |
| DeLong test | Comparacao de AUC-ROC entre dois modelos | H0: AUCs sao iguais |
| Wilcoxon signed-rank | Comparacao de metricas entre folds de cross-validation | H0: mediana das diferencas e zero |
| Cochran's Q | Comparacao de 3+ classificadores simultaneamente | H0: todos os classificadores tem mesma taxa de erro |

### 6.4 Protocolo de validacao por fase

**Fase 1 — MVP rule-based (CPU):**

- Validacao de paridade: novo motor deterministico vs engine atual
- Criterio: exact match em 100% dos casos da amostra dourada (ou justificativa para divergencias)
- Metodo: comparacao direta output por output

**Fase 2 — NLP avancado com embeddings existentes (CPU):**

- Validacao incremental: motor rule-based + semantic_expand vs baseline Fase 1
- Criterio: recall >= baseline com ganho mensuravel em precision (p < 0.05, McNemar)
- Calibracao de thresholds de similaridade por especialidade

**Fase 3 — Encoder compartilhado (GPU):**

- Validacao tripla: encoder vs regras vs medico
- Criterio: F-beta(2) do encoder >= F-beta(2) do baseline com p < 0.05 (McNemar)
- Analise de discordancia: categorizar divergencias para revisao medica
- Bootstrap CI para todas as metricas
- Validacao por especialidade (por head): calibracao (reliability diagram + Brier score), monitoramento de drift (KS test entre janelas temporais)

**Fase 4 — Excelencia / LLM governado (GPU + API):**

- A/B testing operacional: rota parcial de casos para novo motor vs antigo
- Metricas de negocio: taxa de captacao, conversao, concordancia clinica
- Feedback loop: casos revisados pelo medico alimentam retraining
- NER clinico: extracao estruturada validada contra anotacao medica

### 6.5 Monitoramento continuo em producao

| Indicador | Frequencia | Alerta |
|-----------|-----------|--------|
| Distribuicao de scores (media, std, percentis) | Diario | Desvio > 2 std da baseline |
| Taxa de fl_relevante por especialidade | Diario | Variacao > 15% vs media movel 7d |
| Volume de laudos processados | Diario | Queda > 20% vs esperado |
| Latencia de batch | Por execucao | Excede janela operacional acordada |
| Drift de vocabulario (novos termos nao vistos no treino) | Semanal | > 5% de tokens OOV novos |
| Concordancia com amostras medicas periodicas | Mensal | Queda em qualquer metrica principal |

---

## 7. Resumo executivo de decisoes

| Decisao | Escolha | Fundamentacao |
|---------|---------|---------------|
| Arquitetura | Shared encoder + multi-task heads | Peng 2019, Caruana 1997, Ruder 2017 |
| Encoder base | BERTimbau (bert-base-portuguese-cased) | Melhor representacao PT-BR para fine-tuning |
| Treinamento | CPT -> Weak Supervision -> Fine-tuning -> Validacao | Gururangan 2020, Ratner 2020, Howard 2018 |
| Proporcionalidade | Modelo base (~110M), single GPU, batch | Custo/beneficio para volume e dados disponiveis |
| Metrica principal | F-beta (beta=2) | Prioriza recall clinico sem ignorar precision |
| Gate de promocao | p < 0.05 em McNemar + F-beta >= baseline | Rigor estatistico sem over-engineering |
| Fallback | Engine deterministico sempre disponivel | Seguranca clinica: nunca ficar sem cobertura |
| Regras residuais | Pos-processamento (BI-RADS, negacao, scoring) | Regras deterministicas onde sao superiores |
| LLM | Fase 4, fallback seletivo | Custo/latencia para batch, alucinacao |
| Endometriose | Descontinuado | Alinhamento com lideranca |
| Biliar / Neuroimuno | Entrando PRD nesta sprint | Alinhamento com lideranca |
| Piloto | Hepatologia | Engine mais simples, fluxo completo, decisao do Head |
| Posicionamento | Evolucao controlada com coexistencia | Pipelines atuais permanecem ate validacao |
| Canal de saida | Excel/SharePoint (inalterado) | Motor alimenta Delta; serving existente consome |
| Orquestracao | SQL + Jobs Databricks (inalterada) | Reescrita de orquestracao nao e pre-requisito |
| Infra Fases 1-2 | CPU (clusters atuais) | GPU so a partir da Fase 3 |

---

*Fim do documento v0.2. Proximo passo: Fase 1 (MVP rule-based).*
