# Analise profunda dos engines NLP — cenario atual e oportunidades de evolucao (v0)

## Convencoes

- Documento organizado por **tipo de engine**, nao por especialidade.
- Evidencias extraidas por analise estatica dos repositorios em `algoritmos/` e `old(first)/`.
- Listas clinicas resumidas por clareza; versoes completas nos notebooks originais.

---

# PARTE I — Grupos de engine (analise profunda)

---

## Grupo 1: Rule-based classico (spaCy Matcher + listas)

**Especialidade representante:** Hepatologia

### Bibliotecas

spaCy (`pt_core_news_lg`, NER desabilitado, sentencizer customizado com `\n`), NLTK (stopwords, tokenizacao), fuzzywuzzy (importado, pouco uso ativo), gensim (importado, sem uso no fluxo principal), unidecode, striprtf, regex.

### Tecnica NLP

1. Limpeza textual sequencial: `remove_final_laudo` -> `preprocess_text` -> `retira_acentos` -> `retira_pontuacao_stopwords` -> `corrige_orgaos` -> `remove_plurais`.
2. Tokenizacao via spaCy (`nlp(texto)`).
3. Segmentacao em **blocos por orgao**: `monta_blocos_spacy` usa `Matcher` com lista de orgaos para dividir laudo em segmentos anatomicos.
4. Dentro de cada bloco: `Matcher` busca **palavras-chave** (ex.: "figado", "hepat"), depois **problemas** (lista de ~35 termos clinicos).
5. Deteccao de **negacao por janela**: `matcher_irrelevants` busca "ausencia", "nao ha" nos 3 tokens antes/depois do achado.
6. `fl_relevante = True` se qualquer bloco tem pelo menos 1 problema sem negacao.

### Listas clinicas (Hepatologia)

- **Palavras-chave:** `figado`, `hepat`, `vias biliares`, `vesicula biliar`
- **Problemas (~35 termos):** cirrose, fibrose, hepatite, hipertensao portal, trombose veia porta, lirads 4/5, nodulo hipervascular, varizes esofagianas/gastricas, hepatopatia cronica, microlitiase, entre outros.
- **Orgaos (segmentacao):** figado, baco, bexiga, pancreas, reto, rim/rins, utero, ovarios, esofago, estomago, vesicula biliar, vias biliares.
- **Palavras irrelevantes (negacao):** `ausencia`, `nao ha`

### Schema de saida

`dt_execucao`, `id_predicao`, `id_exame`, `id_paciente`, `proced_laudo_exame`, `proced_laudo_tokens` (array), `analise_blocos` (array de structs com bloco/orgaos/palavras-chave/problemas/relevancia), `fl_relevante`.

### Observacoes criticas

- Imports de `gensim`, `CountVectorizer`, `fuzzywuzzy` sem uso no fluxo principal (legado).
- Negacao simplificada (apenas 2 termos, janela de 3 tokens).
- Stop words customizadas mantendo operadores de negacao (`nao`, `sem`, `ha`).
- Fluxo inteiro em Pandas (`.apply` row-by-row), sem paralelizacao batch.

---

## Grupo 1b: Rule-based evoluido (spaCy + Embeddings + Regex + Negacao avancada)

**Especialidades:** Biliar, Neuroimunologia, Reumatologia

### Bibliotecas

spaCy (`pt_core_news_lg`, NER off, sentencizer `\n`), SentenceTransformer (`paraphrase-multilingual-MiniLM-L12-v2`), difflib (`SequenceMatcher`), striprtf, lxml, html2text, ftfy, numpy, regex. **Sem** NLTK, **sem** sklearn.

### Tecnica NLP

1. Limpeza robusta: `to_plain()` com deteccao RTF/HTML/plain, pypandoc fallback, ftfy, normalizacao NFKC, remocao boilerplate OCR, correcoes de acentuacao.
2. `_force_sentence_breaks` + `nlp(text)` (spaCy).
3. Segmentacao por **headers** (regex em cabecalhos de laudo: "FIGADO:", "CONCLUSAO:") ou por **ancoras de orgao** (seeds + regex + lexicon fuzzy).
4. Expansao semantica de findings: `semantic_expand` combina **0.75 * cosine_sim(embeddings) + 0.25 * fuzzy_sim**, threshold >= 0.65.
5. Match de achados via **regex accent-tolerant** (`_accent_rx`) com word boundaries.
6. Filtros de proximidade: finding a <= 200 chars de mencao de orgao, sentenca deve citar orgao, desambiguacao por orgao mais proximo (margem 25 chars).
7. Negacao avancada: lista de ~23 expressoes, janela de **7 tokens** (left/right na mesma sentenca), tratamento de frases multi-token, singleton ban para falsos positivos.
8. Confianca: 0.9 se nao negado, 0.35 se negado.
9. `fl_relevante = 1` se `summary_compact` nao vazio (achados positivos apos filtros).

### CONFIG compartilhado

Os 3 notebooks (biliar, neuroimuno, reumato) **compartilham a mesma mega-CONFIG** com orgaos/findings de multiplos dominios. Cada notebook ativa apenas seu `TARGET_ORGAN`:


| Notebook        | TARGET_ORGAN       | FORCE_FULL_DOC_FOR          |
| --------------- | ------------------ | --------------------------- |
| Biliar          | `doencas_biliares` | nao                         |
| Neuroimunologia | `neuroimunologia`  | sim (doc inteiro = 1 bloco) |
| Reumatologia    | `reumatologia`     | sim (doc inteiro = 1 bloco) |


### Listas clinicas (resumo por especialidade)

**Biliar:** diagnosticos biliares (colecistite cronica, coledocolitiase, pancreatite biliar, ileo biliar, hidropsia, carcinoma vesicula), colelitiase (calculo, sinal WES), neoplasias (polipo sessil, massa sugestiva), sinais inflamatorios (empiema, Murphy US, distensao).

**Neuroimunologia:** sinais desmielinizantes/neuroinflamatorios (neurite optica, mielite, criterios de McDonald), diagnosticos (Esclerose Multipla, NMO, NMOSD, MOGAD, ADEM, Anti-MOG, Encefalite autoimune/limbica).

**Reumatologia:** espondilite (anquilosante, sindesmofito, Romanus), sacroileite (bilateral, erosoes, edema, anquilose), artrite reumatoide (pannus, erosoes marginais), artrite psoriasica (dactilite, entesite, acroosteolise), nodulo reumatoide.

### Schema de saida (comum aos 3)

`record_id`, `id_pct`, `idunidade`, `unidade`, `pct_nome`, `pct_cpf`, `pct_sexo`, `pct_datanasc`, `idade_paciente`, `telefonePacienteDDD`, `telefonePaciente`, `exm_data`, `exm_mod`, `exm_titulo`, `exm_tipo`, `num_pedido_integracao`, `nme_regional_hospital`, `nme_convenio`, `exm_an`, `exm_status`, `exm_laudo_dataliber`, `exm_laudo_texto`, `exm_laudo_texto_tratado`, `exm_laudo_resultado` (JSON summary_compact), `exm_laudo_achados`, `fl_relevante`, `dataExecucaoModelo`.

### Observacoes criticas

- CONFIG duplicado entre notebooks (risco de divergencia).
- Bugs potenciais detectados: virgula faltante em listas (neuroimuno), nomes herdados de DII/colon em helpers.
- Negacao muito mais sofisticada que Hepato (23 expressoes vs 2, janela 7 vs 3).
- `FORCE_FULL_DOC_FOR` para neuroimuno/reumato: sem segmentacao anatomica, todo o doc e analisado.

---

## Grupo 2: Embedding + Rules (SentenceTransformer + regras)

**Especialidades:** Colon, DII (inferencia), Pulmao

### Bibliotecas

Identicas ao Grupo 1b: spaCy, SentenceTransformer, difflib, striprtf, lxml, html2text, ftfy. **Sem** NLTK, **sem** sklearn.

### Tecnica NLP

Mesma arquitetura do Grupo 1b (compartilham base de codigo), com diferencas de parametrizacao:


| Parametro               | Colon        | DII      | Pulmao            |
| ----------------------- | ------------ | -------- | ----------------- |
| TARGET_ORGAN            | `colon_reto` | `dii`    | (similar a colon) |
| min_sim_seed_vocab      | 0.65         | **0.90** | ~0.65             |
| top_k_per_seed          | 24           | **10**   | ~24               |
| window_tokens (negacao) | 7            | **10**   | ~7                |
| FORCE_FULL_DOC_FOR      | nao          | nao      | nao               |


### Listas clinicas

**Colon:** lesao (vegetante, infiltrativa, exofitica, deprimida, estenosante), polipo (pediculado, sessil, Paris 0-II/IIa/IIb/IIc), ulceracao (ulcera, erosao, ulcero-infiltrativa), tumor/massa (LST, adenocarcinoma, carcinoma, neoplasia, metastase).

**DII:** dii_explicito (doenca de Crohn, retocolite ulcerativa, colonoscopia), parede_ativa (9 variantes de espessamento/realce parietal/mucoso com verbos "mantido/persiste/permanece"), complicacoes_sugestivas (trajeto fistuloso, fistula enterovesical/perianal, abscesso perianal, estenose ileal, dilatacao pre-estenotica).

DII tem negacao mais rica: 30 expressoes vs 23 do baseline, incluindo "nao se observa", "nao se caracterizando", "ausencia de evidencias/achados/alteracoes".

### Observacoes criticas

- DII usa threshold de similaridade **muito mais restritivo** (0.90 vs 0.65) e menos candidatos por seed (10 vs 24).
- Colon filtra `modalidade != 'COL'` na entrada (exclui colonoscopia — tratada em notebook separado `ntb_ia_colon_algoritmo_colonoscopia.ipynb`).
- Schema de saida identico ao Grupo 1b.

---

## Grupo 3: Pattern Extraction (regex + NLTK)

**Especialidade:** Birads

### Bibliotecas

NLTK (stopwords, word_tokenize), gensim (importado, sem uso), regex. **Sem** spaCy, **sem** SentenceTransformer, **sem** sklearn.

### Tecnica NLP

1. Normalizacao: `remover_acentos_caracteres_especiais` — ~25 regex sequenciais para limpar medidas, datas, romanos, boilerplate ACR, `bi-rads` -> `bi_rads`.
2. `clean_text`: mais regex para normalizar variantes de "bi rads"/"bi rad"/"birads_acr".
3. Tokenizacao NLTK + remocao de stopwords portuguesas.
4. `get_next_words`: busca tokens matching `birads`/`bi_rads`/`categoria`, coleta **+-3 tokens** vizinhos.
5. `generate_birads`: converte romanos (i-v -> 1-5), extrai digitos, mapeia para categoria BI-RADS.
6. **Priorizacao:** `max()` dos digitos extraidos (categoria mais alta vence).

### Negacao

**Nenhuma.** Linha comentada `#.replace('negativo', ' 9 ')` indica intencao nao implementada.

### Schema de saida

`id_predicao`, `dt_execucao`, `id_exame`, `id_paciente`, `id_medico_encaminhador`, `id_unidade`, `dt_exame`, `vl_proced_birads` (LONG — categoria numerica), `proced_laudo_exame`, `proced_laudo_limpo`, `fl_integracao_concluida`.

### Observacoes criticas

- Abordagem mais simples de todos os engines (puro regex + janela de tokens).
- Sem deteccao de negacao (risco de falso positivo: "BI-RADS 4 descartado" contaria como 4).
- RTF: funcao `laudo_rtf_to_text` definida mas **nao aplicada** no pipeline ativo.
- gensim/fuzzywuzzy instalados sem uso (legado).
- Logica de `eval(str(...))` para flatten de listas: fragil e risco de runtime error.

---

## Grupo 4: Supervised ML (SVM + MLflow)

**Especialidade:** DII (treinamento — em `old(first)/DII/`)

### Bibliotecas

sklearn (`svm.SVC`, `classification_report`), MLflow (experiment tracking, autolog, metricas), pandas, pickle.

### Pipeline de treinamento

1. Leitura de `tbl_gold_dii_treinamento_v1_vetor` com colunas `idTreinamento`, `laudoVetorizado` (vetor 1024-dim, provavelmente de SentenceTransformer), `probabilidade` (target: "alta"/"baixa").
2. Features: `X = DataFrame(laudoVetorizado)` com 1024 colunas.
3. Modelo: `svm.SVC(probability=True, C=50)`.
4. Treina com **dataset inteiro** (sem train/test split real — bloco de split esta comentado).
5. Avaliacao: `classification_report` logado no MLflow.
6. Artefatos salvos em `/Shared/IA/MLflow/{experiment_name}`.

### Observacoes criticas

- **Treinamento sem holdout real:** train = test = dataset completo (overfitting garantido nas metricas reportadas).
- **Sem validacao cruzada** no fluxo ativo.
- **class_weight = None** (comentado).
- Vetorizacao (1024 dims) feita em notebook separado (`v7_vetores.py`), provavelmente com SentenceTransformer.
- Pipeline de treino esta em `old(first)/` — **nao esta no `algoritmos/` ativo**. O algoritmo de inferencia DII atual usa embeddings + regras (Grupo 2), sem consumir modelo SVM.
- Resquicio de ciclo de treino supervisionado que **pode ter sido abandonado** em favor do approach hibrido.

---

## Grupo 5: Business Rules (sem NLP textual)

**Especialidade:** Rim

### Bibliotecas

PySpark (SQL, UDF), regex basico. **Sem** NLP (sem spaCy, NLTK, transformers, sklearn).

### Tecnica

1. SQL corporativo: cruza `tb_gold_mov_observation_exames` (TUSS 40301630 = creatinina) com tabelas de paciente, medico, convenio, organizacao.
2. Limpeza de valor: `replace('Inferior a ', '')`, virgula -> ponto.
3. Calculo de clearance via UDF: formula **CKD-EPI-style** (sem peso):
  - Feminino: `142 * ((creatinina / 0.7) ^ -1.2) * (0.9938 ^ idade) * 1.012`
  - Masculino: `142 * ((creatinina / 0.9) ^ -1.2) * (0.9938 ^ idade)`
4. MERGE INTO `tb_diamond_nefrologia_clearance_saida`.
5. View `vw_gold_creatinina` com filtro: `0.1 <= clearance <= 60`, exclusao de convenio Amil (regex).

### Observacoes

- Cockcroft-Gault e Schwartz definidos mas **nao usados** no fluxo ativo.
- Nao ha processamento de texto livre.
- Saida totalmente estruturada (dados numericos de laboratorio).

---

# PARTE II — Padrao comum das 5 etapas por grupo


| Etapa                | Grupo 1 (Hepato)                                              | Grupo 1b/2 (Biliar/Neuro/Reumato/Colon/DII/Pulmao)                        | Grupo 3 (Birads)                                   | Grupo 5 (Rim)                                        |
| -------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------- | ---------------------------------------------------- |
| **1. Ingestao**      | `gold_corporativo_ia` -> `tb_diamond_mod_hepatologia_entrada` | `gold_corporativo_ia` -> `tb_diamond_mod_{esp}_entrada` ou leitura direta | `tb_diamond_mod_birads_entrada`                    | `gold_corporativo.observation` -> chain `wrk_01..06` |
| **2. Motor**         | spaCy Matcher + listas                                        | spaCy + SentenceTransformer + regex + negacao avancada                    | regex + NLTK (janela +-3 tokens)                   | CKD-EPI UDF (calculo numerico)                       |
| **3. Serving**       | extracao + retorno + navegacao (decrypt, join, export)        | extracao (decrypt, join, export Excel/planilha)                           | envio API + views                                  | MERGE + view filtrada                                |
| **4. Monitoramento** | `ia.tb_diamond_mod_monitoramento` (MERGE)                     | `ia.tb_diamond_mod_monitoramento` (MERGE)                                 | `ia.tb_diamond_mod_monitoramento` + logs analytics | `ia.tb_diamond_mod_monitoramento` (MERGE)            |
| **5. Setup**         | DDL tabelas + views + optimize                                | DDL inline nos notebooks                                                  | DDL CREATE TABLE                                   | DDL via SQL magic cells                              |


---

# PARTE III — Oportunidades de evolucao

## Conversao: quando migrar de um tipo de engine para outro


| De                             | Para                                               | Quando faz sentido                                                                                                 | Pre-requisito                                                                      |
| ------------------------------ | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| Grupo 1 (Hepato: Matcher puro) | Grupo 1b (Matcher + embeddings + negacao avancada) | **Imediato** — ganho direto em negacao (2 termos -> 23+), limpeza de texto (RTF/HTML), e reducao de falso positivo | Migrar listas clinicas para CONFIG unificado; validar paridade com amostra dourada |
| Grupo 3 (Birads: regex puro)   | Grupo 1b ou custom pattern engine + negacao        | Quando taxa de falso positivo for relevante (ex.: "BI-RADS 4 descartado" contado como 4)                           | Implementar negacao minima; validar com equipe clinica                             |
| Grupo 4 (SVM legado)           | Nao migrar — **ja foi abandonado** na pratica      | O DII atual ja usa Grupo 2 (embeddings + regras) para inferencia                                                   | Documentar decisao de descontinuidade do SVM                                       |


## Adicao: camadas complementares


| Camada                                                                         | Onde aplica                                                           | Ganho esperado                                           | Risco                                                             |
| ------------------------------------------------------------------------------ | --------------------------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------------------- |
| **LLM fallback** (GPT-4o/Claude via API)                                       | Casos inconclusivos em qualquer engine (score entre 0.35 e 0.65)      | Resolver ambiguidades que regras + embeddings nao cobrem | Custo por token, latencia, LGPD (anonimizar antes de enviar)      |
| **NER clinico** (modelo fine-tuned ou pre-treinado tipo PubMedBERT/BioMistral) | Extracao de entidades estruturadas (medicamentos, dosagens, anatomia) | Enriquecer achados com contexto clinico                  | Necessita corpus anotado ou modelo pre-treinado adaptado ao PT-BR |
| **Negacao avancada (NegEx/NegBio)**                                            | Grupo 1 (Hepato) e Grupo 3 (Birads)                                   | Reduzir falso positivo sistematicamente                  | Adaptacao ao portugues; integracao com pipeline existente         |
| **Scoring probabilistico** (calibracao)                                        | Todos os engines com confianca binaria (0.35/0.9)                     | Priorizacao operacional mais granular                    | Exige rotulo de validacao para calibrar                           |


## Combinacao: pipeline multi-engine


| Pipeline                                                   | Aplicacao                              | Logica                                                                       |
| ---------------------------------------------------------- | -------------------------------------- | ---------------------------------------------------------------------------- |
| Regras (pre-filtro) -> Embeddings (ranking) -> Score final | Colon/DII com volume alto              | Regras eliminam irrelevantes rapido, embeddings refinam, score prioriza fila |
| Embeddings (deteccao) -> LLM (validacao de caso complexo)  | Neuroimuno/Reumato com laudos ambiguos | Embedding detecta candidatos, LLM confirma com raciocinio sobre contexto     |
| Pattern (extracao) -> Regras (negacao) -> Score            | Birads                                 | Regex extrai categoria, regras validam negacao/contexto, score prioriza      |


## Matriz de decisao para transicao


| Criterio                        | Peso    | Quando autoriza evolucao                                |
| ------------------------------- | ------- | ------------------------------------------------------- |
| Volume de laudos/dia            | alto    | > 500 laudos/dia justifica investimento em otimizacao   |
| Disponibilidade de rotulo       | critico | Sem rotulo confiavel, nao ligar trilha supervisionada   |
| Ganho esperado vs baseline      | alto    | Melhoria estatisticamente relevante em recall/precision |
| Custo operacional (token/GPU)   | medio   | Gate: custo < valor da captacao incremental             |
| Risco clinico de regressao      | critico | Nenhuma evolucao sem paridade em amostra dourada        |
| Capacidade do time de sustentar | alto    | Se equipe nao consegue operar, adiar                    |


---

# PARTE IV — Roadmap de excelencia

## Fase 1: MVP rule-based (imediato)

**Objetivo:** unificar motor com qualidade minima equivalente. **Infra: CPU.**

- Extrair TextPipeline compartilhado do Grupo 1b (limpeza, negacao avancada, segmentacao).
- Estender Hepato com negacao avancada (23 expressoes, janela 7 tokens) e limpeza RTF/HTML.
- Adicionar negacao basica ao Birads.
- Unificar CONFIG em YAML versionado (eliminar duplicacao entre notebooks).
- Padronizar schema de saida (campo `exm_laudo_resultado` com summary_compact em todos).
- Documentar e arquivar trilha SVM (Grupo 4) como legado.

## Fase 2: NLP avancado com embeddings existentes (3-5 semanas)

**Objetivo:** maximizar cobertura com embeddings ja validados em producao. **Infra: CPU.**

- Encapsular SentenceTransformer (MiniLM, ja em uso nos Grupos 1b/2) como componente opcional de `semantic_expand`.
- Calibrar thresholds de similaridade por especialidade com metricas de validacao (precision/recall por categoria de achado).
- Implementar scoring com granularidade (0.0-1.0 ao inves de binario 0.35/0.90).

## Fase 3: Encoder compartilhado + supervisionado (4-6 semanas)

**Objetivo de evolucao:** encoder fine-tuned com specialty heads. **Infra: GPU (a definir com EngML).** Fase condicional a validacao de paridade/ganho das Fases 1-2.

- Continued pre-training do BERTimbau em laudos clinicos PT-BR.
- Weak supervision (outputs atuais como labeling functions).
- Fine-tuning de specialty heads com dados validados pelo corpo medico.
- Validacao tripla (encoder vs regras vs medico) com gate estatistico.
- Pipeline de treino padronizado com MLflow (versionamento, metricas, drift monitoring).
- Validacao com holdout real e cross-validation (corrigir pratica atual de treino=teste).

## Fase 4: Excelencia / LLM governado (continuo)

**Objetivo:** resolver casos inconclusivos com IA generativa, dentro de guardrails. **Infra: GPU + API LLM.**

- LLM como fallback seletivo (nao como engine primario).
- NER clinico para extracao estruturada (entidades anatomicas, procedimentos).
- Anonimizacao obrigatoria antes de chamada externa.
- Schema estrito de resposta (JSON/Pydantic).
- Trilha de auditoria de prompts/versoes/resultados.
- Gate por metricas: so escala se custo < valor de captacao incremental.
- Fallback para regras quando output LLM for inconclusivo.

---

## Caminho de evolucao: de engines fragmentados para encoder compartilhado

**Nota v0.2 -- faseamento:** a arquitetura evolui em 4 fases. O **Shared Encoder + Multi-Task Learning** e o **objetivo de evolucao a partir da Fase 3**, com gate de validacao -- so avanca com evidencia de ganho sobre o baseline anterior (fundamentacao cientifica em `04-visao-refinada-motor-nlp-unificado-v0.md`). As Fases 1-2 constroem a base rule-based e embeddings existentes, sem GPU.

Resumo da transicao:

- **Fase 1 -- TextPipeline compartilhado (CPU):** unifica limpeza/normalizacao/negacao do Grupo 1b como base comum + YAML por especialidade
- **Fase 2 -- Embeddings existentes (CPU):** MiniLM (ja em uso) encapsulado como componente opcional de `semantic_expand`
- **Fase 3 -- Encoder compartilhado (GPU):** BERTimbau com continued pre-training complementa (nao substitui) embeddings existentes; specialty heads por especialidade
- **Fallback deterministico:** engines atuais permanecem como fallback de seguranca em todas as fases
- **Pipeline de treinamento (Fase 3):** CPT -> Weak Supervision (outputs atuais como pseudo-labels) -> Fine-tuning (lotes medicos) -> Validacao comparativa

---

## Resumo: mapa de convergencia por especialidade (atualizado v0.1)


| Especialidade   | Status         | Engine atual             | Fase 1 (MVP rule-based, CPU)                  | Fase 2 (NLP avancado, CPU) | Fase 3 (encoder, GPU)  | Fase 4 (excelencia)       |
| --------------- | -------------- | ------------------------ | --------------------------------------------- | -------------------------- | ---------------------- | ------------------------- |
| Hepatologia     | ativo PRD      | Grupo 1 (Matcher puro)  | **Estender c/ negacao avancada + CONFIG YAML** | + embeddings (MiniLM)      | **Piloto encoder**     | + active learning + NER   |
| Biliar          | **entrando PRD** | Grupo 1b               | Unificar CONFIG                               | Manter embeddings          | Head encoder           | + NER + scoring           |
| Neuroimunologia | **entrando PRD** | Grupo 1b               | Unificar CONFIG                               | Manter embeddings          | Head encoder           | + LLM fallback + NER     |
| Reumatologia    | ativo PRD      | Grupo 1b                | Unificar CONFIG                               | Manter embeddings          | Head encoder           | + NER + scoring           |
| Colon           | ativo PRD      | Grupo 2                 | Unificar CONFIG                               | Manter embeddings          | Head encoder           | + scoring + calibracao    |
| DII             | ativo PRD      | Grupo 2 (SVM arquivado) | Unificar CONFIG, arquivar SVM                 | Manter embeddings          | Head encoder           | + scoring + calibracao    |
| Pulmao          | ativo PRD      | Grupo 2                 | Unificar CONFIG                               | Manter embeddings          | Head encoder           | + scoring + calibracao    |
| Birads          | ativo PRD      | Grupo 3 (regex puro)    | **+ negacao basica**                          | Manter regex               | Hibrido encoder+regex  | + pattern robusto         |
| Rim             | ativo PRD      | Grupo 5 (business rules)| Manter (padronizar config)                    | N/A                        | N/A                    | Manter                    |
| ~~Endometriose~~| descontinuado  | ---                     | ---                                           | ---                        | ---                    | ---                       |

Detalhamento completo de fases, treinamento, configs e validacao: `visao-refinada-motor-nlp-unificado-v0.md`.

**Nota:** Fases 1-2 rodam em CPU nos clusters atuais. GPU so e necessaria a partir da Fase 3. Canal de saida (Excel/SharePoint) permanece inalterado em todas as fases.


