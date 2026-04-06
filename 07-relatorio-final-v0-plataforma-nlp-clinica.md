# Relatorio final v0 -- Plataforma NLP clinica (Laudos)

**Versao:** v0.3
**Natureza:** consolidacao de discovery, analise de engines, visao de arquitetura, roadmap e recomendacoes
**Ambito:** 9 especialidades ativas (hepatologia, colon, DII, birads, pulmao, rim, biliar, neuroimunologia, reumatologia) + template `modelo`. Endometriose descontinuado.
**Contexto:** entregavel da **Sprint 1** -- estudo, diagnostico tecnico e proposta de evolucao arquitetural.
**Nota de revisao (v0.3):** inventario expandido para todas as especialidades; taxonomia de engines consolidada; governanca clinica e lacunas/oportunidades adicionadas; premissas operacionais explicitas.

---

## 1. Proposito deste documento

Este relatorio e o **entregavel principal da Sprint 1**. Centraliza, em linguagem clara para lideranca tecnica e produto:

- o **contexto de negocio** e o que os pipelines entregam hoje;
- o **estado tecnico** dos motores de NLP (cinco tipos distintos, mapeados em 9 especialidades);
- a **direcao recomendada**: um **motor NLP reutilizavel** (`TextPipeline` + `ClinicalNlpEngine` plugavel) e um **frame** operacional fino em Databricks;
- um **plano de acao faseado** (4 fases), praticas adequadas a **NLP em lote** sobre Delta, riscos, metricas e decisoes pendentes;
- as **premissas operacionais** alinhadas ao mapa corporativo (canal, orquestracao, infra, posicionamento).

O restante da Sprint 1 sera utilizado para modelagem inicial, refatoracao e organizacao, preparando a Sprint 2 para execucao em paralelo sem impacto em HML/PRD.

---

## 2. Posicionamento

Esta proposta e uma **evolucao controlada com coexistencia**, nao uma substituicao. Os pipelines atuais continuam rodando em PRD. O motor novo roda em paralelo para validacao. So substitui apos evidencia de paridade ou melhoria. Nao ha big bang, nao ha reescrita de orquestracao, nao ha mudanca de canal de saida.

---

## 3. Sumario executivo

| Dimensao | Sintese |
|----------|---------|
| **Negocio** | Apoio a **captacao** e linhas de cuidado com base em **laudos** (imagem e clinica), com resultados em tabelas analiticas e **consumo operacional via Excel / SharePoint / OneDrive** (canal oficial principal), alem de FTP e API (birads). Canal atual permanece; evolucao para API/Delta como canal primario e progressiva. |
| **Tipo de solucao** | Predominantemente **NLP em lote** no Databricks sobre **Delta**; integracoes **assincronas** (export, API em birads). Biliar e neuroimunologia entrando em PRD nesta sprint; endometriose descontinuado. |
| **Estado atual** | 9 especialidades ativas com **5 tipos de engine NLP** distintos (regras puras; embeddings + regras; regex/NLTK; SVM supervisionado; business rules). Duplicacao de utilitarios de texto, `pip install` repetido em notebooks, CI focado em **sincronizacao** sem gate de testes. 3 faixas de runtime Python (3.7, 3.9, 3.12) e 3 especialidades sem CI formal. |
| **Direcao** | **Evolucao controlada** (nao substituicao): biblioteca interna versionada com tronco comum de texto + **engines** por dominio + **configuracao por especialidade** (YAML/JSON); notebooks reduzidos a **fios de ligacao**; **contrato de entrada/saida** versionado. Pipelines atuais coexistem com o motor novo ate validacao de paridade. |
| **Risco de inacao** | Deriva de ambientes, bugs replicados em varias copias, onboarding lento para novas linhas clinicas, custo crescente de manutencao proporcional ao numero de algoritmos. |

---

## 4. Missao de negocio (inferida)

- Consolidar identificacao de **relevancia** ou **sinais estruturados** (categorias, achados) a partir de texto de laudo, alinhado a **linhas de cuidado** e operacao das unidades.
- **KPIs**, criterios legais de "captacao" e metas clinicas por linha devem ser **validados** com produto, compliance e operacoes (nao sao inferidos so pelo codigo).

---

## 5. Inventario tecnico -- visao por produto

### 5.1 Template `modelo`

- **Papel:** cookiecutter para **gerar** estrutura de novo projeto (pastas `data`, `serving`, `monitoring`, `setup`, `test`).
- **NLP:** nao contem motor de negocio; e **esqueleto**.

### 5.2 Hepatologia

- **Engine:** Grupo 1a -- regras puras.
- **NLP:** regras + **spaCy (Matcher)**, listas clinicas, segmentacao em blocos, flag de relevancia e analise estruturada na saida.
- **Negacao:** rudimentar (lista de tokens, match direto).
- **Treino:** nao ha ciclo supervisionado; imports legados (gensim/sklearn) sem uso no corpo principal.
- **Entrega:** padrao entrada Diamond -> algoritmo -> saida -> extracao/retorno/navegacao -> **Excel/SharePoint** (canal oficial).
- **CI:** `azure-pipelines.yml` (Python 3.12).

### 5.3 Colon

- **Engine:** Grupo 1b -- embeddings + regras + negacao avancada.
- **NLP:** **SentenceTransformer** (MiniLM multilingue) **+** regras (`process_report`, negacao por sentenca, taxonomia de colon/reto).
- **Negacao:** avancada (23 expressoes, janela 7 tokens, multi-token).
- **Treino:** modelo base pre-treinado publico; fine-tuning explicito nao identificado.
- **Formato:** notebooks principalmente `.ipynb`.
- **CI:** sem `azure-pipelines.yml`.

### 5.4 DII

- **Engine:** Grupo 2 -- supervisionado (SVM) + Grupo 1b para inferencia.
- **NLP / ML:** trilho **supervisionado** com **MLflow** e classificador **SVM** (v6 e v7 coexistindo); trilho de **inferencia** com spaCy + sentence-transformers, semelhante ao colon.
- **Negacao:** avancada (herdada da base colon).
- **Treino:** presente e versionado em notebooks dedicados (vetores, score, classificador). Maior proximidade com **MLOps classico**.
- **CI:** `azure-pipelines.yml` (Python 3.9).

### 5.5 Birads (BI-RADS / Enfermeiras navegadoras)

- **Engine:** Grupo 3 -- extracao por padrao (regex + NLTK).
- **NLP:** extracao por **regex e regras** (`get_num_birads`, `generate_birads`), **NLTK** para tokenizacao; saida com categoria BI-RADS numerica.
- **Treino:** nao supervisionado; `gensim` instalado sem uso aparente (candidato a limpeza).
- **Distintivo:** integracao via **API HTTP** e conjunto denso de scripts de monitoramento/logs.
- **CI:** `azure-pipelines.yml` (Python 3.12).

### 5.6 Pulmao

- **Engine:** Grupo 1b -- embeddings + regras.
- **NLP:** SentenceTransformer (MiniLM) + regras, limpeza de texto robusta (HTML/RTF). Estrutura similar a colon.
- **Treino:** nao identificado.
- **CI:** `azure-pipelines.yml` (Python 3.12).

### 5.7 Rim

- **Engine:** Grupo 4 -- business rules (calculo numerico).
- **NLP:** **nao utiliza NLP textual**. Aplica regras de negocio para calculo de clearance renal a partir de dados estruturados.
- **Treino:** nao aplicavel.
- **CI:** `azure-pipelines.yml` (Python 3.12).

### 5.8 Biliar

- **Engine:** Grupo 1b -- embeddings + regras + negacao avancada.
- **NLP:** hibrido regras + regex + spaCy + SentenceTransformer (MiniLM) com limpeza robusta de texto (HTML, RTF, boilerplate).
- **Negacao:** avancada (23 expressoes, janela 7 tokens, multi-token).
- **Status:** entrando em PRD nesta sprint.
- **CI:** sem `azure-pipelines.yml`.

### 5.9 Neuroimunologia

- **Engine:** Grupo 1b -- embeddings + regras + negacao avancada.
- **NLP:** mesmo padrao hibrido de biliar (regras + regex + spaCy + SentenceTransformer + limpeza robusta).
- **Negacao:** avancada (mesma implementacao).
- **Status:** entrando em PRD nesta sprint.
- **CI:** sem `azure-pipelines.yml`.

### 5.10 Reumatologia

- **Engine:** Grupo 1b -- embeddings + regras + negacao avancada.
- **NLP:** padrao hibrido similar a biliar/neuroimuno; SentenceTransformer + regras + negacao avancada.
- **Negacao:** avancada.
- **CI:** `azure-pipelines.yml` (Python 3.9).

### 5.11 Endometriose (descontinuado)

- **Status:** descontinuado. Continha notebooks de gold standard com `MlflowClient`.
- **CI:** `azure-pipelines.yml` (Python 3.9) -- inativo.
- **Observacao:** evidencia de possivel treinamento pre-existente; dados podem ser aproveitados como referencia futura.

---

## 6. Taxonomia consolidada de engines NLP

A analise dos 9 algoritmos identificou **5 grupos tecnicos** distintos, com grau crescente de sofisticacao:

| Grupo | Tipo | Especialidades | Componentes-chave |
|-------|------|----------------|-------------------|
| **1a** | Regras puras | Hepatologia | spaCy Matcher, listas clinicas, negacao rudimentar |
| **1b** | Embeddings + regras | Colon, Biliar, Neuroimuno, Reumatologia, Pulmao | SentenceTransformer (MiniLM), regras, negacao avancada (23 expr., janela 7 tok.) |
| **2** | Supervisionado (SVM) | DII (treino) | MLflow, sklearn SVM, pipeline vetores/score/classificador |
| **3** | Extracao por padrao | Birads | Regex, NLTK, extracao de categoria numerica |
| **4** | Business rules | Rim | Calculo numerico, sem NLP textual |

**Convergencia identificada:**

- O **Grupo 1b** e o mais representativo (5 de 9 especialidades) e o mais maduro em NLP. Sera a base do `TextPipeline` + `ClinicalNlpEngine`.
- O **Grupo 1a** (hepato) converge facilmente ao motor unificado: ja usa spaCy/regras, falta incorporar negacao avancada e embeddings.
- O **Grupo 2** (DII) utiliza engines Grupo 1b para inferencia; o treino supervisionado permanece como componente independente.
- O **Grupo 3** (birads) requer extracao especifica (regex + padrao numerico); encaixa como specialty head `HeadCategoryExtraction`.
- O **Grupo 4** (rim) e ortogonal (dados estruturados, sem texto); permanece como `BusinessRulesEngine`.

---

## 7. Como funciona o desenvolvimento hoje (maquina)

1. **Codigo** em Git por produto: notebooks **`.py`** (export Databricks) ou **`.ipynb`**.
2. **Azure Pipelines:** instalacao de **rdmlops** e execucao de **atualizacao de repositorio** no Databricks -- sincroniza conteudo com o **workspace** (nao substitui, por si, treino ou bateria de testes).
3. **Databricks:** **Jobs** encadeiam notebooks (entrada -> modelo/predicao -> serving -> monitoring), com **widgets** para ambiente, catalogo, data de execucao e identificacao do projeto. **Orquestracao:** SQL + Jobs lineares agendados.
4. **Dados:** leitura/escrita em **Delta**; dependencia frequente de `pip install` no inicio do notebook -> risco de **deriva** entre execucoes se nao houver imagem ou wheel padronizados.
5. **Saida operacional:** serving layer gera **Excel / SharePoint / OneDrive** (canal principal), API (birads), FTP e dashboards Power BI.
6. **Runtime:** 3 faixas -- Python 3.12 (hepato, birads, pulmao, rim), Python 3.9 (DII, reumatologia, endometriose), sem CI formal (colon, biliar, neuroimunologia).

---

## 8. Arquitetura alvo -- motor NLP + frame

### 8.1 Motor NLP (foco de reutilizacao)

A arquitetura evolui em **4 fases**. As **Fases 1-2** usam apenas engines rule-based + embeddings existentes (**CPU**, sem GPU). O **Shared Encoder + Multi-Task Learning** e o **objetivo de evolucao a partir da Fase 3**, com gate de validacao por fase -- so avanca com evidencia de ganho mensuravel sobre o baseline anterior.

Tres blocos do objetivo de evolucao (Fase 3+, condicional a validacao das fases anteriores):

1. **`TextPipeline`** -- normalizacao, limpeza (HTML, RTF, rodape), tokenizacao generica, segmentacao por secao/orgao, deteccao de negacao avancada (23 expressoes, janela 7 tokens); **pin** de versoes spaCy / NLTK num so lugar. Construido a partir da **consolidacao do melhor de cada algoritmo existente** (Grupo 1b como base principal).
2. **`SharedEncoder`** (Fase 3) -- transformer pre-treinado multilingual (BERTimbau) com continued pre-training em laudos clinicos PT-BR; representacao compartilhada entre todas as especialidades.
3. **`Specialty Heads`** (Fase 3) -- camadas de classificacao por tarefa:
   - **HeadBinaryRelevance** -- hepato, biliar, neuroimuno, reumato, colon, DII, pulmao;
   - **HeadCategoryExtraction** -- birads (hibrido com regex);
   - **RuleBasedFallback** -- engine deterministico como fallback de seguranca para todos;
   - **BusinessRulesEngine** -- rim (calculo numerico, sem NLP textual).

**Configuracao por `specialty_id`:** taxonomias, listas, tipo de head, thresholds e hiperparametros em **YAML** versionado (Git como fonte da verdade). YAML sera desenvolvido em conjunto com a EngML (externalizacao de configuracoes).

### 8.2 Frame operacional

Orquestracao permanece como **SQL + Jobs Databricks** (pipeline linear agendada) no curto prazo: resolver ambiente, nomes de tabelas, leitura conforme **contrato**, despacho ao engine certo, gravacao da saida, metadados (`specialty_id`, `config_version`, `engine_version`), politicas de otimizacao de tabela quando aplicavel. Nao ha reescrita de orquestracao.

**Separacao de responsabilidades:** ingestao SQL, **Excel / SharePoint / OneDrive** (canal oficial atual), FTP, API e dashboards sao **adaptadores** em volta do nucleo NLP e permanecem inalterados; o motor alimenta Delta e o serving layer existente continua consumindo. O maior ganho de reuse e o maior risco de divergencia clinica concentram-se no **texto**.

### 8.3 Contrato de dados

- Esquema de entrada/saida **versionado** (ex.: `clinical_nlp_io_v1`).
- Extensoes por especialidade sem quebrar o nucleo comum.

---

## 9. Plano de acao (fases)

### 9.1 Premissas operacionais

- **Canal de saida:** Excel / SharePoint / OneDrive e o canal oficial atual e permanece inalterado. Evolucao para API/Delta como canal primario e progressiva.
- **Orquestracao:** SQL + Jobs Databricks lineares agendados e o padrao atual. LangGraph, Prefect, Airflow ou Databricks Workflows sao opcoes futuras opcionais, nao pre-requisitos.
- **Clusters:** CPU (clusters atuais) para Fases 1-2. GPU a definir com EngML/MLOps apenas quando pertinente (Fase 3+).
- **Posicionamento:** evolucao, nao substituicao. Pipelines atuais coexistem em PRD ate validacao de paridade.
- **Gold standard:** outputs atuais dos algoritmos em PRD servem como baseline para validacao comparativa.
- **TextPipeline:** consolidacao do melhor de cada algoritmo existente; nao implementacao do zero.

### 9.2 Faseamento

| Fase | Prazo | Foco | Infra | Criterio de pronto |
|------|-------|------|-------|---------------------|
| **1 -- MVP rule-based** | 2-4 sem | Extrair TextPipeline do Grupo 1b; interface ClinicalNlpEngine; configs YAML hepato; negacao avancada; metricas; validacao paralela com hepato | **CPU** (clusters atuais) | Motor rule-based com paridade ou melhoria ao baseline |
| **2 -- NLP avancado** | 3-5 sem | Embeddings existentes (MiniLM) como componente opcional; configs biliar/neuroimuno; testes pytest; baseline metricas por especialidade | **CPU** (clusters atuais) | Paridade por especialidade + melhoria mensuravel |
| **3 -- Encoder** | 4-6 sem | Continued pre-training BERTimbau; weak supervision; fine-tuning head hepato; validacao tripla (encoder vs regras vs medico) | **GPU** (a definir com EngML) | F-beta(2) >= baseline com p < 0.05 |
| **4 -- Excelencia** | Continuo | Heads por especialidade; calibracao; monitoramento drift; NER clinico; LLM fallback seletivo | GPU + API LLM | Evolucao continua com evidencia estatistica |

**Dependencia logica:** cada fase apos a anterior; uma fase pode ser quebrada em n tasks distribuidas em uma ou mais sprints, conforme capacidade do time e facilidades encontradas.

### 9.3 Sprint 1 (atual)

- Estudo e diagnostico tecnico dos 9 algoritmos.
- Producao deste relatorio como entregavel principal.
- Restante da sprint: modelagem inicial, refatoracao e organizacao para a Sprint 2.

---

## 10. Governanca clinica

### 10.1 Fluxo pragmatico

1. **DS propoe:** implementa regra, engine ou modelo; valida contra gold standard (output atual).
2. **Medico valida:** revisa amostra de divergencias; aprova ou solicita ajustes.
3. **Deploy versionado:** so vai para HML/PRD apos aprovacao documentada.

### 10.2 Criterios por fase

| Fase | Validacao | Aprovador |
|------|-----------|-----------|
| 1 (MVP) | Paridade: exact match contra amostra dourada (outputs atuais) | DS + medico por amostragem |
| 2 (NLP avancado) | Paridade + melhoria mensuravel (precision/recall) | DS + medico |
| 3 (Encoder) | Tripla: encoder vs regras vs medico; F-beta(2) com p < 0.05 | DS + medico + gate estatistico |
| 4 (Excelencia) | A/B testing operacional; metricas de negocio | DS + medico + produto |

---

## 11. Lacunas e oportunidades identificadas

### 11.1 Lacunas criticas

| Lacuna | Impacto | Resposta proposta |
|--------|---------|-------------------|
| Duplicacao de logica de texto (limpeza, negacao, tokenizacao) em cada algoritmo | Bugs replicados, custo de manutencao multiplicado | `TextPipeline` unificado |
| `pip install` em notebook (sem wheel/imagem padronizada) | Deriva de dependencias entre execucoes | Wheel versionado ou imagem de cluster padronizada |
| CI apenas como sincronizacao (rdmlops) | Sem gate de qualidade, testes ou regressao | Gates progressivos: testes da lib como pre-requisito |
| 3 faixas de runtime Python (3.7, 3.9, 3.12) + 3 sem CI | Impede biblioteca unica sem alinhamento | Convergir para runtime unico (3.12 recomendado) com EngML |
| Ausencia de metricas padronizadas (precision/recall/F1 por engine) | Impossivel medir melhoria ou regressao objetivamente | Baseline de metricas na Fase 1 |
| Ausencia de logs estruturados de execucao NLP | Baixa observabilidade, dificil diagnosticar falhas | Metadados padronizados (`specialty_id`, `config_version`, `engine_version`) |

### 11.2 Oportunidades de alto valor

| Oportunidade | Beneficio |
|--------------|-----------|
| Negacao avancada do Grupo 1b (23 expressoes, janela 7 tokens) pronta para ser centralizada | Melhoria imediata para hepato (Grupo 1a, negacao rudimentar) |
| Outputs atuais dos algoritmos em PRD como gold standard e labeling functions (weak supervision) | Dados de treino em escala sem custo de anotacao manual |
| Algoritmos de biliar/neuroimuno/reumatologia com codigo quase identico | Consolidacao rapida via configuracao (YAML) ao inves de codigo separado |
| Cookiecutter `modelo` pode evoluir para consumir a lib diretamente | Onboarding de novas especialidades em horas ao inves de dias |

---

## 12. Boas praticas (NLP em lote no Databricks)

- Governanca de dados com **Unity Catalog** e convencoes de nome explicitas (resolver `ia` vs `diamond_*` por ADR).
- Uso de **batch** (ex.: `nlp.pipe`, batches de embeddings) em volumes grandes.
- **Nao** usar `nltk.download('all')` em producao; apenas recursos necessarios.
- **MLflow** no trilho supervisionado (DII); registrar versao de artefato na saida quando fizer sentido.
- **Testes** com frases sinteticas; evitar PHI em repositorio.
- **Logs** sem texto integral de laudo nem identificadores em claro.

---

## 13. Riscos consolidados

| Risco | Mitigacao |
|-------|-----------|
| Nomenclatura fragmentada de tabelas e catalogos | ADR antes de qualquer "builder" generico |
| Incompatibilidade entre abordagens (embeddings vs so regras) | Engines plugaveis, sem monolito unico |
| Bug em matcher regex (hepato) | Testes e correcao na fase de hardening |
| CI apenas como sync | Introduzir testes na lib e, gradualmente, gate de qualidade |
| Divergencia **Python 3.9** vs **3.12** entre pipelines; colon/biliar/neuroimuno sem CI | Alinhar com EngML; convergir para runtime unico |
| Big bang em todas as linhas | Uma linha piloto (hepato), depois expansao |
| Resistencia do time a mudanca arquitetural | Evolucao gradual; coexistencia; entrada pela Fase 1 (simples, CPU, sem complexidade nova) |
| Dependencia de EngML para wheel/cluster/GPU | Alinhamento proativo; Fases 1-2 nao dependem de GPU |

---

## 14. Metricas sugeridas

| Tipo | Exemplo |
|------|---------|
| Engenharia | Reducao de codigo duplicado; tempo para alterar so YAML |
| Qualidade | Numero de casos de teste por engine; falhas de regressao |
| Operacao | Incidentes por deriva de dependencia |
| Negocio | KPIs de captacao e concordancia clinica (definir na fase 1) |

---

## 15. Decisoes pendentes

| ID | Tema | Opcoes a decidir | Fase |
|----|------|-------------------|------|
| A | Hospedagem da biblioteca | Repositorio dedicado com wheel no feed interno **vs** pacote em monorepo | 1 |
| B | Onde vivem as regras | YAML/JSON no Git **vs** tabelas Delta **vs** hibrido | 1 |
| C | Produto piloto | **Hepatologia** (prioridade definida com lideranca) | 1 |
| D | Rigidez do CI | Somente sync **vs** incluir testes da lib como pre-requisito | 2 |
| E | Formato padrao de artefato | Notebooks `.py` exportados **vs** `.ipynb` para novos projetos | 1 |
| F | Canal de saida | Excel/SharePoint permanece; Delta como fonte intermediaria (confirmar) | 1 |
| G | Clusters GPU | Definicao de cluster GPU a alinhar com EngML/MLOps | 3 |
| H | Runtime Python | Convergir para 3.12 (recomendado) vs manter dual (3.9/3.12) | 1 |

---

## 16. Alinhamento com o mapa atual do sistema

Este relatorio esta **alinhado** ao diagrama corporativo **"Mapa Atual do Sistema de NLP Clinico"**, que descreve o fluxo operacional ponta a ponta e os gargalos que a plataforma proposta pretende enderecar.

### 16.1 Fluxo resumido (conforme o mapa)

- **Fontes:** sistemas hospitalares (HIS, TASY, MV, SPDT), exames, movimentacao de pacientes, laudos e dados clinicos associados.
- **Data Lake / camada Gold:** ingestao em Lake legado (1.0) e camada corporativa (2.0), convergindo para uso analitico.
- **ETL / engenharia:** pre-processamento com repositorios mestre e paralelos; referencia a tabelas analiticas centrais (por exemplo `gold_corporativo_m`, `m_gold_mov_paciente`, `m_gold_mov_exame`).
- **Orquestracao:** "malha-execucao" com gatilhos SQL; para **BI-RADS** e **Hepato** o mapa registra pipeline **linear e agendada**. SQL + Jobs Databricks e o padrao atual e permanece.
- **Camada de modelos NLP:** repositorios por especialidade (Hepato, Mama, Rim, entre outras), hoje **fragmentada** em 5 tipos de engine distintos.
- **Saida / consumo:** **Excel / SharePoint / OneDrive** (canal oficial principal), dashboards (Power BI), API (birads), indicadores clinicos.

### 16.2 Gargalos do mapa -- resposta da arquitetura alvo

| Gargalo ou risco (mapa) | Resposta (motor + frame + governanca) |
|-------------------------|----------------------------------------|
| Execucao em **cascata** (latencia e acoplamento) | **Engines** independentes por especialidade; possibilidade de **paralelizar** jobs por linha quando o contrato de dados permitir; frame fino so coordena I/O e metadados. |
| **Redundancia** (varias replicas do "mesmo" modelo) | **Biblioteca unica** (`TextPipeline` + interface comum); config por `specialty_id`; uma vez por tipo de engine, nao por copia de notebook. |
| Falta de **padrao / template** unificado | Cookiecutter `modelo` evolui para consumir a **lib**; estrutura e responsabilidades explicitas (dominio vs adaptadores). |
| **Regras hardcoded** | **Configuracao externa** versionada (YAML/JSON) com schema; Git como fonte da verdade. |
| **Deploy manual** DEV -> HML | CI/CD com **rdmlops** pode ganhar **gates** progressivos (testes da lib, pin de runtime); matriz Python alinhada a EngML. |
| Dependencia excessiva de **notebook** | Logica de negocio na **lib**; notebooks como orquestracao minima no Databricks. |
| **Baixa observabilidade** | Metadados de execucao (`specialty_id`, `config_version`, `engine_version`); evolucao para hub de monitoramento alinhado ao bloco "metricas" do mapa. |
| **Excel como canal de saida** | Excel / SharePoint e o **canal oficial atual e permanece**. Motor alimenta Delta; serving layer existente consome e gera Excel. Evolucao para API/Delta como canal primario e progressiva, nao pre-requisito. |

### 16.3 Piloto e prioridade de validacao

O mapa coloca **Hepato** e **BI-RADS** na mesma malha operacional. A decisao de produto priorizada com lideranca e validar **Hepatologia legado vs. novo motor** (paridade em amostra dourada antes de ampliar). **BI-RADS** permanece referencia de padrao (extracao por padrao) para fases subsequentes, conforme maturidade e capacidade do time.

---

## 17. Limitacoes deste relatorio (v0.3)

- Conteudo baseado em **analise estatica** dos repositorios do workspace; **nao** substitui runbooks internos de MLOps nem configuracao real dos Jobs no Databricks.
- README de nivel de produto esta **incompleto** em alguns repos (ex.: DII); descricoes de negocio finas devem ser **confirmadas** pelo time dono.
- Auditoria complementar especifica (ex.: matcher regex em hepato) pode ser anexada em **v1** apos revisao de codigo direcionada.
- Metricas reais de baseline (precision, recall, F1) por especialidade ainda nao calculadas; sera prioridade na Fase 1.

---

*Fim do relatorio v0.3.*
