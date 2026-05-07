# Documento auxiliar do brainstorm v0

**Tema:** Motor de triagem para captacao de pacientes de alta complexidade a partir de laudos  
**Foco:** motor, ciencia de dados, NLP, deep learning, LLM e ML  
**Base:** consolidacao critica do brainstorm + contexto tecnico atual dos algoritmos em operacao

---

## 1) Leitura executiva

O brainstorm tem boa direcao estrategica: sair de uma logica de "pagina estatica" para um **motor NLP de triagem inteligente** com modularizacao por especialidade.  
Para o nosso contexto, a recomendacao e:

- usar **arquitetura hibrida** (regras + semantica + opcional supervisionado),
- padronizar o **motor comum** antes de escalar LLM em toda a cadeia,
- e tratar LLM como camada complementar com governanca clinica e LGPD.

---

## 2) Sintese critica do brainstorm (Adotar, Adaptar, Evitar)

## Adotar agora

- **Motor modular por especialidade** (codigo separado de configuracao).
- **Scoring de priorizacao** (urgencia/complexidade) para fila operacional.
- **Camada de seguranca/LGPD** (mascaramento, trilha de auditoria).
- **Pipeline em camadas**: ingestao -> extracao -> scoring -> entrega operacional.
- **Separacao motor vs canais** (Excel/API/CRM/FTP como adaptadores).

## Adaptar ao nosso contexto

- **LLM para extracao estruturada**: usar em casos ruidosos/ambiguos, nao como base unica.
- **RAG clinico**: **fora do escopo atual**, pois nao ha acesso a protocolos/diretrizes medicas estruturadas para formar base confiavel.
- **Vector DB**: fase de expansao, nao prerequisito para MVP.
- **ML supervisionado (XGBoost/LightGBM)**: somente com rotulo confiavel e governanca.
- **Promessa de feedback ao paciente em tempo real**: condicionar a validacao medico-juridica.

## Evitar na fase inicial

- Abordagem **LLM-first** sem padrao de qualidade e sem guardrails.
- Comecar por abordagem de canal/marketing-first sem maturidade do motor de triagem.
- Big bang de migracao em todas as especialidades ao mesmo tempo.
- Dependencia de stack nova complexa antes de consolidar o baseline.

---

## 3) Mapeamento para o contexto real (estado atual)

Hoje temos multiplos estilos convivendo (9 especialidades ativas, endometriose descontinuado):

- regras + spaCy (hepatologia),
- embeddings + regras + negacao avancada (biliar **[PRD nesta sprint]**, neuroimunologia **[PRD nesta sprint]**, reumatologia, colon, DII, pulmao),
- regex/tokenizacao para categorias especificas (birads),
- business rules sem NLP textual (rim),
- trilha supervisionada legado arquivada (DII SVM).

Logo, o "melhor de todos os mundos" nao e um algoritmo unico, e sim:

1. **TextPipeline comum** (limpeza/normalizacao/segmentacao),
2. **ClinicalNlpEngine** (interface comum),
3. **engines plugaveis** por tipo de problema,
4. **config por especialidade** (YAML/JSON),
5. **contrato de saida versionado**.

---

## 3.1) Alinhamento ao mapa atual do sistema NLP (oficial)

O diagrama **“Mapa Atual do Sistema de NLP Clinico - Rede D’Or”** confirma o diagnostico deste documento: fluxo **Lake 1.0/2.0 -> ETL e malha de execucao (SQL agendada) -> camada NLP fragmentada por especialidade -> consumo (Excel/SharePoint como canal oficial principal, BI, API, indicadores)**. O **Relatorio final v0 -- Plataforma NLP clinica** consolida o resumo do fluxo, a tabela **gargalo -> resposta da plataforma** e a prioridade de piloto **Hepatologia (legado vs motor novo)** frente a **BI-RADS** na mesma malha.

**Enfase tecnica (mapa -> evolucao)**

- **Orquestracao hoje:** gatilhos SQL e pipeline **linear** (mapa cita BI-RADS e Hepato nesse padrao). Isso **nao invalida** a biblioteca NLP; exige que o **frame** Databricks respeite a malha existente enquanto a EngML evolui o deploy.
- **LangGraph:** encaixa como **camada de orquestracao de estados** (ramificacoes, fallbacks, retries) **acima** do motor, quando houver necessidade de fluxos multi-etapas ou integracoes assincronas -- **nao** como substituto imediato dos gatilhos SQL nem como pre-requisito para o piloto Hepato.
- **MCP:** util como **padrao de conectores** para ferramentas e contextos quando surgirem agentes ou servicos que precisem invocar catalogo, validadores ou APIs internas com governanca; **nao** substitui contrato de dados, LGPD nem o nucleo `TextPipeline` / `ClinicalNlpEngine`.

**Escopo editorial (inalterado):** foco em **NLP**; **RAG clinico permanece fora do escopo** enquanto nao houver base confiavel de protocolos/diretrizes.

---

## 4) Arquitetura recomendada (v0 -> v0.2 refinada)

**Evolucao arquitetural:** a arquitetura v0 de engines plugaveis evolui para **Shared Encoder + Multi-Task Learning** como alvo da **Fase 3** (fundamentacao cientifica em `visao-refinada-motor-nlp-unificado-v0.md`). As **Fases 1-2 usam apenas engines rule-based + embeddings existentes (CPU, sem GPU)**.

## 4.1 Nucleo (reutilizavel)

- `TextPipeline`
  - limpeza HTML/RTF
  - normalizacao unicode/acentos
  - tokenizacao/segmentacao
  - deteccao de negacao avancada (23 expressoes, janela 7 tokens)
- `SharedEncoder` (Fase 3 -- transformer fine-tuned)
  - BERTimbau (`neuralmind/bert-base-portuguese-cased`) com continued pre-training em laudos clinicos PT-BR
  - ~110M params (modelo base, proporcional ao problema)
  - **Nota:** nao entra nas Fases 1-2; MVP usa engines rule-based
- `ClinicalNlpEngine` (interface)
  - `process_batch(input_df, config, context) -> output_df`
  - Implementations: SharedEncoderEngine (primario), RuleBasedFallback (seguranca)
- `ScoringCore`
  - regras de priorizacao
  - pesos por especialidade
- `QualityGuard`
  - validacao estrutural de output
  - campos obrigatorios e thresholds minimos

## 4.2 Specialty Heads (por tarefa)

- **HeadBinaryRelevance** (hepato, biliar, neuroimuno, reumato, colon, DII, pulmao) — sigmoid
- **HeadCategoryExtraction** (birads) — softmax + regex residual
- **RuleBasedFallback** (todos) — engine deterministico como fallback de seguranca
- **BusinessRulesEngine** (rim) — calculo numerico, fora do escopo do encoder

## 4.3 Treinamento (pipeline em 4 etapas)

1. Continued Pre-Training (MLM em laudos nao-rotulados)
2. Weak Supervision (outputs atuais como labeling functions)
3. Fine-Tuning supervisionado (lotes validados pelo corpo medico)
4. Validacao comparativa (encoder vs regras vs medico)

Detalhamento completo: `visao-refinada-motor-nlp-unificado-v0.md`, secoes 3 e 6.

## 4.4 Adaptadores (fora do nucleo NLP)

- ingestao (batch/hospital/arquivos),
- persistencia Delta,
- exportacao operacional,
- API/CRM/FTP,
- monitoramento e auditoria.

## 4.5 Papel de orquestracao avancada (LangGraph e alternativas)

**Orquestracao atual:** SQL + Jobs Databricks (pipeline linear agendada). Funciona bem para batch NLP e **permanece como padrao**. Nao ha reescrita de orquestracao.

**Quando orquestracao avancada faz sentido (Fase 4+):**

- controlar fluxo com ramificacoes (ex.: "regra deterministica resolveu?" -> se nao, aciona LLM fallback),
- registrar trilha de decisao por etapa (auditabilidade),
- implementar retries/timeouts/circuit breaker em chamadas externas,
- padronizar pipeline multi-etapas com checkpoints.

**Opcoes equivalentes (nenhuma e pre-requisito):**

| Ferramenta | Quando considerar | Vantagem |
|-----------|-------------------|----------|
| **LangGraph** | Fluxos com LLM, routing condicional, agentes | Nativo para orchestrar agentes/LLM |
| **Prefect** | Pipelines de dados com monitoramento avancado | Boa integracao com Databricks |
| **Airflow** | Orquestracao generica de DAGs | Maduro, ampla comunidade |
| **Databricks Workflows** | Jobs nativos com dependencias | Ja disponivel, zero infra adicional |

Uso nao recomendado agora:

- reescrever todo o pipeline apenas para "usar ferramenta X",
- acoplar regras clinicas diretamente no grafo sem passar por engine/config.

---

## 4.6 Papel do MCP (Model Context Protocol)

**MCP pode ser usado como camada de integracao segura de ferramentas/contextos** para agentes e servicos de IA.

No nosso caso, faz sentido para:

- expor conectores controlados (catalogo de tabelas, servicos internos, validadores) com autorizacao e trilha,
- separar "quem chama" da implementacao do conector (padrao unico para ferramentas),
- reduzir acoplamento entre motor e integracoes auxiliares.

Limite pratico:

- MCP **nao substitui** contrato de dados, governanca clinica ou arquitetura do motor;
- deve ser considerado como camada complementar de integracao, principalmente quando houver agentes/autonomia operacional.

---

## 5) Deep Learning, LLM e ML - como usar com criterio

## 5.1 LLM

**Papel recomendado:** extracao estruturada e desambiguacao de casos complexos.  
**Nao recomendado:** depender de LLM para 100% da triagem sem fallback deterministico.

Guardrails obrigatorios:

- anonimizar dados sensiveis antes de chamadas externas,
- exigir formato estruturado (JSON schema/Pydantic),
- manter trilha de prompts/versoes/resultado,
- fallback para regras quando output vier inconclusivo.

## 5.2 Deep learning semantico (embeddings)

**Papel recomendado:** detecao semantica e matching com criterios clinicos internos ja validados no produto.  
**Quando entra forte:** depois que o pipeline baseline estiver estabilizado.

## 5.3 ML supervisionado

**Papel recomendado:** ranking final (probabilidade de caso de alta complexidade).  
Prerequisitos:

- rotulo confiavel,
- conjunto de validacao,
- monitoramento de drift,
- versionamento formal de artefatos.

## 5.4 Unificar para replicar, consolidar e evoluir

Para "extrair o melhor de todos os mundos", a estrategia de unificacao recomendada e:

1. **Replicar o que funciona hoje** (padrao operacional):
   - runtime Databricks padrao,
   - contrato de entrada/saida comum,
   - monitoramento minimo em todos os projetos.
2. **Consolidar no motor comum**:
   - funcoes de limpeza e normalizacao de texto,
   - interface unica de engine,
   - configuracao por especialidade em YAML/JSON.
3. **Evoluir por trilhas tecnicas**:
   - trilha regras/regex,
   - trilha embeddings+regras,
   - trilha supervisionada (quando houver rotulo),
   - fallback LLM com guardrails.

O que **pode** unificar:

- pipeline tecnico de texto, scoring base, auditoria, observabilidade e governanca.

O que **nao pode** unificar (deve permanecer por dominio):

- dicionarios clinicos, gatilhos, taxonomias e regras de encaminhamento por especialidade.

---

## 6) Stack por fase (pragmatica)

## Fase 1 -- MVP rule-based (CPU)

- Python + Spark + Delta
- spaCy/NLTK/regex (sem embeddings, sem GPU, sem LLM)
- config YAML por especialidade
- scoring rule-based
- metricas de qualidade basicas
- canal de saida: Excel/SharePoint inalterado (motor alimenta Delta, serving existente consome)

## Fase 2 -- NLP avancado (CPU)

- biblioteca interna versionada (semver)
- testes automatizados no nucleo (pytest)
- runtime padronizado (reduzir pip ad hoc)
- trilha de auditoria e metadata de execucao
- embeddings existentes (MiniLM) como componente opcional de `semantic_expand`

## Fase 3 -- Encoder (GPU)

- BERTimbau com continued pre-training + specialty heads
- MLflow para tracking e model registry
- weak supervision + fine-tuning supervisionado

## Fase 4 -- Excelencia (GPU + API LLM)

- LLM para casos inconclusivos (fallback seletivo)
- aprofundar robustez de extracao/score sem dependencia de base RAG
- opcional supervisionado para ranking avancado
- orquestracao avancada (LangGraph, Prefect ou equivalente) se necessario

---

## 7) Criterios de decisao (checklist tech lead)

Para cada nova camada/tecnologia, avaliar:

- **Privacidade/LGPD:** dado sai do ambiente controlado?
- **Precisao clinica:** ganho real versus baseline?
- **Custo total:** token, GPU, manutencao, observabilidade.
- **Latencia/throughput:** suporta lote diario sem gargalo?
- **Auditabilidade:** decisao rastreavel por versao?
- **Operabilidade:** equipe consegue sustentar em producao?

Se a resposta for "nao" em 2 ou mais criterios, adiar para proxima fase.

---

## 8) Riscos especificos e mitigacoes

- **Alucinacao/erro LLM** -> usar schema estrito + validacao + fallback.
- **Drift de regras por especialidade** -> config versionada + PR review clinico.
- **Divergencia de ambientes** -> padronizacao de runtime/imagem.
- **Legado paralelo** -> politica de deprecacao com data de corte.
- **Aumento de custo sem ganho** -> gate por metricas antes de escalar.

---

## 9) Decisoes que destravam a execucao

1. Definir piloto do motor (**prioridade alinhada ao mapa e à liderança: Hepatologia** legado vs motor novo; alternativas na fase 0).
2. Definir fronteira do MVP (sem LLM ou LLM apenas em fallback).
3. Definir repositorio da biblioteca comum.
4. Definir formato oficial de configuracao por especialidade.
5. Definir metricas minimas de sucesso tecnico e de negocio.

---

## 10) Recomendacao final v0

Para este contexto, a melhor estrategia e:

- **motor NLP comum com engines plugaveis** (evolucao controlada, nao substituicao),
- **motor NLP de triagem priorizada** (nao apenas pagina de captura),
- **evolucao por fases com governanca pragmatica** (DS propoe, medico valida, deploy versionado).

Em resumo: primeiro consolidar base reutilizavel e qualidade, depois expandir com LLM e ML supervisionado onde houver ganho comprovado.  
**Nota de escopo atual:** NLP (nao LP) e sem RAG por ausencia de documentos de protocolo/diretriz disponiveis.

---

## 11) Plano aprofundado de estudo tecnico (pre-plano de acao e historias)

**Contexto definido:** estamos em fase de estudo/aprofundamento tecnico para consolidar plano de acao e backlog de historias, com baseline de compliance **LGPD corporativo**.

### 11.1 Objetivo desta etapa

Produzir, de forma verificavel:

- arquitetura alvo validada (motor + frame + integracoes),
- criterios tecnicos e clinicos de aceite,
- mapa de migracao por especialidade,
- backlog inicial priorizado (epicos/historias/tarefas),
- plano de evolucao sem big bang.

### 11.2 Principios de engenharia e dados (nao negociaveis)

- **Clean Architecture:** dominio isolado de infraestrutura e canais.
- **Clean Code:** nomes claros, baixa complexidade ciclomática, funcoes pequenas, comentarios somente quando agregam contexto.
- **DRY:** eliminar duplicacoes de limpeza textual, scoring base e runtime.
- **SOLID pragmático:** abstrair somente variacoes reais (engines/config/adapters).
- **Data contracts first:** schema e semantica versionados antes de escalar.
- **Reprodutibilidade:** controle de versao de codigo, config, dependencias e artefatos.
- **Observabilidade by design:** logs, metricas e auditoria desde o inicio.
- **LGPD by default:** minimizacao de dado, mascaramento e rastreabilidade.

### 11.3 Workstreams (frentes paralelas)

1. **Arquitetura de software**
   - contrato `ClinicalNlpEngine`;
   - contrato `TextPipeline`;
   - padrao de adapters (Delta, API, FTP, export);
   - governance de versao (`engine_version`, `config_version`).
2. **Ciencia de dados e estatistica**
   - desenho do score (rule-based baseline + calibracao);
   - plano de avaliacao (precisao, recall, FPR, drift);
   - criterios para ligar trilha supervisionada.
3. **NLP/LLM**
   - baseline deterministico por especialidade;
   - fallback LLM para casos inconclusivos;
   - schema estrito de resposta e validacao.
4. **Plataforma Databricks/Azure**
   - padrao de jobs, widgets, catalog/schema;
   - dependencia/runtime (imagem ou init scripts);
   - CI/CD com gates minimos.
5. **Aplicacao medica e operacao**
   - criterios de prioridade clinica com dono funcional;
   - regras de encaminhamento por especialidade;
   - circuito de feedback clinico-operacional.

### 11.4 Arquitetura alvo (detalhamento pratico)

**Camada Domain**

- `TextPipeline`:
  - limpeza de entrada (HTML/RTF/layout noise),
  - normalizacao linguistica,
  - segmentacao,
  - negacao/contexto.
- `ClinicalNlpEngine`:
  - interface unica de processamento em lote,
  - engines: regras, embeddings+regras, pattern extraction, supervisionado.
- `ScoringCore`:
  - score de urgencia/complexidade com pesos configuraveis.

**Camada Application**

- orquestracao do caso de uso (`run_specialty`):
  - carrega config,
  - executa pipeline,
  - gera output estruturado,
  - dispara adaptadores.

**Camada Infrastructure**

- adapters de entrada e persistencia Delta,
- adapters de saida (API/FTP/Excel),
- monitoramento e auditoria.

### 11.5 Estrategia de unificacao (replicar -> consolidar -> evoluir)

**Replicar (padrao minimo)**

- padrao runtime Databricks,
- padrao naming/tabelas por camada,
- padrao de logs e metadados.

**Consolidar (motor comum)**

- funcoes de text preprocessing comuns,
- contrato unico de engine,
- config por especialidade.

**Evoluir (capacidade)**

- expandir embeddings/semantica,
- adicionar trilha supervisionada por evidencias,
- LLM fallback com guardrails.

### 11.6 O que sincronizar vs o que preservar por dominio

**Sincronizar obrigatoriamente**

- contratos I/O e metadados;
- padrao de runtime/deploy;
- componentes comuns de texto e observabilidade;
- politica de deprecacao legado.

**Preservar por especialidade**

- taxonomias, dicionarios, gatilhos clinicos;
- regras de navegacao/encaminhamento;
- thresholds clinicos aprovados pelo dominio.

### 11.7 Estatistica e validacao de modelo (aplicada ao caso)

Para cada especialidade, definir:

- conjunto de avaliacao (holdout ou janela temporal),
- metricas principais:
  - recall para casos criticos,
  - precision para reduzir falso positivo operacional,
  - FNR em categorias de maior risco clinico;
- avaliacao de calibracao de score (quando probabilistico),
- monitoramento de drift de termos e classes.

**Regra de decisao para evoluir para ML supervisionado:**

- somente com rotulo confiavel e volume suficiente;
- baseline rule-based documentado;
- melhoria estatisticamente relevante versus baseline.

### 11.8 Padrao de historias (template para backlog)

**Epic:** Unificar motor NLP por especialidade  
**Story:** Como time de dados, quero externalizar configuracao clinica para YAML por especialidade para reduzir alteracoes em codigo.  
**Critérios de aceite:**

1. Config valida por schema;
2. Pipeline roda com config sem alteracao de codigo;
3. Output preserva contrato;
4. Testes de regressao passam.

**Tarefas tecnicas:**

- criar schema da config,
- migrar regras atuais,
- implementar loader com validacao,
- criar testes de regressao com fixtures.

### 11.9 Definicao de pronto (DoD) por entrega tecnica

Uma historia so fecha quando:

- codigo + testes automatizados + documentacao minima;
- contrato de dados atualizado (se impactado);
- metricas de qualidade registradas;
- review tecnico e validação funcional concluídos;
- sem regressao em baseline da especialidade.

### 11.10 Plano de riscos e mitigacao (refinado)

- **Risco:** divergencia de runtime e dependencias  
  **Mitigacao:** baseline de versao e matriz de compatibilidade.
- **Risco:** regressao clinica por unificacao agressiva  
  **Mitigacao:** rollout por especialidade com paridade.
- **Risco:** acoplamento de regras ao codigo  
  **Mitigacao:** config versionada + schema + review funcional.
- **Risco:** custo LLM sem ganho real  
  **Mitigacao:** fallback seletivo + gate por metricas de valor.

### 11.11 Entregaveis da etapa de estudo (saida obrigatoria)

1. **Blueprint tecnico v1** (arquitetura e contratos);
2. **Matriz de migracao** por especialidade (esforco, risco, dependencia);
3. **Backlog inicial** de epicos e historias priorizadas;
4. **Plano de validacao estatistica** por especialidade;
5. **Plano de observabilidade e LGPD** operacional.

### 11.12 Cronograma de estudo sugerido (sem data fechada)

- **Sprint S0 (governanca):** alinhamento de principios, escopo, criterios de sucesso.
- **Sprint S1 (arquitetura+contratos):** RFC motor, contratos I/O, padrao de config.
- **Sprint S2 (prova tecnica):** spike com uma especialidade piloto.
- **Sprint S3 (consolidacao):** backlog final priorizado e plano de execucao.

---

