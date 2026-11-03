---
tags: [mapa, repositorio]
---

# Mapa — repositório `fabrica-ia-nlp-platform`

↑ [[mapa-projetos]]

> Repositório oficial (Azure DevOps): este mapa fica **fora** dele, para não criar arquivo no repositório. Lista todas as notas, agrupadas por pasta.

## `.`

- [[fabrica-ia-nlp-platform/README|README]] — fabrica-ia-nlp-platform

## `docs`

- [[fabrica-ia-nlp-platform/docs/TODO|TODO]] — TODO — Segurança, Performance e Débitos Técnicos (fabrica-ia-nlp-platform)

## `docs/e2e`

- [[fabrica-ia-nlp-platform/docs/e2e/01-arquitetura-e-operacao|01-arquitetura-e-operacao]] — Motor NLP E2E — Arquitetura, Performance e Operação
- [[fabrica-ia-nlp-platform/docs/e2e/02-alteracoes-refatoracao|02-alteracoes-refatoracao]] — Refatoração do Motor NLP E2E — Registro de Alterações
- [[fabrica-ia-nlp-platform/docs/e2e/03-flag-reprocessamento|03-flag-reprocessamento]] — Flag de reprocessamento (reprocess_enable) — apenas dev

## `docs/example`

- [[fabrica-ia-nlp-platform/docs/example/README|README]] — Projeto de IA — Fábrica de IA

## `docs/usage`

- [[fabrica-ia-nlp-platform/docs/usage/README|README]] — Guia de uso — fabrica-ia-nlp-platform

## `docs/usage/boas-praticas`

- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/01-fundamentos-e-limites|01-fundamentos-e-limites]] — 01 · Fundamentos e limites da sua alçada
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/02-criando-uma-nova-especialidade|02-criando-uma-nova-especialidade]] — 02 · Criando uma nova especialidade
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/03-calibracao-do-lexico-e-regras|03-calibracao-do-lexico-e-regras]] — 03 · Calibração do léxico e das regras
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/04-selecao-de-dados-na-gold|04-selecao-de-dados-na-gold]] — 04 · Seleção de dados na Gold
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/05-llm-embeddings-e-custo|05-llm-embeddings-e-custo]] — 05 · LLM, embeddings e custo
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/06-configurando-o-json-do-job|06-configurando-o-json-do-job]] — 06 · Configurando o JSON do job
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/07-ambientes-execucao-e-promocao|07-ambientes-execucao-e-promocao]] — 07 · Ambientes, execução e promoção
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/08-observabilidade-e-diagnostico|08-observabilidade-e-diagnostico]] — 08 · Observabilidade e diagnóstico
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/09-checklist-e-antipadroes|09-checklist-e-antipadroes]] — 09 · Checklist e antipadrões
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/10-arquivo-de-navegacao|10-arquivo-de-navegacao]] — 10 · Construindo o arquivo de navegação
- [[fabrica-ia-nlp-platform/docs/usage/boas-praticas/README|README]] — Boas práticas para o cientista de dados

## `docs/usage/specs`

- [[fabrica-ia-nlp-platform/docs/usage/specs/00-visao-geral-arquitetura|00-visao-geral-arquitetura]] — 00 · Visão geral e arquitetura
- [[fabrica-ia-nlp-platform/docs/usage/specs/01-runner-motor-e2e|01-runner-motor-e2e]] — 01 · Runner ntb_ia_motor_e2e
- [[fabrica-ia-nlp-platform/docs/usage/specs/02-config-environment|02-config-environment]] — 02 · EnvironmentConfig / EnvType
- [[fabrica-ia-nlp-platform/docs/usage/specs/03-config-loader|03-config-loader]] — 03 · ConfigLoader
- [[fabrica-ia-nlp-platform/docs/usage/specs/04-config-dependencies|04-config-dependencies]] — 04 · DependencyInstaller
- [[fabrica-ia-nlp-platform/docs/usage/specs/05-config-widgets|05-config-widgets]] — 05 · WidgetConfigLoader / PipelineConfigPayload
- [[fabrica-ia-nlp-platform/docs/usage/specs/06-validations-date|06-validations-date]] — 06 · PipelineDatePayload / PipelineDateProcessor
- [[fabrica-ia-nlp-platform/docs/usage/specs/07-observability-logging|07-observability-logging]] — 07 · Logger / BrasilFormatter
- [[fabrica-ia-nlp-platform/docs/usage/specs/08-observability-ml-run|08-observability-ml-run]] — 08 · MLflowRunConfigurator
- [[fabrica-ia-nlp-platform/docs/usage/specs/09-observability-ml-telemetry|09-observability-ml-telemetry]] — 09 · MLflowTelemetry
- [[fabrica-ia-nlp-platform/docs/usage/specs/10-observability-monitoring|10-observability-monitoring]] — 10 · MonitoringCalculator
- [[fabrica-ia-nlp-platform/docs/usage/specs/11-data-gold-filters|11-data-gold-filters]] — 11 · GoldFilterBuilder
- [[fabrica-ia-nlp-platform/docs/usage/specs/12-data-input-mapper|12-data-input-mapper]] — 12 · InputMapper
- [[fabrica-ia-nlp-platform/docs/usage/specs/13-data-engine-runner|13-data-engine-runner]] — 13 · EngineRunner
- [[fabrica-ia-nlp-platform/docs/usage/specs/14-data-export-view|14-data-export-view]] — 14 · InferenceExportView / JoinSpec
- [[fabrica-ia-nlp-platform/docs/usage/specs/15-pipeline-00-load-control|15-pipeline-00-load-control]] — 15 · Estágio 00 · LoadControlTracker
- [[fabrica-ia-nlp-platform/docs/usage/specs/16-pipeline-01-config|16-pipeline-01-config]] — 16 · Estágio 01 · NLPConfigProcessor
- [[fabrica-ia-nlp-platform/docs/usage/specs/17-pipeline-02-input|17-pipeline-02-input]] — 17 · Estágio 02 · InputDataProcessor
- [[fabrica-ia-nlp-platform/docs/usage/specs/18-pipeline-03-persisters|18-pipeline-03-persisters]] — 18 · Estágio 03 · DataPersistenceManager
- [[fabrica-ia-nlp-platform/docs/usage/specs/19-pipeline-04-process|19-pipeline-04-process]] — 19 · Estágio 04 · NLPProcessor
- [[fabrica-ia-nlp-platform/docs/usage/specs/20-pipeline-05-output|20-pipeline-05-output]] — 20 · Estágio 05 · OutputPersistenceManager
- [[fabrica-ia-nlp-platform/docs/usage/specs/21-pipeline-06-monitoring|21-pipeline-06-monitoring]] — 21 · Estágio 06 · MonitoringManager
- [[fabrica-ia-nlp-platform/docs/usage/specs/22-pipeline-07-view|22-pipeline-07-view]] — 22 · Estágio 07 · ExportViewManager
- [[fabrica-ia-nlp-platform/docs/usage/specs/23-tools-data-manager|23-tools-data-manager]] — 23 · GoldDataManager (data_manager)
- [[fabrica-ia-nlp-platform/docs/usage/specs/24-tools-data-persister|24-tools-data-persister]] — 24 · DataPersister
- [[fabrica-ia-nlp-platform/docs/usage/specs/25-tools-data-exchange|25-tools-data-exchange]] — 25 · InferenceIngestor (data exchange)
- [[fabrica-ia-nlp-platform/docs/usage/specs/26-controle-de-carga|26-controle-de-carga]] — 26 · Controle de carga (ntb_ia_controle_carga)
- [[fabrica-ia-nlp-platform/docs/usage/specs/27-config-especialidade|27-config-especialidade]] — 27 · Config de especialidade (CONFIG)
- [[fabrica-ia-nlp-platform/docs/usage/specs/28-config-organs-shared|28-config-organs-shared]] — 28 · Léxico compartilhado (ORGANS_SHARED)
- [[fabrica-ia-nlp-platform/docs/usage/specs/29-jobs-e-recipe-devops|29-jobs-e-recipe-devops]] — 29 · Recipe de jobs e CI/CD
- [[fabrica-ia-nlp-platform/docs/usage/specs/30-trilha-operacao-e-qualidade|30-trilha-operacao-e-qualidade]] — 30 · Trilha de operação corporativa e gate de qualidade
- [[fabrica-ia-nlp-platform/docs/usage/specs/README|README]] — Especificações dos componentes — fabrica-ia-nlp-platform

## `jobs`

- [[fabrica-ia-nlp-platform/jobs/exemplo|exemplo]] — Exemplo de referência — job multi-task com todos os campos
- [[fabrica-ia-nlp-platform/jobs/README|README]] — jobs/definicoes/ — Jobs do motor NLP por especialidade

## `plataform`

- [[fabrica-ia-nlp-platform/plataform/README|README]] — Motor NLP E2E — Fábrica de IA

## `plataform/config/exchange`

- [[fabrica-ia-nlp-platform/plataform/config/exchange/README|README]]

## `plataform/config/shared`

- [[fabrica-ia-nlp-platform/plataform/config/shared/README|README]]

## `plataform/config/speciality`

- [[fabrica-ia-nlp-platform/plataform/config/speciality/README|README]]

## `plataform/tools`

- [[fabrica-ia-nlp-platform/plataform/tools/README|README]]
