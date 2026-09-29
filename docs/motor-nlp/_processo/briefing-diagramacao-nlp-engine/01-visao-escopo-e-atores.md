---
titulo: Visão, escopo e atores da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - README.md · 0.13.0
  - RELEASE.md · 0.13.0
  - docs/spec-0.10.0 a spec-0.13.0
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha o bloco "o que a solução é", o bloco "fora de escopo — declarado"
  e a faixa de atores da prancha 1, sem precisar perguntar o que a lib faz.
relacionado:
  - 02-arquitetura-e-camadas.md
  - 06-contrato-de-api.md
  - 09-codigos-decisoes-e-intencao.md
---

# Visão, escopo e atores

> **O que este arquivo é:** a pergunta de negócio que a lib responde, o recorte do que ela faz e não faz, e quem a chama.
> **O que ele não é:** como ela faz — isso é o `02` e o `04`; nem o que ela devolve — isso é o `06`.

## A pergunta de negócio, em cinco linhas

Um laudo de exame é texto livre escrito por quem laudou. A rede produz dezenas de milhares por dia.
A navegação clínica precisa saber **quais deles trazem um achado que muda a conduta**, para
capturar o paciente na linha de cuidado certa, no momento em que ainda há decisão a acelerar.

A `nlp-engine` responde **uma pergunta por laudo: este laudo é relevante para esta linha de
cuidado, e com que evidência?** Ela devolve a decisão, o grau de confiança e a trilha que permite
auditar como chegou lá.

Ela **não** decide o que é relevante: quem decide é a **configuração da especialidade**, escrita
pelo cientista de dados junto com o negócio. A lib é o motor que executa essa régua de forma
igual, repetível e auditável em todas as linhas.

## Escopo positivo

| o que entrega | onde aparece na saída |
|---|---|
| decisão binária de relevância por laudo | `fl_relevante` |
| grau de confiança calibrado em `[0, 1]` | `confidence_score` |
| os achados reconhecidos, com o termo que casou e a posição no texto | `findings`, `findings_match`, `findings_spans` |
| o texto tratado que a régua de fato leu | `exm_laudo_texto_tratado` |
| a trilha de decisão completa, etapa a etapa | `exm_laudo_resultado` (30 campos) |
| a versão do motor e da régua que produziram a decisão | `engine_version`, `config_version` |
| métricas de qualidade e de dado de entrada | subpacote `monitoring` |

## Escopo negativo — declarado, não esquecido

Esta é a parte que mais importa para a prancha 1: **quase tudo que se espera de um "motor de NLP"
é deliberadamente de fora.**

| fora_de_escopo | por_que | quem_decidiu |
|---|---|---|
| **Ler configuração de arquivo** | a lib recebe `dict`; quem lê YAML, notebook ou tabela é o chamador. Mantém a lib testável sem ambiente | arquitetura declarada |
| **Descobrir conexão ou credencial** | não usa o `databricks-sdk`; `base_url` e chave vêm da config ou do ambiente. Foi o que permitiu diagnosticar o `403` como divergência de workspace, e não como bug da lib | `D4` no `09` |
| **Persistir qualquer coisa** | não escreve tabela, não cria catálogo, não conhece Spark. Devolve `list[dict]` e acabou | arquitetura declarada |
| **Selecionar quais laudos processar** | o `gold_filter` vive na configuração da especialidade e é aplicado **antes**, pelo runner. A lib recebe o lote pronto | fronteira com a plataforma |
| **Agendar, reprocessar, deduplicar** | é do job da plataforma. A lib é síncrona e sem estado entre chamadas | fronteira com a plataforma |
| **Entregar ao negócio** | a lib decide; quem monta arquivo, aplica a view e envia é a camada de exchange | fronteira com a plataforma |
| **Streaming em `process()`** | quebraria a API pública, e a plataforma já contorna montando lotes externamente. Sai para bump próprio | `D2` no `09` |
| **Decidir sem evidência de régua** | invariante: o juiz LLM **filtra** o que a régua marcou, nunca cria relevância do nada | `D9` no `09` · ⚠️ ver `R7` |

⚠️ **Duas fronteiras que costumam ser atribuídas à lib e não são dela:** o filtro de entrada
(`gold_filter`, aplicado pelo runner sobre a descrição do procedimento) e o que chega à tabela de
saída (a plataforma decide quais campos viram coluna). Desenhar qualquer um dos dois como bloco da
lib está errado.

## Atores e consumidores

| ator | sistema | o_que_consome | modo |
|---|---|---|---|
| **Runner de produção** | `fabrica-ia-nlp-platform` — notebook `ntb_ia_motor_e2e` | `ClinicalNlpEngine.process()` sobre o lote do dia | lote |
| **Monitoramento** | notebook da plataforma | `nlp_engine.monitoring.runner` — volumetria, PSI, schema, baselines | lote |
| **Cientista de dados** | bancada local e notebook de dev | a mesma API, com configuração em iteração | síncrono |
| **Esteira de release** | Azure Pipelines | o wheel publicado em dois feeds, por ambiente | assíncrono |
| **Endpoint de LLM** | Databricks Model Serving | ⚠️ é ator **de saída**: a lib chama, não é chamada | síncrono |

ℹ️ **Seis linhas de cuidado em produção** consomem a lib através do runner: hepatologia,
reumatologia, `cancer_rim`, TI-RADS, `cancer_estomago` e `transplante_pulmao`. Cada uma com sua
própria configuração e sua própria régua; **o motor é o mesmo binário para todas.**

## O que a versão desta descrição significa

`0.13.0` é uma versão de **estrutura**: nenhum laudo muda de decisão em relação à `0.12.3`, e isso
foi provado — 2.920 linhas byte a byte, `sha256` idêntico (ver `05`). Para efeito de prancha, o
comportamento desenhado aqui vale igualmente para a `0.12.3`, que é a que roda em produção hoje.
