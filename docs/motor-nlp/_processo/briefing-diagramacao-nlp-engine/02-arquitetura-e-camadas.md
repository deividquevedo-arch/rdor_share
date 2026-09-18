---
titulo: Arquitetura e camadas de decisão da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - tests/api_surface.json · 0.13.0
  - docs/spec-0.11.0 · falha de infra não é negativa clínica
  - docs/spec-0.12.2 · âncora ausente não é "não se aplica"
  - RELEASE.md · 0.13.0
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha as faixas da prancha 1 e o encadeamento da prancha 2 — sabendo,
  para cada etapa, o que ela decide, o que a desliga e como ela degrada quando falha.
relacionado:
  - 04-cargas-transformacoes-e-regras.md
  - 07-sequencia-e-degradacao.md
  - 09-codigos-decisoes-e-intencao.md
---

# Arquitetura e camadas de decisão

> **O que este arquivo é:** as etapas pelas quais um laudo passa, em ordem, com o que liga e desliga cada uma.
> **O que ele não é:** o detalhe de cada transformação — isso é o `04`; nem a sequência temporal de uma chamada — isso é o `07`.

## 🔴 Desvio de unidade, declarado

**Não existem camadas de dado aqui.** A `nlp-engine` não tem bronze, silver, gold ou diamond — não
lê catálogo, não escreve tabela, não persiste. A "camada" desta solução é a **etapa de decisão**: o
laudo atravessa uma sequência de julgamentos, e cada um pode confirmar, rebaixar ou promover.

As colunas abaixo são as do template, com o conteúdo redefinido: `camada` é a etapa, `cadencia` é
quando ela roda, `reconstruivel_de` é o que a reproduz.

## As etapas, em ordem

| camada | catalogo | schema | o_que_responde | cadencia | reconstruivel_de |
|---|---|---|---|---|---|
| **0 · tratamento de texto** | n/a | `text_pipeline` | "o que a régua vai de fato ler?" | todo laudo | `exm_laudo_texto` + a config de boilerplate |
| **1 · segmentação** | n/a | `text_pipeline.by_headers` | "o laudo inteiro, ou só as seções que importam?" | todo laudo | `segmentation.mode` |
| **2 · régua léxica** | n/a | `rule_engine` | "há termo de achado, não negado, no órgão-alvo?" | todo laudo | `findings`, `organs`, `negation` |
| **3 · expansão semântica** | n/a | `semantic_expand` | "há trecho *parecido* com um achado, acima do limiar?" | quando `use_embeddings` | `embeddings.*` |
| **4 · critérios quantitativos** | n/a | `quantitative` | "a medida satisfaz o limiar? o gate libera?" | quando há `quantitative` | `quantitative.<criterio>` |
| **5 · extração ordinal** | n/a | `ordinal_extraction` | "qual a categoria RADS, e ela promove?" | quando há `ordinal_extraction` | `systems.<x>` |
| **6 · escore calibrado** | n/a | `scoring` | "qual a confiança composta desta decisão?" | todo laudo | pesos da `score_policy` |
| **7 · juiz LLM** | n/a | `llm_router_backend` | "o achado que a régua marcou se sustenta no texto?" | só dentro da `uncertainty_band` | `llm_router.*` |
| **8 · invariantes de saída** | n/a | `output_invariants` | "a linha devolvida respeita o contrato?" | todo laudo | `contracts.py` |
| **9 · observabilidade** | n/a | `monitoring` | "a entrada e o resultado estão dentro do esperado?" | por execução | `monitoring.runner` |

ℹ️ **Etapa que não existe numa linha é declarada como inexistente, não omitida.** Uma
configuração `rule_only` executa 0, 1, 2, 6 e 8; as demais ficam registradas na trilha como não
aplicadas — e é assim que a auditoria distingue "não achou" de "não rodou".

## O que liga, o que desliga e como degrada

| camada | o_que_a_liga | o_que_a_desliga | como_degrada | codigo |
|---|---|---|---|---|
| 3 · semântica | `embeddings.use_embeddings: True` | ausência da chave | ⚠️ **modelo inválido cai em `token_overlap` sem erro** — o perfil "híbrido" medido não é híbrido | `R3` |
| 4 · quantitativa | bloco `quantitative` na config | ausência do bloco | erro de LLM na extração **não rebaixa** desde a `0.11.0` | `D5` |
| 5 · ordinal | `ordinal_extraction.enabled: True` | ausência ou `False` | ⚠️ valor de chave desconhecido deixa o bloco **inerte em silêncio** | `R6` |
| 7 · juiz | `llm_router.enabled: True` **e** score dentro da banda | ausência de `enabled` → a lib assume `False` | ⚠️ `mode` desconhecido cai em `deterministic`, e o juiz **não é chamado** | `R4` `R6` |
| 7 · juiz | — | — | ⚠️ falha de transporte com `fallback_policy: positive_in_band` **entrega** o laudo | `R2` |

🔴 **Todas as degradações acima são silenciosas por desenho** — a execução termina com sucesso e a
régua sustenta a taxa de relevância, então a monitoria de volumetria não acusa. É o motivo pelo
qual a trilha de decisão (`decision_trail`) existe e é gravada laudo a laudo.

## Origens

| origem | sistema | mecanismo | cadencia | volume | fonte |
|---|---|---|---|---|---|
| lote de laudos | runner da plataforma | chamada Python em processo | diária | 200 a 12.000 laudos/dia por linha | `engine_version` das tabelas de saída, 2026-09-15 |
| configuração da especialidade | runner da plataforma | `dict` passado por parâmetro | por execução | 1 por linha de cuidado | `ntb_ia_<especialidade>_config.py` |
| órgãos compartilhados | `ORGANS_SHARED` | `merge_with_shared_organs()` | por execução | 1 | `config_loader` |
| modelo de embeddings | Volume do Unity Catalog | caminho na config, carregado sob demanda | por processo | 1 | ⚠️ `L1` — indisponível localmente |
| endpoint de LLM | Databricks Model Serving | HTTP, `httpx` | por laudo dentro da banda | 7 a 6.111 chamadas/dia por linha | medição de 2026-09-17 |

⚠️ **A variação de volume de chamadas ao juiz é de três ordens de grandeza** entre linhas, e
depende **só da largura da `uncertainty_band`** — não do tamanho do corpus. É o principal fator de
custo do sistema; ver `RNFD` no `07`.

## Rotas de exceção

| rota | por_que_existe | codigo | risco |
|---|---|---|---|
| `runtime.llm_router` sobrepõe `nlp.llm_router` | herança de um mecanismo de widget que não existe mais | `R1` | 🔴 o que a config declara **não é** o que executa, e não há log |
| queda para `token_overlap` | o modelo de embeddings pode não resolver no ambiente | `R3` | 🔴 executa um perfil que nunca foi homologado |
| `fallback_policy: positive_in_band` | manter entrega quando o LLM falha | `R2` | 🔴 **falha de infraestrutura vira entrega clínica** |
| `waive` do gate quantitativo | evidência alternativa dispensa a medida | `D8` | 🟢 opt-in; nenhuma config o declara hoje |
| promoção semântica sem arbitragem | a semântica promove fora da banda do juiz | `R7` | 🔴 entrega sem evidência de régua, por caminho que a banda não alcança |

**Exceção contada é exceção; exceção silenciosa é erosão.** As cinco acima estão na trilha; as
três marcadas 🔴 são as que hoje não têm alarme próprio.
