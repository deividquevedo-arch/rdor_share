---
titulo: Inventário de unidades de configuração da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - src/.../config_loader.py · 0.13.0
  - docs/REFERENCIA-PARAMETROS.md · >= 0.13.0
  - tests/api_surface.json · 0.13.0
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha os blocos da prancha 2 e o domínio da prancha 4 sabendo, para cada
  unidade que a régua declara, o que ela é, em que etapa age e o que sustenta o semáforo.
relacionado:
  - 02-arquitetura-e-camadas.md
  - 04-cargas-transformacoes-e-regras.md
  - 09-codigos-decisoes-e-intencao.md
---

# Inventário de unidades de configuração

> **O que este arquivo é:** o inventário das unidades que a régua clínica declara, com endereço, etapa e evidência.
> **O que ele não é:** a API da biblioteca — isso é o `06`; nem a régua de uma especialidade concreta — cada linha de cuidado tem a sua, e este arquivo descreve o **vocabulário disponível**.

## 🔴 Desvio de unidade, declarado

O template pede `endereco (catalogo.schema.tabela)`. **A lib não tem objeto de dado** — não cria
tabela, não lê catálogo. O que ela tem, e que cumpre exatamente o papel de "objeto" na prancha, é a
**unidade de configuração**: o bloco declarativo que o cientista de dados escreve e que o motor
executa.

A coluna se chama `endereco`, e o endereço aqui é o caminho na configuração: `nlp.findings.ulcera`.
**As oito colunas do template são preenchidas sem renomear nenhuma.**

## Inventário

| endereco | camada | grao | chave | origem_de | situacao | evidencia | codigo |
|---|---|---|---|---|---|---|---|
| `nlp.target_organs` | 2 · régua | uma linha por órgão-alvo da linha de cuidado | nome do órgão | config da especialidade | 🟢 | usado pelas 6 linhas em produção | — |
| `nlp.organs.<orgao>.seeds` | 2 · régua | um termo que ancora o órgão no texto | termo | config + `ORGANS_SHARED` | 🟢 | `merge_with_shared_organs()`, coberto por teste | — |
| `nlp.organs.<orgao>.regex` | 2 · régua | um padrão que ancora o órgão | padrão | config da especialidade | 🟢 | validado no `load()` | — |
| `nlp.findings.<achado>` | 2 · régua | um achado clínico, com seus termos | nome canônico do achado | config da especialidade | 🟢 | `findings_match` e `findings_spans` na saída | — |
| `nlp.negation.phrases` | 2 · régua | uma frase que nega o achado seguinte | frase | config da especialidade | 🟢 | `n_negated_spans` na saída | — |
| `nlp.negation.window` | 2 · régua | alcance em tokens | inteiro | config da especialidade | 🟢 | coberto por teste | — |
| `nlp.negation.direction_default` | 2 · régua | direção do escopo da negação | `left` / `right` / `both` | config da especialidade | 🟡 | ⚠️ declarar `None` **não** cai no default `left` da lib | `R8` |
| `nlp.segmentation.mode` | 1 · segmentação | como o laudo é recortado | `full_doc` / `auto` | config da especialidade | 🟡 | `segmentation_coverage` e `segmentation_dropped_headers` na saída | `R9` |
| `nlp.header_aliases` | 1 · segmentação | um cabeçalho reconhecido | nome da seção | config da especialidade | 🟢 | usado pelo `by_headers` | — |
| `nlp.embeddings.use_embeddings` | 3 · semântica | liga/desliga a etapa | booleano | config da especialidade | 🟢 | `semantic_backend` na saída | — |
| `nlp.embeddings.embedding_model` | 3 · semântica | o modelo a carregar | caminho ou Model do UC | config da especialidade | 🔴 | ⚠️ caminho inválido cai em `token_overlap` **sem erro** | `R3` |
| `nlp.embeddings.similarity_threshold` | 3 · semântica | o corte de similaridade | float `[0,1]` | config da especialidade | 🟢 | `semantic_score` na saída | — |
| `nlp.embeddings.decision_mode` | 3 · semântica | como a semântica compõe com a régua | `hybrid` / … | config da especialidade | 🟡 | ⚠️ `ambiguity_band` é **inerte** em `hybrid` | `R5` |
| `nlp.quantitative.<criterio>` | 4 · quantitativa | um critério mensurável | nome do critério | config da especialidade | 🟢 | bloco `quantitative` na saída, com `met`, `value`, `evidence` | — |
| `nlp.quantitative.<c>.require_measure` | 4 · quantitativa | exige medida para valer | booleano | config da especialidade | 🟡 | ⚠️ âncora ausente já significou "não se aplica" — corrigido na `0.12.2` | `D7` |
| `nlp.ordinal_extraction.enabled` | 5 · ordinal | liga/desliga a etapa | booleano | config da especialidade | 🟢 | coberto por teste de payload byte a byte | — |
| `nlp.ordinal_extraction.systems.<s>` | 5 · ordinal | um sistema de categorias (BI-RADS, TI-RADS…) | chave do sistema | config da especialidade | 🟢 | `ordinal_mentions`, `ordinal_max_by_system` | — |
| `…systems.<s>.relevance_policy.promote_categories` | 5 · ordinal | quais categorias promovem | lista de categorias | config da especialidade | 🟢 | golden: 300 de 365 laudos exercitam | — |
| `…systems.<s>.aggregation_legend_filter` | 5 · ordinal | ignora legenda de rodapé | mapa | config da especialidade | 🟡 | ⚠️ vai **dentro** do sistema, não no topo | `R10` |
| `nlp.llm_router.enabled` | 7 · juiz | liga/desliga o juiz | booleano | config da especialidade | 🔴 | ⚠️ **ausente ⇒ `False`**, mesmo com o bloco inteiro preenchido | `R4` |
| `nlp.llm_router.mode` | 7 · juiz | como o juiz decide | `llm` / `deterministic` | config da especialidade | 🔴 | ⚠️ valor desconhecido cai em `deterministic`, **sem chamar o LLM** | `R6` |
| `nlp.llm_router.uncertainty_band` | 7 · juiz | a faixa de score que vai ao juiz | par de floats | config da especialidade | 🔴 | **é o principal fator de custo** — ver `RNFD` no `07` | `P3` |
| `nlp.llm_router.fallback_policy` | 7 · juiz | o que fazer quando a chamada falha | `keep_current` / `positive_in_band` | config da especialidade | 🔴 | ⚠️ **erro de transporte vira entrega** | `R2` |
| `nlp.llm_router.prompt_system` | 7 · juiz | a instrução dada ao juiz | texto | config da especialidade | 🟡 | o prompt **filtra**, nunca cria relevância | `D9` |
| `nlp.document_vet` | 2 · régua | rebaixa o laudo inteiro por contexto | bloco | config da especialidade | 🟢 | usado no DII: `soft_findings` + `normality_phrases` | — |
| `runtime.llm_router` | 7 · juiz | **sobrepõe** o bloco `nlp` | bloco | config da especialidade | 🔴 | ⚠️ **o que executa não é o que o `nlp` declara**, e não há log | `R1` |

## Colunas críticas da saída

Só as que aparecem em prancha ou em contrato. Fonte: `contracts.py`, `0.13.0`.

| tabela | coluna | tipo | obrigatoria | classificacao_lgpd | preenchimento | fonte |
|---|---|---|---|---|---|---|
| `EngineOutputRow` | `id_exame` | `str` | sim | identificador de exame | 100% | `contracts.py` |
| `EngineOutputRow` | `exm_laudo_texto` | `str` | sim | **texto clínico** — ver `08` | 100% | `contracts.py` |
| `EngineOutputRow` | `exm_laudo_texto_tratado` | `str` | sim | **texto clínico derivado** | ⚠️ pode sair vazio; medido em 77 de 10.000 num run de dev | run de `cancer_rim`, 2026-09-16 |
| `EngineOutputRow` | `fl_relevante` | `int` | sim | decisão | 100% | `contracts.py` |
| `EngineOutputRow` | `confidence_score` | `float` | sim | decisão | 100% | `contracts.py` |
| `EngineOutputRow` | `findings` | `str` | sim | achado clínico | preenchido quando `fl_relevante = 1` | `contracts.py` |
| `EngineOutputRow` | `exm_laudo_resultado` | `str` (JSON) | sim | trilha de auditoria, 30 campos | 100% | `contracts.py` |
| `EngineOutputRow` | `engine_version` · `config_version` | `str` | sim | rastreabilidade | 100% | `contracts.py` |
| `ExmLaudoResultadoPayload` | `llm_prompt_tokens` · `llm_completion_tokens` | `int \| None` | não | custo | ⚠️ **só o caminho do juiz** preenche; a camada quantitativa não | `P3` |
| `ExmLaudoResultadoPayload` | `segmentation_coverage` | `float` | não | qualidade | < 1,0 em 3.867 de 4.507 na hepatologia | medição de 2026-09-08 |

⚠️ **`EngineInputRow` declara 8 campos, e dois são histórico:** `Laudo` (nome herdado do legado) e
`id_unidade`. O contrato de entrada efetivo são `id_exame`, `exm_laudo_texto`, `exm_mod`,
`exm_tipo` e `dt_exame`.

## Subpacote `monitoring` — 6 módulos, sem documentação própria

| endereco | camada | grao | chave | origem_de | situacao | evidencia | codigo |
|---|---|---|---|---|---|---|---|
| `monitoring.runner` | 9 · observabilidade | ponto de entrada chamado pelo notebook da plataforma | — | plataforma | 🟡 | 8 arquivos da plataforma o importam | `L4` |
| `monitoring.data_quality` | 9 | volumetria, PSI e schema da entrada | — | plataforma | 🟡 | 6 arquivos de teste | `L4` |
| `monitoring.baselines` | 9 | persistência de métrica em Delta e comparação com baseline | — | plataforma | 🟡 | — | `L4` |
| `monitoring.quality_guard` | 9 | precisão, recall e F1 | — | plataforma | 🟡 | — | `L4` |
| `monitoring.logging` | 9 | reexporta o logging estruturado do núcleo | — | núcleo | 🟢 | `[P2-11]` | — |

🟡 **Todos em amarelo pelo mesmo motivo:** o subpacote **é consumido** — está na superfície pública
(6 das 35 entradas) e é importado por 8 arquivos da plataforma —, mas **nunca teve documentação
própria**, então não há como atestar que o que ele expõe é o que se pretende que ele exponha. É a
lacuna `L4`, e ela é a única do pacote cuja resolução é escrever, não medir.
