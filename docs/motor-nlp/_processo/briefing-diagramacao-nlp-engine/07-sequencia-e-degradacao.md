---
titulo: Sequência de uma chamada e degradação da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - scripts/medir_custo_por_camada.py · execução de 2026-09-18
  - docs/spec-0.11.0 · falha de infra não é negativa clínica
  - medições de produção, 2026-08 a 2026-09
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha a prancha 3 — atores em ordem, mensagens numeradas, e o que o
  usuário vê em cada cenário de falha — e sabe qual requisito não funcional tem número e qual não.
relacionado:
  - 02-arquitetura-e-camadas.md
  - 06-contrato-de-api.md
  - 09-codigos-decisoes-e-intencao.md
---

# Sequência e degradação

> **O que este arquivo é:** a ordem temporal de uma chamada, o que falha, e o que se vê quando falha.
> **O que ele não é:** o desenho estático das etapas — isso é o `02`.

## Atores, em ordem

1. **Job da plataforma** — agenda, monta a janela e aplica o `gold_filter`
2. **Runner** (`ntb_ia_motor_e2e`) — lê a Gold, monta as linhas e **normaliza a configuração**
3. **`ClinicalNlpEngine`** — a lib
4. **Etapas internas** — texto → segmentação → régua → semântica → quantitativa → ordinal → escore → juiz → invariantes
5. **Endpoint de LLM** (Databricks Model Serving) — ⚠️ ator **de saída**: a lib chama, não é chamada
6. **Volume do Unity Catalog** — de onde o modelo de embeddings é carregado
7. **Persisters** — gravam a saída; **fora da lib**
8. **Monitoramento** (`monitoring.runner`) — roda depois, sobre o que foi gravado

## Caminho feliz

| n | de | para | o_que_pede | o_que_volta | codigo |
|---|---|---|---|---|---|
| 1 | job | runner | executa a janela do dia | — | — |
| 2 | runner | Gold | os laudos da janela, já filtrados | `list[dict]` | — |
| 3 | runner | `config_loader` | `normalize_config(cfg)` | config normalizada, ou `ConfigInvalida` | — |
| 4 | runner | `engine` | `process(rows, cfg, specialty_id, config_version)` | — | — |
| 5 | engine | `text_pipeline` | trate este texto | `exm_laudo_texto_tratado` | — |
| 6 | engine | `rule_engine` | há achado, não negado, no órgão-alvo? | spans positivos e negados | — |
| 7 | engine | Volume | carregue o modelo de embeddings | modelo, **ou `FileNotFoundError`** | `R3` |
| 8 | engine | `semantic_expand` | há trecho parecido acima do limiar? | `semantic_score` e evidência | — |
| 9 | engine | `quantitative` | a medida satisfaz o limiar? | `met`, `value`, `evidence` | — |
| 10 | engine | endpoint de LLM | extraia a medida deste trecho | valor, **ou erro de transporte** | `R2` |
| 11 | engine | `ordinal_extraction` | qual a categoria, e ela promove? | menções e máximo por sistema | — |
| 12 | engine | `scoring` | qual o escore composto? | `confidence_score` | — |
| 13 | engine | endpoint de LLM | **só se dentro da banda** — este achado se sustenta? | veredito e confiança | `R2` `R7` |
| 14 | engine | `output_invariants` | a linha respeita o contrato? | linha validada | — |
| 15 | engine | runner | `list[EngineOutputRow]` | — | — |
| 16 | runner | persisters | grave a saída | — | — |
| 17 | job | `monitoring.runner` | volumetria, PSI, schema e baseline | métricas em Delta | `L4` |

🔴 **A mensagem 13 é a única cuja frequência varia três ordens de grandeza** — de 7 a 6.111
chamadas por dia na mesma lib — e depende **só da `uncertainty_band`**.

## Degradação

| cenario | condicao | resposta | o_que_o_usuario_ve |
|---|---|---|---|
| **Modelo de embeddings ausente** | o caminho não resolve no ambiente | queda para `token_overlap` | 🔴 **nada.** O run fecha em sucesso, a régua sustenta a taxa. Medido em produção: `FileNotFoundError` em **21.530 laudos em 24 h**, quatro linhas — hepatologia 99,3%, `cancer_rim` 98,4%, TI-RADS 86,2%, `cancer_estomago` 100% |
| **Endpoint de LLM indisponível** | `403`, `5xx`, timeout | depende de `fallback_policy` | 🔴 com `positive_in_band`, **o laudo é ENTREGUE pela falha**. Medido: **1.371 laudos entregues** no incidente de 21 e 26/08 |
| **LLM falha na extração de medida** | erro na mensagem 10 | `require_measure` **não rebaixa** desde a `0.11.0` | 🟢 o laudo não é negativado por indisponibilidade |
| **Âncora da medida não reconhecida** | texto não traz o termo esperado | o critério **permanece no gate** desde a `0.12.2` | 🟢 antes disso: **36 de 1.032 entregas** saíam sem conferência |
| **Carga atrasada / lote vazio** | a dedup bloqueia a janela | `ValueError: Nenhum laudo recebido` | 🟡 o run **falha alto** — visível, mas a causa (dedup por `id_exame` sem `config_version`) não é óbvia |
| **Texto zerado pelo tratamento** | HTML de editor, laudo numa linha só | `fl_relevante: 0` com texto tratado vazio | 🔴 **indistinguível de "não achou"**. Medido: **21 de 10.000** num run de dev |
| **Segmentação descarta seções** | `mode: auto` | decide sobre o que sobrou | 🔴 `segmentation_coverage < 1,0` em **3.867 de 4.507** na hepatologia |
| **Configuração inválida** | chave malformada | `ConfigInvalida` antes do primeiro laudo | 🟢 falha cedo e alto |
| **Chave de config não reconhecida** | valor fora do domínio | o bloco fica **inerte** | 🔴 sem erro e sem log — `R6` |

🔴 **Sete dos nove cenários degradam em silêncio**, e o padrão é sempre o mesmo: **a régua sustenta
a taxa de relevância, então a monitoria de volumetria não acusa.** No TI-RADS a taxa ficou
3,17% → 3,21% enquanto **4.703 chamadas de LLM falhavam**. É por isso que a trilha por laudo
existe, e é por isso que `alert_threshold_relevance_drop` não cobre esta classe.

## Fases

| fase | natureza | quem controla |
|---|---|---|
| leitura da Gold e filtro de entrada | **lote, assíncrono** | job da plataforma |
| `process()` sobre o lote | **síncrono**, em memória, sem estado | a lib |
| chamadas ao LLM (mensagens 10 e 13) | **síncrono, por laudo**, dentro do `process()` | a lib, com retry e failover de modelo |
| persistência e exchange | **lote, assíncrono** | plataforma |
| monitoramento | **lote**, posterior | `monitoring.runner` |

## RNFD — requisitos não funcionais declarados

**Fonte:** `scripts/medir_custo_por_camada.py`, `0.13.0`, execução de 2026-09-18. Corpus do golden —
365 laudos sintéticos × 8 configurações, mediana de 3 repetições com aquecimento. Windows local,
Python 3.12.

| codigo | requisito | valor | medido_como | fonte |
|---|---|---|---|---|
| `RNFD-01` | custo por laudo, **régua pura** | **20,76 ms** | mediana de 3 × 365 | harness |
| `RNFD-02` | custo marginal da camada semântica (`token_overlap`) | **+3,51 ms** | contra a régua pura | harness |
| `RNFD-03` | custo marginal da camada ordinal | **+0,33 ms** | contra a régua pura | harness |
| `RNFD-04` | custo marginal do juiz, **rede estubada** | **+2,65 ms** | contra a régua pura | harness |
| `RNFD-05` | perfil completo | **24,44 ms** — **+18%** sobre a régua | contra a régua pura | harness |
| `RNFD-06` | vazão local | **≈ 48 laudos/s** | 365 laudos em 7,58 s | harness |
| `RNFD-07` | latência com o **modelo real** de embeddings | `[NAO INFORMADO]` | — | `L1` |
| `RNFD-08` | latência do juiz **com rede** | `[NAO INFORMADO]` | — | `L2` |
| `RNFD-09` | janela de carga, retenção, auditoria | `[NAO INFORMADO]` | — | é do job da plataforma, não da lib |

⚠️ **`RNFD-02`, `03` e `04` estão perto do ruído** — desvio de ±0,5 ms/laudo. **Não afirmar ordem
entre as três camadas** com estes dados.

🔴 **A leitura que decide arquitetura:**

1. **A régua responde por ~85% do custo local** — 20,8 de 24,4 ms.
2. **O motor não é gargalo em lugar nenhum.** Os 12.184 laudos diários da hepatologia são
   **≈ 4 minutos de CPU**.
3. **O custo real do sistema não é o motor — é quantos laudos vão ao juiz.** Um *round-trip* ao
   endpoint é da ordem de segundos, **~100× o processamento local do laudo inteiro**. Isso torna a
   **largura da `uncertainty_band`** a variável dominante de custo, e não só de qualidade.

Daí `RNFD-07` e `RNFD-08` serem lacunas que importam: elas medem justamente o que domina, e só um
run no ambiente as responde.
