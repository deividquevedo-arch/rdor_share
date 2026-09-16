# Proposta — Portfólio de modelos próprios e roadmap (v0)

> **Status:** proposta para decisão. Macro, deliberadamente incompleta no *como*.
> **Público:** Head, dono da Central de Navegação, plataforma/MLOps, time de DS.
> **O que decide:** o quê construir, em que ordem e por quê. **O que deixa aberto:** o como,
> a alinhar com a plataforma sobre a proposta pronta, não a partir do zero.
> **Origem:** brainstorm da página 21 do mapa do sistema (`.alt.doc/Mapa do Sistema ... .drawio`),
> cruzado com os POPs da Fábrica, os docs de fundação e o que está medido em produção.
> **Retrato de produção:** medido em **16/09/2026**, janela de 7 dias, nas seis tabelas de saída de
> `diamond_fabrica_ia`, mais o `ESTADO.md` de 16/09. Onde um número tem outra data, ela está dita.
> **Data:** 2026-09-16 (v0.1, corrige o retrato de 10/09 da primeira redação).

---

## 0. Resumo executivo

A tese é uma só: **cada pergunta clínica ou operacional que a Rede D'Or faz aos seus dados vira um
modelo nosso**, com gabarito nosso, pesos nossos, versão nossa e dono nosso. O NLP de laudo foi o
primeiro e roda em seis linhas. O próximo é o prontuário eletrônico. Os seguintes já estão nomeados
nos desenhos do time, mas sem pergunta e sem gabarito, e por isso ainda não são modelos.

Três razões sustentam a tese, e todas as três já foram medidas neste projeto:

1. **O que não é nosso não é observável.** O LLM de terceiro ficou dias em erro 403 em agosto e
   **1.371 laudos foram entregues pela falha**, por uma política de fallback declarada num bloco de
   config que sobrepõe o outro. A taxa de entrega não acusou, porque a régua sustenta o número. A
   camada de embeddings roda quebrada em quatro das seis linhas hoje, sem alarme.
2. **Modelo cujos pesos mudam por baixo invalida o gabarito.** A base ouro só vale contra um modelo
   congelado. Modelo próprio registrado com alias é reproduzível; endpoint de terceiro não é.
3. **Texto de laudo e prontuário são PHI.** Hoje o trecho do laudo sai para um modelo que não é da
   casa, mesmo que servido dentro do Databricks. O ciclo de vida da Fábrica já prescreve que modelo
   adaptado herde a proteção da coorte que o treinou. Isso só é possível se o modelo for nosso.

E uma quarta, medida em 16/09, que é de arquitetura e não de fornecedor: **118 laudos foram
entregues como relevantes pelo juiz LLM sem nenhum span positivo de regra**, todos na hepatologia,
36 deles correntes. Decisão sem evidência é o que a proposta proíbe para qualquer modelo, nosso ou
de terceiro.

**O que a proposta pede que se decida agora** está na seção 6. **O que dá para começar hoje, sem
GPU e sem alinhamento novo**, está na onda 0 da seção 5: ligar o ciclo de rótulos que a plataforma
já tem implementado e desligado, e dar ao prontuário uma pergunta.

---

## 1. Por que modelos próprios, e por que agora

**Custo não é o argumento.** O uso de LLM é pequeno. Medido em produção, 10 a 16/09:

| linha | laudos em 7 dias | juiz chamado | LLM na extração de medida | erro de LLM |
|---|---|---|---|---|
| hepatologia | 28.814 | 318 | 0 | 0 |
| cancer_rim | 129.580 | 16 | 0 | 0 |
| tirads | 8.783 | 0 | 703 | 0 |
| transplante_pulmao | 743 | 0 | 484 | 0 |
| cancer_estomago | 1.385 | 0 | 23 | 0 |
| reumatologia | 86.843 | 0 | 0 | 0 |
| **total** | **256.148** | **334** | **1.210** | **0** |

São cerca de 220 chamadas por dia. **1.210 das 1.544 não têm contabilidade de token**, porque a
extração de medida não registra nada; só o juiz registra. Os volumes de ca-rim e reumatologia são
de esteira de histórico, não de regime.

**Os argumentos são governança, reprodutibilidade e soberania do dado.** E há um quarto, de
produto: um modelo próprio é um ativo que acumula. Cada rótulo do revisor, cada retorno da
navegação e cada correção do clínico melhora o próximo ciclo. Um prompt em endpoint de terceiro não
acumula nada.

**Por que agora.** Três condições que faltavam passaram a existir:

- A plataforma nova tem **rotulagem humana implementada** no MLflow Review App, com revisores
  designados, taxa de amostragem e Evaluation Dataset no Unity Catalog. Está desligada por padrão
  (`active: False`). É o ciclo de rótulos inteiro, pronto.
- O **POP de ciclo de vida de ML** já prescreve tudo o que um modelo nosso precisa: baseline antes
  de modelo complexo, Models in Unity Catalog, modelo adaptado em `diamond.<produto>`, gabarito
  congelado com versão, avaliação por recorte, inferência batch por padrão, promoção de código e
  não de modelo, aliases referência, desafiante e campeão. Falta aprovar o POP, não escrever o método.
- O **motor NLP já tem a porta certa e a versão está pinada.** O LLM entra por um adaptador atrás de
  uma etapa da cascata; modelo próprio é outro adaptador da mesma porta. E desde 15/09 as seis
  definições de job declaram `0.12.3` literal, então o que roda é o que se homologou.

**O que a fundação já previa.** Encoder próprio em português clínico, specialty heads por linha,
cluster GPU single node e a história S15 de fine-tuning piloto estão nos docs de fundação desde o
início, com o gate estatístico escrito. Foi adiado por decisão explícita: "o encoder é objetivo de
evolução, não ponto de partida". Esta proposta reabre essa fase e a estende para além do laudo.

---

## 2. O que é um "modelo nosso" — definição operacional

Um modelo entra no portfólio quando cumpre os oito critérios. A forma técnica (régua, tabular,
encoder, LLM ajustado) segue a pergunta, não o contrário.

| # | critério | de onde vem |
|---|---|---|
| 1 | Responde **uma** pergunta, com contrato de entrada e saída em dicionário | diretriz das libs; contexto do paciente |
| 2 | **Não faz I/O.** Recebe dado pronto, devolve decisão com evidência. Buscar, guardar e agendar é plataforma | doc macro do contexto do paciente §4 |
| 3 | Registrado em **Models in Unity Catalog**, em `diamond.<produto>`, com alias. Proibido `latest` | POP-IA-001 passo 11; pin aplicado em 15/09 |
| 4 | **Gabarito congelado e versionado no lake**, com dono clínico | POP-IA-001 passo 9; decisão de 02/09 sobre a base ouro |
| 5 | Avaliado **por recorte**, com gate estatístico contra a referência antes de promover | POP-IA-001; roadmap de fundação (F-beta e McNemar) |
| 6 | **Inferência batch por padrão.** Serving só com requisito de milissegundos | POP-IA-001 passo 16 |
| 7 | **Rotulagem humana contínua** por amostra, no Review App, alimentando o gabarito | framework de homologação HITL; notebook de labeling da plataforma |
| 8 | **Observável:** versão em toda predição, log de fallback, contabilidade de custo. **Nunca decide sem evidência** | lição da 0.12.1, do 403 e da medição do P0-29 |

Corolário: **regra determinística também é modelo nosso** quando cumpre os oito. O cruzamento de
sangue da tireoide, por exemplo, é régua por janela e entra no portfólio como tal.

---

## 3. Arquitetura macro do sistema

```
FONTES                    ESTADO                    MODELOS ESPECIALISTAS           ORQUESTRAÇÃO               CONSUMO
(Gold corporativa)        (paciente-condição)       (portfólio, cada um por contrato) (determinística)           (Central de Navegação)

laudo de exame  ─────┐                              ┌─ M0 NLP de laudo ─────────┐
prontuário / FHIR ───┤    ┌──────────────────┐      ├─ M1 prontuário ───────────┤    ┌──────────────────┐    ┌──────────────────────┐
exames de laboratório┼───▶│ evidência c/ papel│─────▶├─ M2 laboratório ──────────┼───▶│ pendência · prazo │───▶│ navegação · clínico  │
movimentação/convênio┤    │ pendência c/ prazo│      ├─ M3 elegibilidade/priorid.┤    │ proveniência      │    │ qualidade · executivo│
medicamentos ────────┘    │ proveniência      │      └─ M5 interpretador (LLM) ──┘    │ dedup · ação      │    └──────────┬───────────┘
                          └────────▲─────────┘                                        └──────────────────┘               │
                                   │                  ┌─ M6 encoder clínico PT-BR (infra compartilhada) ─┐               │
                                   │                                                                                     │
                                   └──────────── rótulos (Review App) · retorno da navegação · base ouro ◀───────────────┘
```

Cinco regras de fronteira, todas já decididas em documento anterior e reafirmadas aqui:

1. **Modelo não faz I/O e não conhece o ambiente.** Quem monta o lote, busca histórico, guarda
   estado e expira prazo é a plataforma.
2. **Orquestração é determinística.** O orquestrador "não é um decisor": casa por chave
   `(id_paciente, condição, critério)`, guarda, expira e chama o modelo. Sem trigger próprio, sem
   planejamento por LLM. Isso responde ao "agente supervisor" do brainstorm: o papel existe, mas é
   um fluxo com estado, não um agente generativo.
3. **Evidência clínica é função do documento e é estável. Elegibilidade é função de hoje e é
   volátil.** Não se misturam no mesmo modelo, ou o mesmo laudo passa a dar resultado diferente
   conforme o dia e a base ouro deixa de valer.
4. **A Central consome por contrato**: view de exportação, Models in Unity Catalog, MLflow e Jobs
   API. Nunca chama modelo diretamente nem reimplementa regra. É a regra de ouro do SDD do portal.
   ⚠️ O contrato precisa de teste dos dois lados: a coluna de achado do TI-RADS saiu **vazia em
   100% das linhas** em produção até o PR 7287, de 15/09, e ninguém a jusante acusou.
5. **O estado do paciente-condição é mão dupla.** Cada modelo enriquece o estado, e o próximo parte
   de mais contexto. O Clinical Data Hub é o destino natural; até ele aceitar estado derivado, o
   estado vive no nosso schema, com contrato agnóstico de onde mora.

**Onde o LLM fica.** Em dois lugares, e só neles: como **referência a ser batida** nas tarefas do
M0 (juiz na banda e extração de medida), até um desafiante próprio vencer; e como **interpretador
de pergunta aberta** na busca conversacional, onde a pergunta não é conhecida de antemão. **Nunca
como decisor sem evidência de regra.** Isso já é invariante da lib e já foi violado em produção: os
118 laudos do P0-29. O precedente de desenho contrário também existe no legado: uma PoC de captação
decidia por prompt único entre oncologia, cirurgia, ambulatório ou nenhum. Não é este desenho.

**Onde RAG fica.** Fora, por enquanto. A fundação tirou RAG clínico do escopo por não haver
protocolos e diretrizes estruturadas. Se os vetores são os laudos, essa camada já existe e está
quebrada em produção. Consertar antes de ampliar. RAG volta quando houver corpus com dono: diretriz
clínica versionada, ou recuperação de casos similares para apoiar o revisor, não a decisão.

---

## 4. Portfólio — catálogo

| id | modelo | pergunta que responde | entrada | forma provável | dado existe? | gabarito existe? | dono do dado | estado em 16/09 |
|---|---|---|---|---|---|---|---|---|
| **M0** | NLP de laudo | Este laudo tem evidência do achado que a linha busca? | texto do laudo | régua + medida + juiz | sim | parcial, por linha, fora do lake | linha de cuidado | **em produção em 6 linhas**, engine `0.12.3` pinada; ver 4.1 |
| **M1** | Prontuário eletrônico | O paciente já está em acompanhamento? Há exclusão ou contraindicação? Qual o estado da condição? | `encounter`, `procedure`, `medication_*`, `service_request`, evolução em texto | régua + tabular primeiro; encoder para texto livre depois | **sim, mapeado** no `data_manager` (domínio `prontuario`) | **não** | hub + linha de cuidado | **próximo**; falta a pergunta e o gabarito |
| **M2** | Laboratório | O exame de sangue confirma ou refuta a suspeita do laudo? | analitos por janela (ex.: TSH, T4 livre) | régua determinística por janela | sim | parcial (142 casos, 95 refutados) | linha de cuidado | especificado (fase 3 do contexto do paciente) |
| **M3** | Elegibilidade e priorização | Devemos agir, e em que ordem? | vivo, convênio, região, em acompanhamento, custo | tabular; **fora do motor clínico** | parcial (convênio e região já saem na view) | não | operação | não iniciado |
| **M4** | Medicamentos | *indefinida* | `medication_request`, `medication_statement` | *indefinida* | sim | não | *sem dono* | **não entra** até ter pergunta e dono |
| **M5** | Interpretador de pergunta aberta | O que o usuário quer buscar? (pergunta → plano de consulta) | frase do usuário | LLM; candidato a modelo próprio tardio | n/a | não | busca conversacional | planejado (componente C2) |
| **M6** | Encoder clínico PT-BR | *não é produto*: base compartilhada de M0, M1 e M5 | corpus de laudos e prontuário | BERTimbau ou BioBERTpt com continued pre-training | sim | n/a | DS | previsto na fundação, adiado |

**Leitura da tabela.** Só M0 e M2 têm gabarito. M1 é o próximo porque tem dado pronto, fecha a
pendência que M0 declara e é onde nasce o valor da navegação (fase 2 do contexto do paciente). M3
é necessário e barato, mas não é modelo clínico e fica fora do motor. M4 não é modelo, é uma tabela
sem pergunta.

### 4.1 M0 — onde está e o que "refinar" significa

**Seis linhas na plataforma nova**, todas pinadas em `0.12.3`, job diário às 04:00 em dias úteis:

| linha | config em produção | juiz LLM | embeddings | observação |
|---|---|---|---|---|
| hepatologia | `0.1.13-hep-emb-volume` | ligado **pelo bloco `runtime`**, com `fallback_policy: positive_in_band` | quebrados (28.571 de 28.814) | ⚠️ decisão de 16/09: **não está de fato em PRD** por faltar o fluxo complementar; nada se toca isoladamente, tudo pelo card `303791` |
| tirads | `0.8.0-tirads` | desligado; LLM só na medida | quebrados (7.433 de 8.783) | achado passou a ser entregue no PR 7287 |
| cancer_estomago | `0.6.9-cancer_estomago` | ligado, zero chamadas em 7 dias (banda `[0,60; 0,97]`) | quebrados (1.385 de 1.385) | em prd desde 04/09 |
| transplante_pulmao | `0.1.2-pulmao-failover` | `nlp` diz `False`, `runtime` diz `True`; zero chamadas ao juiz; LLM na medida | não declara | agendado |
| cancer_rim | `0.6.0-cancer_rim` | ligado (`nlp.llm_router.enabled: True`), 16 chamadas | quebrados (128.164 de 129.580) | em prd desde 14/09 |
| reumatologia | `0.1.0-reumatologia` | desligado | não declara | em prd desde 14/09; legado desligado |

Duas decisões de 15/09 valem para tudo o que segue: **o bloco `runtime` sai das configs e
`enabled` mora no `nlp`**, e **nada se altera nas configs antes do alinhamento**. A proposta não
pede exceção a nenhuma das duas.

**Legado ainda ativo.** No schema `legado` de `diamond_fabrica_ia`, a **ateromatose gravou em
16/09** (20.795 laudos em 30 dias) e está em migração para perfil completo (PR 7275, config
`0.2.0-ateromatose_coronariana-perfil-completo`). Cólon, DII, doenças biliares v2 e reumatologia
legada **gravaram pela última vez em 01 ou 02/09**; neuroimunologia em 24/08. As migrações de
biliares, neuro, cólon, DII e ateromatose **saíram da fila do time de NLP em 15/09** e seguem com
outros donos. Para o portfólio isso significa: M0 ganha linhas por migração, sem trabalho nosso, e
cada uma chega com a mesma dívida de gabarito.

**Refinar** tem três frentes, em ordem:

1. **Ligar o ciclo de rótulos** em uma linha, com amostra das divergências e das decisões na banda.
   Sem isso, nenhuma das outras duas frentes tem gabarito. Candidata: **ca-rim**, que tem juiz
   ligado por `nlp`, config de referência e zero laudos sem evidência no P0-29.
2. **Extração de medida própria** como desafiante em sombra do LLM. É marcação de span mais
   normalização, tem o maior volume (1.210 das 1.544 chamadas em 7 dias) e a evidência que o motor
   já grava serve de supervisão fraca. Linhas: tirads e transplante de pulmão.
3. **Juiz próprio** como desafiante na banda de incerteza. É classificação de documento em banda
   estreita. Entra depois, porque depende de mais rótulo. E o P0-29 (`0.14.0`) precisa fechar
   antes: juiz sem evidência de regra não pode ser a referência de nada.

### 4.2 M1 — o que precisa ser respondido antes de codar

O prontuário é a fonte com mais perguntas possíveis e nenhuma escolhida. O Research (RPI) do M1
tem quatro entregas, e nenhuma é código:

1. **A pergunta.** Recomendação: começar por **exclusão e refutação no escopo do paciente**, que é a
   fase 0 do contexto do paciente, já especificada, e por **"já em acompanhamento"**, que é a
   pergunta em aberto número 2 daquele desenho. As duas reduzem falso positivo da navegação, que é o
   retorno mais frequente do negócio.
2. **O inventário das fontes** com cobertura medida: quantos pacientes das linhas ativas têm
   `encounter`, `procedure` e `medication_*` na janela relevante. O mapeamento de prontuário em
   curso no time é o insumo. Se ele for FHIR, o contrato de entrada do M1 nasce em FHIR.
3. **De onde vem o gabarito.** Candidatos: o retorno da navegação já gravado
   (`tb_mod_monitoramento_retorno`), a coluna de acompanhamento da linha de cuidado, e rotulagem por
   amostra no Review App.
4. **O contrato de saída**, no formato de evidência com papel: indica, confirma, refuta. É o mesmo
   contrato do estado paciente-condição, para que M1 alimente o orquestrador sem adaptador.

---

## 5. Roadmap por ondas

Ondas, não datas: cada onda tem pré-condição e gate de saída. Datas entram quando a onda 0 medir
capacidade de rotulagem e o Head decidir a seção 6.

### Onda 0 — fundação, sem GPU e sem alinhamento novo

| entrega | por quê | gate de saída |
|---|---|---|
| Ligar o labeling do MLflow em uma linha (ca-rim) | é o ciclo de rótulos inteiro, já implementado | rótulos por semana medidos; Evaluation Dataset no Unity Catalog |
| Congelar os gabaritos existentes com versão, no lake | base ouro sem lugar oficial já custou conclusão errada | cada linha com `versao_gabarito` e dono |
| Research do M1 (seção 4.2) | prontuário sem pergunta não é modelo | SPEC do M1 com pergunta, fontes, cobertura e origem do gabarito |
| Contrato do estado paciente-condição | é o que liga todos os modelos ao orquestrador e ao hub | contrato agnóstico de onde mora, chave `(id_paciente, condição, critério)` |
| Decisões da seção 6 levadas ao Head | destravam as ondas 1 e 2 | decisão registrada |

### Onda 1 — primeiros desafiantes

| entrega | pré-condição | gate de saída |
|---|---|---|
| M0 extrator de medida próprio, em sombra no job noturno (tirads, pulmão) | rótulos da onda 0 | não inferior ao LLM no gabarito congelado, por recorte |
| M2 régua de sangue da tireoide | contrato do estado | aceite já escrito: 95 de 142 refutados |
| Orquestrador de pendência (fase 2 do contexto) | contrato do estado | pendência com prazo e proveniência gravada e expirando |

### Onda 2 — prontuário e encoder

| entrega | pré-condição | gate de saída |
|---|---|---|
| M1 v1: refutação e "já em acompanhamento" | SPEC do M1; gabarito inicial | redução medida de falso positivo na navegação |
| M0 juiz próprio, em sombra | rótulos na banda; `0.14.0` entregue | não inferior ao LLM por recorte, **zero decisão sem evidência** |
| M6 encoder clínico PT-BR | GPU decidida; corpus com proteção da coorte | modelo base em `gold.modelo` com métricas no MLflow |

### Onda 3 — estado no hub e consumo executivo

| entrega | pré-condição | gate de saída |
|---|---|---|
| Hub recebe estado derivado; busca própria substituída | hub aceitar evidência com papel, pendência e proveniência | fase 4 do contexto do paciente |
| M3 elegibilidade e priorização | dono na operação; critérios de custo definidos | fora do motor; auditável |
| M5 interpretador na busca conversacional | gates B8 a B12 e D1, D4 fechados | plano de consulta validado sem LLM |

---

## 6. Decisões que a proposta pede

Cada decisão vem com a nossa recomendação. O objetivo é chegar com proposta pronta e pedir
decisão, não alinhamento.

| # | decisão | recomendação | quem decide |
|---|---|---|---|
| 1 | Onde se treina com PHI e quem aprova a coorte | treino nos catálogos `hml` e `prd`, nunca em `dev`; coorte aprovada pelo especialista clínico; modelo adaptado em `diamond.<produto>` herda a proteção | Head + governança |
| 2 | GPU | um cluster single node, sob demanda, só a partir da onda 2 | plataforma |
| 3 | Onde mora o estado paciente-condição até o hub | no nosso schema, com contrato agnóstico; migra quando o hub aceitar estado derivado | Head + dono do hub |
| 4 | Como a Central consome os modelos | por contrato: view, Models in Unity Catalog, MLflow, Jobs API; nunca chamada direta; **teste de contrato dos dois lados** | dono da Central + plataforma |
| 5 | Dono do gabarito por modelo | um modelo, um dono clínico; sem dono, não entra no portfólio | Head |
| 6 | Papel do LLM de terceiro | referência a ser batida e interpretador de pergunta aberta; nunca decisor sem evidência | DS, com aval do Head |
| 7 | Aprovação dos POPs da Fábrica | aprovar; são o método desta proposta | Head |

---

## 7. O que esta proposta não é, e os riscos

**Não é:**

- **Um supervisor generativo.** O "agente supervisor" do brainstorm é um orquestrador com estado.
  Agentes são modelos por contrato. Planejamento por LLM só na pergunta aberta.
- **RAG.** Sem corpus com dono, não há o que recuperar.
- **Substituir a régua.** A régua é a referência e o piso; modelo próprio entra como desafiante.
- **Uma base de conhecimento paralela ao hub.** Prontuário e histórico são o hub. Não se duplica.
- **Mexer em config agora.** Vale a decisão de 15/09: nada muda antes do alinhamento.

**Riscos, com o que os mitiga:**

| risco | mitigação |
|---|---|
| Rótulo escasso e de qualidade duvidosa (planilha não é gabarito; MCC 0,58 virou 0,98 quando se cruzou com a base ouro) | onda 0 mede rótulos por semana antes de prometer modelo; gabarito só no lake, com versão e dono |
| PHI no treino | decisão 1; modelo herda a proteção da coorte; dev sem PII |
| GPU nunca provisionada | onda 1 inteira roda em CPU; GPU só na onda 2 |
| Regressão invisível (a régua sustenta a taxa; o P0-29 ficou 20 dias sem medição) | desafiante sempre em sombra; monitoria ganha coluna de modelo e de evidência, não só de taxa |
| Capacidade do time | as migrações de legado saíram da fila em 15/09; onda 0 é pequena; ondas 1 a 3 entram no plano de sprints com prioridade explícita |
| Escopo crescer por nome bonito (medicamentos, custo, condição de saúde) | critério de entrada: pergunta, dado, gabarito e dono. Sem os quatro, fica na tabela como candidato |

---

## 8. Próximos passos

1. Revisar esta proposta com o time de DS e fechar a redação.
2. Levar ao Head as sete decisões da seção 6, com esta proposta como anexo.
3. Sessão com o dono da Central de Navegação e a plataforma para o **como**: contrato do estado,
   consumo por contrato e ativação do labeling.
4. Abrir os cards da onda 0: labeling em uma linha; congelamento dos gabaritos; Research do M1;
   contrato do estado paciente-condição.
5. Página nova no mapa do sistema com a arquitetura da seção 3, substituindo o brainstorm da
   página 21.

---

## Anexo A — documentos de base

| assunto | documento |
|---|---|
| estado atual das frentes | `docs/motor-nlp/ESTADO.md` (16/09/2026) |
| contexto do paciente, orquestrador, estado, hub | `doc-desenho-macro-contexto-paciente-v0.md`, `nota-review-contexto-paciente.md` |
| cruzamento por paciente, sangue | `doc-design-cruzamento-por-paciente-v0.md` |
| busca conversacional, interpretador | `doc-busca-conversacional-componentes-v0.md` |
| arquitetura alvo do motor, modelo por etapa | `proposta-arquitetura-alvo-v0.md` |
| encoder, fine-tuning, gate estatístico | `02-analise-profunda-engines-nlp-v0.md`, `04-visao-refinada-motor-nlp-unificado-v0.md`, `05-roadmap-entregas-sprint-v0.md`, `06-resumo-alinhamento-engml-v0.md` |
| homologação e HITL | `framework-homologacao-hitl-v0.md` |
| P0-29 medido | `_processo/medicao-p0-29-juiz-sem-evidencia-2026-09-16.md` |
| ciclo de vida de ML da Fábrica | `.alt.doc/POPs/POP-IA-001_Ciclo_de_Vida_ML.docx` |
| portal e contrato com o motor | `fabrica-ia-nlp-platform/.docs/03-sdd-especificacao-funcional.md`, `05-sdd-arquitetura.md` |
| labeling no MLflow | `fabrica-ia-nlp-platform/plataform/observability/ntb_ia_ml_labeling.py` |
| domínio `prontuario` mapeado | `fabrica-ia-nlp-platform/docs/usage/specs/23-tools-data-manager.md` |
| definições de job com pin | `fabrica-ia-nlp-platform/jobs/definicoes/*.json` |

## Anexo B — como o retrato de produção foi medido

Seis tabelas `diamond_fabrica_ia.<linha>.tb_mod_diamond_<linha>_saida_v0`, janela
`dt_execucao_modelo >= current_date() - 7`, em 16/09/2026. Juiz chamado = `$.llm_called` de topo
do blob; LLM na medida = ocorrências de `"llm_called": true` menos as de topo; embeddings quebrados =
blob contendo `FileNotFoundError`. Legado: `max(dataExecucaoModelo)` por tabela de
`diamond_fabrica_ia.legado`. Consultas via CLI da plataforma, perfil `nlp-platform`.
