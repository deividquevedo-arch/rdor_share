# Proposta — Portfólio de modelos próprios e roadmap (v0.2)

> **Versão:** v0.2 — aplica P0 e P1 da revisão cruzada de 16/09
> (`_fundacao/propostas/revisao-proposta-portfolio-modelos-proprios-2026-09-16.md`). A v0.1 corrigiu o
> retrato de produção; a v0.2 corrige o que a revisão mostrou não se sustentar no código, nos
> dados e nos POPs. Histórico no fim do documento.
> **Status:** proposta para decisão. Macro, deliberadamente incompleta no *como*.
> **Público:** Head, dono da Central de Captação (Navegação), plataforma/MLOps, time de DS.
> **O que decide:** o quê construir, em que ordem e por quê. **O que deixa aberto:** o como,
> a alinhar com a plataforma sobre a proposta pronta, não a partir do zero.
> **Nomenclatura:** o consumidor das listas é a **Central de Captação**, chamada de "Navegação"
> no repositório e nas conversas. O **Portal Clínico de IA** do SDD da plataforma é outro produto,
> onde o clínico configura soluções. Esta proposta usa os termos do repositório.
> **Retrato de produção:** medido em **16/09/2026**, janela de 7 dias, nas seis tabelas de saída de
> `diamond_fabrica_ia`, mais metadados dos catálogos e o `ESTADO.md` de 16/09. Onde um número tem
> outra data, ela está dita. Método no Anexo B.
> **Origem:** brainstorm da página 21 de `.alt.doc/Mapa do Sistema Rede D'Or NLP Base.drawio`.

---

## 0. Resumo executivo

A tese é uma só: **cada pergunta clínica ou operacional que a Rede D'Or faz aos seus dados vira um
modelo próprio**, com gabarito, pesos, versão e dono próprios. O NLP de laudo foi o primeiro e roda
em seis linhas. O próximo é o prontuário eletrônico. Os seguintes já estão nomeados nos desenhos do
time, mas sem pergunta e sem gabarito, e por isso ainda não são modelos.

Três razões sustentam a tese, e todas as três já foram medidas neste projeto:

1. **O que não é próprio não é observável.** O LLM de terceiro ficou dias em erro 403 em agosto e,
   no dia do incidente, **1.371 laudos foram entregues pela falha**, por uma política de fallback
   declarada no bloco `runtime` da config, que sobrepõe o bloco `nlp`. A taxa de entrega não acusou,
   porque a régua sustenta o número. A camada de embeddings roda quebrada em quatro das seis linhas
   hoje, sem alarme.
2. **Modelo cujos pesos mudam por baixo invalida o gabarito.** A base ouro só vale contra um modelo
   congelado. Modelo próprio registrado com alias é reproduzível; endpoint de terceiro não é.
3. **Texto de laudo e prontuário são PHI.** Hoje o juiz recebe até 8.000 caracteres do laudo
   tratado, sem de-identificação, e o SDD da plataforma deixa esse gate em aberto (RNF-09). O POP
   de edição da plataforma exige ciência formal de Compliance e DPO para LLM em produção. Modelo
   próprio herda a proteção da coorte que o treinou; endpoint de terceiro exige o gate. Nos dois
   casos, a ciência do DPO é pré-condição, e vale igual para o Review App e para o dataset de rótulos.

E uma quarta, medida em 16/09, que é de arquitetura e não de fornecedor: **118 laudos foram
entregues como relevantes pelo juiz LLM sem nenhum span positivo de regra**, todos na hepatologia,
36 deles correntes. "Nunca decidir sem evidência" é invariante de desenho da lib, mas **não está no
código**: só a banda de incerteza declarada na config o sustenta. A proposta o exige de qualquer
modelo, próprio ou de terceiro, e a versão `0.14.0` da lib é o que o leva ao código.

**O que a proposta pede que se decida agora** está na seção 6. **O que dá para começar hoje, sem
GPU**, está na onda 0 da seção 5: especificar e pedir à plataforma o ciclo de rótulos por laudo,
registrar a ciência do DPO, congelar os gabaritos no lake e dar ao prontuário uma pergunta. Nada
disso altera config ou motor antes do alinhamento, que é a decisão de 15/09
(`_processo/alinhamentos/alinhamento-configuracao-nlp-2026-09-15.md`).

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

**Por que agora.** Três condições que faltavam passaram a existir, nenhuma delas inteira:

- **A plataforma tem o esqueleto da rotulagem humana.** O runner instancia uma sessão do MLflow
  Review App e registra um trace por execução (`plataform/ntb_ia_motor_e2e.py:441-456`, `:721`).
  Mas está **ativo só em dev e hml, por código** (`:443`), o trace **não carrega laudo nem
  `id_exame`**, a pergunta ao revisor é uma só e sobre extração, o Evaluation Dataset **nunca é
  gravado** e o revisor é fixo. O que falta é pequeno e é da plataforma: levar `id_exame` e o
  trecho de evidência ao trace, gravar o dataset com a proteção da coorte, definir quem acessa, e
  decidir a ativação em produção. Não é "ligar", é **especificar e pedir**.
- **O POP de ciclo de vida de ML** já prescreve tudo o que um modelo próprio precisa: baseline antes
  de modelo complexo, Models in Unity Catalog, modelo adaptado em `diamond.<produto>`, gabarito
  congelado com versão, avaliação por recorte, inferência batch por padrão, promoção de código e
  não de modelo, aliases referência, desafiante e campeão. Falta aprovar o POP, não escrever o
  método.
- **O motor NLP fala com o LLM por endpoint compatível com OpenAI, configurável por `base_url` e
  `model`, e a versão está pinada.** Um modelo próprio servido no Model Serving com esse contrato
  entra sem mudar a lib. Um modelo em processo, não: o juiz chama HTTP direto e a extração de medida
  aceita um `caller` injetável que o pipeline não repassa (`quantitative.py:31-32`;
  `decision_pipeline.py:817-828`). Plugar modelo em processo é um minor aditivo da lib. E desde a
  verificação de 15/09 as seis definições de job declaram `0.12.3` literal, então o que roda é o
  que se homologou.

**O que a fundação já previa.** Encoder próprio em português clínico, specialty heads por linha,
cluster GPU single node e o gate estatístico estão nos docs de fundação desde o início, como
**Fase 3** (`02-analise-profunda-engines-nlp-v0.md` §Fase 3; `04-visao-refinada-motor-nlp-unificado-v0.md`;
`05-roadmap-entregas-sprint-v0.md`; `06-resumo-alinhamento-engml-v0.md`). Foi adiada por decisão
explícita: o encoder é objetivo de evolução a partir da Fase 3, não ponto de partida. Esta proposta
reabre essa fase e a estende para além do laudo.

---

## 2. O que é um "modelo próprio" — definição operacional

Um modelo entra no portfólio quando cumpre os oito critérios. A forma técnica (régua, tabular,
encoder, LLM ajustado) segue a pergunta, não o contrário.

| # | critério | de onde vem |
|---|---|---|
| 1 | Responde **uma** pergunta, com contrato de entrada e saída em dicionário, próprio de cada modelo | diretriz das libs; contexto do paciente |
| 2 | **Não faz I/O.** Recebe dado pronto, devolve decisão com evidência. Buscar, guardar e agendar é plataforma | doc macro do contexto do paciente §5 e §6 |
| 3 | **Artefato versionado e reproduzível:** pesos em **Models in Unity Catalog**, em `diamond.<produto>`, com alias; **ou**, para régua e config, `config_version` + `engine_version` em toda saída. Proibido `latest` | POP-IA-001 passo 11; POP-IA-09; diretriz das libs |
| 4 | **Gabarito congelado e versionado no lake**, com dono clínico | POP-IA-001 passos 5 e 9 |
| 5 | Avaliado **por recorte**, com gate estatístico contra a referência antes de promover | POP-IA-001; roadmap de fundação (F-beta e McNemar) |
| 6 | **Inferência batch por padrão.** Serving só com requisito de milissegundos | POP-IA-001 passo 16 |
| 7 | **Rotulagem humana contínua** por amostra, no Review App, alimentando o gabarito | framework de homologação HITL; runner da plataforma |
| 8 | **Observável e nunca decide sem evidência:** versão em toda predição, log de fallback, contabilidade de custo, evidência de regra como pré-condição de qualquer promoção | lição da 0.12.1, do 403 e da medição do P0-29 |

Corolário: **regra determinística também é modelo próprio** quando cumpre os oito, pelo caminho do
critério 3 que lhe cabe: `config_version` + `engine_version`, não Models in UC. O cruzamento de
sangue da tireoide, por exemplo, é régua por janela e entra no portfólio como tal.

---

## 3. Arquitetura macro do sistema

```
FONTES            SELEÇÃO           MODELOS ESPECIALISTAS              ORQUESTRADOR                 CONSUMO
(Gold corporativa)(plataforma)      (lib e app · sem I/O)              (plataforma · determinístico)(Central de Captação / Navegação)

laudo ─────────┐                    M0 NLP de laudo                    casa por (id_paciente,       elegibilidade e priorização (E3)
prontuário/FHIR┼──▶ monta o lote ──▶ M1 prontuário            ─────▶   condição, critério)     ──▶  dedup de encaminhamento
laboratório ───┤    por linha        M2 laboratório                    guarda · expira · reavalia   ação: contato ou pedido de exame
movimentação ──┘                    M5 interpretador (pergunta aberta)        │        ▲             visões: navegação · clínico ·
                                    recebem dict pronto;                      ▼        │             qualidade · executivo
                                    devolvem evidência COM PAPEL       ESTADO paciente-condição
                                    (indica · confirma · refuta)       fila de pendência com prazo e proveniência
                                    ou PENDENTE (declaram o que falta) (schema do time de DS; hub quando aceitar estado derivado)
                                                                                       ▲
                    M6 encoder clínico PT-BR = infraestrutura de M0 e M1               │
                                                                                       │
                    rótulos (Review App) · retorno da navegação · base ouro ◀──────────┘─────────────────────────────┘
```

**O que o diagrama afirma, e a v0.1 dizia errado.** O estado **não alimenta os modelos**. O modelo
decide com o dict que recebe e, quando falta evidência, **declara a pendência**. Quem lê o estado é
o orquestrador, para casar a pendência com o que chegou, reavaliar e expirar. É a inversão que
sustenta o desenho do contexto do paciente: histórico para todo o lote custaria 5.624 exames em
seis dias no TI-RADS; só para quem tem pergunta, 324.

Cinco regras de fronteira, já decididas no desenho macro do contexto do paciente (§5 a §7) e na
arquitetura alvo do motor, reafirmadas aqui:

1. **Modelo não faz I/O e não conhece o ambiente.** Quem monta o lote, busca histórico, guarda
   estado e expira prazo é a plataforma.
2. **Orquestração é determinística.** O orquestrador "não é um decisor": casa por chave, guarda,
   expira e chama o modelo. Sem trigger próprio, sem planejamento por LLM. Isso responde ao "agente
   supervisor" do brainstorm: o papel existe, mas é um fluxo com estado, não um agente generativo.
3. **Evidência clínica é função do documento e é estável. Elegibilidade é função de hoje e é
   volátil.** Por isso elegibilidade e priorização (E3) ficam **no consumo**, fora do portfólio de
   modelos clínicos. Misturá-las faria o mesmo laudo decidir diferente conforme o dia, e a base ouro
   deixaria de valer.
4. **A Central consome por contrato.** O contrato hoje é **CONFIG + Jobs API + view de exportação +
   MLflow** (`05-sdd-arquitetura.md:174`), e a regra de ouro do SDD do portal, "nenhuma regra
   clínica fora do motor" (`:42-43`), é aqui **estendida** à Central. Models in Unity Catalog entra
   como **extensão proposta** desse contrato, não como algo que o SDD já preveja. ⚠️ Contrato exige
   **teste dos dois lados**: a coluna de achado do TI-RADS saiu vazia em 100% das linhas até o PR
   7287, que corrige a view de exportação, mergeado na `hml` em 15/09 com deploy em produção a
   confirmar. Ninguém a jusante acusou.
5. **O estado paciente-condição é saída dos modelos e entrada do orquestrador.** Cada modelo
   enriquece o estado; o orquestrador decide o que reavaliar. O **contrato agnóstico de onde mora**
   vale **só para a fila de pendência**, por chave natural. O Clinical Data Hub é o destino natural;
   até ele aceitar estado derivado, a fila vive no schema do time de DS.

### 3.1 Onde cada coisa mora

| camada | o que | de quem |
|---|---|---|
| **lib `nlp_engine`** | M0 (cascata do laudo); avaliação por paciente do M2 (dado o conjunto reunido, confirma ou refuta: é régua e não faz I/O); M6 como `embedding_model` ou backend | Dono do NLP Engine |
| **lib `data_manage`** | contratos de entrada e saída, leitura e escrita Delta, `data_manager` da plataforma (domínios por fonte) | plataforma |
| **lib `monitoring`** | métricas, coluna de modelo e de evidência, drift | plataforma |
| **plataforma** | seleção do lote, orquestrador de pendência, estado, labeling, exportação, distribuição | MLOps |
| **app de busca conversacional** | M5 interpretador (C2), validador (C3), row-LLM (C5) | busca conversacional |
| **consumo** | E3 elegibilidade e priorização, dedup, ação de navegação | operação e Central |

**Onde o LLM fica.** Em quatro pontos, nomeados: no M0, o **juiz na banda de incerteza** e a
**extração de medida**; na extração ordinal, o `llm_fallback` opt-in; na busca conversacional, o
**interpretador C2** e o **row-LLM C5**. Nos dois primeiros o LLM é a **referência a ser batida**
por um desafiante próprio. **Nunca como decisor sem evidência de regra**: isso é invariante de
desenho da arquitetura alvo, foi violado em produção (os 118 laudos do P0-29) e a `0.14.0` o leva ao
código. O precedente de desenho contrário existe no legado: uma PoC de captação decidia por prompt
único entre oncologia, cirurgia, ambulatório ou nenhum
(`zOPs/fabrica-ia-plataforma-handover/apps/databricks/dr_captacao/README.md`). Não é este desenho.

**Onde RAG fica.** Fora, por enquanto. A fundação tirou RAG clínico do escopo por não haver
protocolos e diretrizes estruturadas (`03-documento-auxiliar-brainstorm-motor-ds-nlp-llm-ml-v0.md`).
Se os vetores são os laudos, essa camada já existe e está quebrada em produção. Consertar antes de
ampliar. RAG volta quando houver corpus com dono: diretriz clínica versionada, ou recuperação de
casos similares para apoiar o revisor, não a decisão.

---

## 4. Portfólio — catálogo

| id | modelo | pergunta que responde | fonte de entrada (verificada em 16/09) | forma provável | gabarito existe? | dono do dado | estado |
|---|---|---|---|---|---|---|---|
| **M0** | NLP de laudo | Este laudo tem evidência do achado que a linha busca? | texto do laudo (Gold) | régua + medida + juiz | **parcial, por linha, fora do lake** | linha de cuidado | **em produção em 6 linhas**, engine `0.12.3` pinada; ver 4.1 |
| **M1** | Prontuário eletrônico | O paciente já está em acompanhamento? Há exclusão ou contraindicação? Qual o estado da condição? | **`gold_fabrica_ia_dev.fhir`**: 24 tabelas achatadas (`encontro`, `procedimento`, `condicao`, `declaracao_medicamento`, `laudo_diagnostico`, `paciente`…), gravadas até 11/09, **só em dev**; canônico em `gold_corporativo.<resource>` FHIR. ⚠️ O domínio `prontuario` do `data_manager` aponta para um snapshot de `workarea` de jan–mai/2026, **sem atualização desde 14/05 e sem consumidor** | régua + tabular primeiro; encoder para texto livre quando houver fonte de texto | **não** | hub + linha de cuidado | **próximo**; falta a pergunta e o gabarito |
| **M2** | Laboratório | O exame de sangue confirma ou refuta a suspeita do laudo? | **`gold_fabrica_ia_dev.fhir.observacao_exames`**: 629,6 milhões de linhas, `vl_numerico` em 392,6 milhões, **`un_medida` vazia em 100%**; canônico `gold_corporativo.observation` e `resultados_laboratorio`. Sem domínio no `data_manager` (`paciente.exames_lab` tem 0 preenchidos em 16,15 milhões) | régua determinística por janela | **aceite medido** (95 de 142 refutados por TSH), **sem gabarito validado** | linha de cuidado | especificado (fase 3 do contexto do paciente) |
| **E3** | Elegibilidade e priorização | Devemos agir, e em que ordem? | vivo, convênio, região (já na view), em acompanhamento, custo | tabular; **no consumo, fora do portfólio clínico** | não | operação | não iniciado |
| **M4** | Medicamentos | *indefinida* | `fhir.declaracao_medicamento`, `administracao_medicamento` (só dev) | *indefinida* | não | *sem dono* | **não entra** até ter pergunta e dono |
| **M5** | Interpretador de pergunta aberta | O que o usuário quer buscar? (pergunta → plano de consulta) | frase do usuário | LLM; candidato a modelo próprio tardio | não | busca conversacional | planejado (componente C2) |
| **M6** | Encoder clínico PT-BR | *não é produto*: base compartilhada de M0 e M1 | corpus de laudos e prontuário, com proteção da coorte | BERTimbau ou BioBERTpt com continued pre-training | n/a | DS | previsto na fundação (Fase 3), adiado |

**Leitura da tabela.** **Só M0 tem gabarito, e parcial.** M1 é o próximo porque tem fonte viva em
dev, fecha a pendência que M0 declara e é onde nasce o valor da navegação (fase 2 do contexto do
paciente). M2 tem fonte, mas sem unidade de medida a camada quantitativa não aplica `plausible_range`.
E3 é necessário e barato, mas não é modelo clínico. M4 não é modelo, é uma tabela sem pergunta.

**Três dívidas de dados atravessam M1, M2 e o estado**, e entram na onda 0:

- **Tudo existe só em dev** ou num catálogo único sem promoção. O catálogo `gold_fabrica_ia` não
  existe; existe `gold_fabrica_ia_dev`. Nada de M1 e M2 está em hml ou prd.
- **Quatro chaves de paciente sem de-para:** `id_patient` (`workarea`), `id_paciente` (`fhir`),
  `codPaciente` (`condicao_saude`), `subject` (FHIR cru).
- **Qualidade:** datas em 1900, 8026 e 9926; unidade vazia; uma cópia de planilha de homologação em
  `diamond_ia_hml.workarea` com **PHI em claro** (nome, CPF, telefone), que precisa de card e
  remoção independentemente desta proposta. Sem gate de qualidade, o gabarito nasce contaminado.

### 4.1 M0 — onde está e o que "refinar" significa

**Seis linhas na plataforma nova**, todas pinadas em `0.12.3`, job diário às 04:00 em dias úteis:

| linha | config em produção | juiz LLM | embeddings | observação |
|---|---|---|---|---|
| hepatologia | `0.1.13-hep-emb-volume` | ligado **pelo bloco `runtime`**, com `fallback_policy: positive_in_band` | quebrados (28.571 de 28.814) | ⚠️ decisão de 16/09 (`ESTADO.md` §Hepatologia): **não está de fato em PRD** por faltar o fluxo complementar; nada se toca isoladamente, tudo pelo card `303791` — *Plano de Migração algoritmos final* |
| tirads | `0.8.0-tirads` | desligado; LLM só na medida | quebrados (7.433 de 8.783) | achado corrigido na view pelo PR 7287; deploy em prd a confirmar |
| cancer_estomago | `0.6.9-cancer_estomago` | ligado, zero chamadas em 7 dias (banda `[0,60; 0,97]`) | quebrados (1.385 de 1.385) | em prd desde 04/09 |
| transplante_pulmao | `0.1.2-pulmao-failover` | `nlp` diz `False`, `runtime` diz `True`; zero chamadas ao juiz; LLM na medida | desligados | agendado |
| cancer_rim | `0.6.0-cancer_rim` | ligado (`nlp.llm_router.enabled: True`), 16 chamadas | quebrados (128.164 de 129.580) | em prd desde 14/09 |
| reumatologia | `0.1.0-reumatologia` | desligado | desligados | em prd desde 14/09; legado desligado |

Duas decisões de 15/09 (`_processo/alinhamentos/alinhamento-configuracao-nlp-2026-09-15.md`) valem para tudo o
que segue: **o bloco `runtime` sai das configs e `enabled` mora no `nlp`**, e **nada se altera nas
configs antes do alinhamento**. A proposta não pede exceção a nenhuma das duas.

**Legado.** O schema `diamond_fabrica_ia.legado` é **cópia pontual de 05/09**, feita por usuário e
não por job: 17 tabelas, 15 nunca mais alteradas. Só a ateromatose recebe escrita recorrente (09 e
11/09, por service principal), e está em migração para perfil completo (PR 7275, config
`0.2.0-ateromatose_coronariana-perfil-completo`). A data de execução das demais mede a cópia, não a
última execução do legado, que roda no workspace antigo. As migrações de biliares, neuro, cólon,
DII e ateromatose **saíram da fila do time de NLP em 15/09** e seguem com outros donos, sob o card
`303791`. Para o portfólio isso significa: M0 ganha linhas por migração, sem trabalho do time de
NLP, e cada uma chega com a mesma dívida de gabarito.

**Refinar** tem três frentes, em ordem:

1. **Rótulos por laudo.** Especificar e pedir à plataforma: `id_exame` e o **trecho de evidência**
   (não o laudo inteiro) no trace; amostra das divergências e das decisões na banda; pergunta de
   relevância além da de extração; dataset gravado no Unity Catalog com a proteção da coorte; acesso
   definido; ativação em produção por decisão do Ops. Linha candidata: **ca-rim**, com juiz ligado
   pelo bloco `nlp`, config de referência e zero laudos sem evidência no P0-29.
2. **Extração de medida própria** como desafiante em sombra do LLM. O LLM hoje só localiza e lê:
   parse, conversão de unidade, comparação, faixa plausível e gating por âncora já são código. É
   marcação de span mais normalização, com o maior volume (1.210 das 1.544 chamadas em 7 dias).
   Pré-condições: **minor aditivo da lib** (`caller` no `EngineContext` e repasse em `step_measure`;
   bloco de sombra na saída sem tocar `met`, `fl` e `decision_source`), e como isso é **chave nova
   de saída**, alinhamento com o Ops antes, no mesmo pacote da `0.14.0` e `0.15.0`. Ressalva: a
   `evidence` gravada hoje é texto livre, não offset; virar supervisão fraca exige alinhamento ao
   texto. Linhas: tirads e transplante de pulmão.
3. **Juiz próprio** como desafiante na banda de incerteza, **condicionado aos spans de regra**: um
   encoder de 512 tokens recebe janelas centradas nos `findings_spans`, não o documento inteiro, o
   que fecha o P0-29 por construção. Entra depois da `0.14.0` e de mais rótulo.

### 4.2 M1 — o que precisa ser respondido antes de codar

O prontuário é a fonte com mais perguntas possíveis e nenhuma escolhida. No desenho macro do
contexto do paciente ele é a **pergunta em aberto número 3**, marcada como Research separado; não é
a fase 0, que é exclusão e refutação **no escopo do laudo**. O Research do M1 tem quatro entregas, e
nenhuma é código:

1. **A pergunta.** Recomendação: começar por **"já em acompanhamento"**, que é a pergunta em aberto
   número 2 do desenho macro, e por **exclusão e contraindicação no escopo do paciente**, que
   estende a fase 0 do laudo. As duas reduzem falso positivo da navegação, que é o retorno mais
   frequente do negócio.
2. **O inventário das fontes** com cobertura medida: quantos pacientes das linhas ativas têm
   `encontro`, `procedimento`, `condicao` e `declaracao_medicamento` na janela relevante, em
   `gold_fabrica_ia_dev.fhir`. O mapeamento FHIR em curso no time já produz essas tabelas; o contrato
   de entrada do M1 nasce delas e do FHIR canônico de `gold_corporativo`, **não** do snapshot de
   `workarea`. Inclui o de-para das quatro chaves de paciente.
3. **De onde vem o gabarito.** A tabela de retorno `tb_mod_monitoramento_retorno` **não serve**: é
   carga única de planilha de homologação, sem `id_exame`. Candidatos: `diamond_fabrica_ia_dev.jornada.tb_mov_navegacao`
   (10.295 linhas, `fl_navegado` e `fl_captado` preenchidos, desfecho e linha de cuidado vazios, só
   dev, parada em 19/08), a coluna de acompanhamento da linha de cuidado, e rotulagem por amostra
   no Review App.
4. **O contrato de saída**, no formato de evidência com papel: indica, confirma, refuta. É o mesmo
   contrato do estado paciente-condição, para que M1 alimente o orquestrador sem adaptador.

---

## 5. Roadmap por ondas

Ondas, não datas: cada onda tem pré-condição e gate de saída. Datas entram quando a onda 0 medir
capacidade de rotulagem e o Head decidir a seção 6.

### Onda 0 — fundação: especificar e pedir, sem GPU, sem tocar config ou motor

| entrega | por quê | gate de saída |
|---|---|---|
| **SPEC do labeling por laudo** e pedido à plataforma (§4.1 frente 1) | o esqueleto existe; o que falta é da plataforma e a ativação em prd é do Ops | PR da plataforma aceito; rótulos por semana medidos em dev/hml |
| **Ciência formal de Compliance e DPO** para LLM em prd, Review App e dataset (RNF-09 em aberto) | exigência do POP-IA-08; hoje não há de-identificação antes do juiz | registro formal |
| Congelar os gabaritos existentes com versão, no lake | base ouro sem lugar oficial já custou conclusão errada | cada linha com `versao_gabarito` e dono |
| Research do M1 (seção 4.2), incluindo o de-para das quatro chaves | prontuário sem pergunta não é modelo | SPEC do M1 com pergunta, fontes, cobertura e origem do gabarito |
| **Contrato da fila de pendência**, chave `(id_paciente, condição, critério)`, agnóstico de onde mora | é o que liga modelos, orquestrador e hub | contrato escrito; ponto de partida avaliado: `gold_corporativo.condicao_saude.tb_gold_mod_condicao_saude_historico` (3,03 milhões de linhas, 19 condições, sem critério nem prazo) |
| Card e remoção do PHI em claro em `diamond_ia_hml.workarea` | achado da revisão; independe da proposta | tabela removida ou mascarada |
| Decisões da seção 6 levadas ao Head | destravam as ondas 1 e 2 | decisão registrada |

### Onda 1 — fases 0 e 1 do contexto e primeiros desafiantes

| entrega | pré-condição | gate de saída |
|---|---|---|
| **Fase 0 do contexto:** papel de exclusão e refutação no escopo do laudo (atende o pulmão V2, especificado e parado) | nenhuma | aceite da SPEC da fase 0 |
| **Fase 1 do contexto:** desfecho PENDENTE e contrato da demanda | fase 0 | motor declara o que falta na saída |
| **Minor da lib:** `caller` no `EngineContext`, bloco de sombra na saída | alinhamento com o Ops das chaves novas, no pacote da `0.14.0` | byte-identidade de `fl`, score e `decision_source` com desafiante ligado e desligado, mutante verificado |
| M0 extrator de medida próprio, em sombra (tirads, pulmão) | rótulos da onda 0; minor da lib | não inferior ao LLM no gabarito congelado, por recorte |
| M2 régua de sangue da tireoide | contrato da fila; fonte com unidade resolvida | aceite medido de 95 em 142 confirmado contra gabarito com dono |
| Orquestrador de pendência (fase 2 do contexto) | fases 0 e 1; contrato da fila | pendência com prazo e proveniência gravada e expirando |

### Onda 2 — prontuário e encoder

| entrega | pré-condição | gate de saída |
|---|---|---|
| M1 v1: "já em acompanhamento" e exclusão no escopo do paciente | SPEC do M1; gabarito inicial; fonte promovida além de dev | redução medida de falso positivo na navegação |
| M0 juiz próprio, em sombra, condicionado aos spans de regra | `0.14.0` entregue; rótulos na banda | não inferior ao LLM por recorte; **zero decisão sem evidência** |
| M6 encoder clínico PT-BR | GPU decidida; treino em dev sobre coorte aprovada; materializado em Volume até a lib ter loader `models:/` | modelo base em `gold.modelo` com métricas no MLflow |

### Onda 3 — estado no hub e consumo executivo

| entrega | pré-condição | gate de saída |
|---|---|---|
| Hub recebe estado derivado; busca própria substituída | hub aceitar evidência com papel, pendência e proveniência | fase 4 do contexto do paciente |
| E3 elegibilidade e priorização no consumo | dono na operação; critérios de custo definidos | fora do motor; auditável |
| M5 interpretador na busca conversacional | gates B8 a B12 e D1, D4 fechados | QueryPlan aprovado pelo validador C3 |

---

## 6. Decisões que a proposta pede

Cada decisão vem com uma recomendação. O objetivo é chegar com proposta pronta e pedir decisão,
não alinhamento.

| # | decisão | recomendação | quem decide |
|---|---|---|---|
| 1 | Onde se treina e como se promove modelo com PHI | **conforme o POP-IA-001:** discovery e treino do candidato em `dev`, sobre coorte aprovada pelo especialista clínico e **sem cópia indiscriminada de PII** (passos 4 e 5; POP-IA-07 §7.1); registro e gate em `hml`; **re-treino em `prd` pelo job da esteira** (passo 15), nunca manual. Modelo adaptado em `diamond.<produto>` herda a proteção da coorte | Head + governança |
| 2 | GPU | um cluster single node, sob demanda, só a partir da onda 2 | plataforma |
| 3 | Onde mora a fila de pendência até o hub | no schema do time de DS, com contrato agnóstico restrito à fila; migra quando o hub aceitar estado derivado | Head + dono do hub |
| 4 | Como a Central consome os modelos | por contrato: CONFIG, Jobs API, view de exportação e MLflow, **estendido** a Models in Unity Catalog; nunca chamada direta; **teste de contrato dos dois lados** | dono da Central + plataforma |
| 5 | Dono do gabarito por modelo | um modelo, um dono clínico; sem dono, não entra no portfólio | Head |
| 6 | Papel do LLM de terceiro | referência a ser batida e interpretador de pergunta aberta; nunca decisor sem evidência | DS, com aval do Head |
| 7 | Aprovação dos POPs da Fábrica | aprovar; são o método desta proposta e o pedido único da `_processo/alinhamentos/pauta-minima-ops.md` | Head |
| 8 | **Saída por paciente-condição ou por exame** | por paciente-condição, com o exame como evidência; é a decisão "mais cara de reverter" do desenho macro (lib, persister, view, consumo) e todo o §3 a pressupõe | arquitetura + DS + Central |

---

## 7. O que esta proposta não é, e os riscos

**Não é:**

- **Um supervisor generativo.** O "agente supervisor" do brainstorm é um orquestrador com estado.
  Agentes são modelos por contrato. Planejamento por LLM só na pergunta aberta.
- **RAG.** Sem corpus com dono, não há o que recuperar.
- **Substituir a régua.** A régua é a referência e o piso; modelo próprio entra como desafiante.
- **Uma base de conhecimento paralela ao hub.** Prontuário e histórico são o hub. A fila de
  pendência é o único estado do time, e é transitório.
- **Mexer em config ou motor agora.** Vale a decisão de 15/09: nada muda antes do alinhamento. A
  onda 0 especifica e pede.

**Riscos, com o que os mitiga:**

| risco | mitigação |
|---|---|
| Rótulo escasso e de qualidade duvidosa (planilha não é gabarito: no TI-RADS o MCC foi de 0,60 para 0,98 quando se cruzou com a base ouro, `tireoide/base-ouro-tirads-v2-metricas.md`) | onda 0 mede rótulos por semana antes de prometer modelo; gabarito só no lake, com versão e dono |
| PHI no treino, no trace do Review App e no dataset de rótulos | decisão 1; trecho de evidência e não o laudo inteiro; proteção da coorte; ciência do DPO como pré-condição |
| Fonte de M1 e M2 só em dev, com chaves sem de-para e qualidade sem gate | onda 0 entrega o de-para e o inventário; onda 2 exige fonte promovida |
| GPU nunca provisionada | onda 1 inteira roda em CPU; GPU só na onda 2 |
| Regressão invisível (a régua sustenta a taxa; o P0-29 ficou 20 dias sem medição) | desafiante sempre em sombra; `monitoring` ganha coluna de modelo e de evidência, não só de taxa |
| Distribuição e notificação dependem de segredo e destinatários literais no código (`data_exchange`, `jobs/ambientes`) | sanear antes de ampliar o consumo pela Central; card próprio, fora desta proposta |
| Capacidade do time | as migrações de legado saíram da fila em 15/09; onda 0 é pequena; ondas 1 a 3 entram no plano de sprints com prioridade explícita |
| Escopo crescer por nome bonito (medicamentos, custo, condição de saúde) | critério de entrada: pergunta, dado, gabarito e dono. Sem os quatro, fica na tabela como candidato |

---

## 8. Próximos passos

1. Revisar esta v0.2 com o time de DS e fechar a redação.
2. Levar ao Head as oito decisões da seção 6, com esta proposta como anexo.
3. Sessão com o dono da Central de Captação e a plataforma para o **como**: SPEC do labeling por
   laudo, contrato da fila, consumo por contrato.
4. Abrir os cards da onda 0: labeling por laudo (plataforma); PHI em claro em `workarea` (Crítico);
   congelamento dos gabaritos; Research do M1; contrato da fila.
5. Página nova no mapa do sistema com a arquitetura da seção 3, substituindo o brainstorm da
   página 21.

---

## Anexo A — documentos de base

| assunto | documento |
|---|---|
| estado atual das frentes | `docs/motor-nlp/ESTADO.md` (16/09/2026) |
| revisão cruzada desta proposta | `_fundacao/propostas/revisao-proposta-portfolio-modelos-proprios-2026-09-16.md` |
| contexto do paciente, orquestrador, estado, hub, fases | `doc-desenho-macro-contexto-paciente-v0.md` §5 a §9, `nota-review-contexto-paciente.md` |
| cruzamento por paciente, sangue | `doc-design-cruzamento-por-paciente-v0.md` |
| busca conversacional, interpretador, row-LLM | `doc-busca-conversacional-componentes-v0.md` |
| arquitetura alvo do motor, invariantes do LLM | `proposta-arquitetura-alvo-v0.md` §2 e §6 |
| as três libs | `anexo02-arquitetura-motor-nlp-v0.md`, `diretriz-desenvolvimento-libs-v0.md` |
| encoder, fine-tuning, gate estatístico, RAG fora do escopo | `02-analise-profunda-engines-nlp-v0.md`, `03-documento-auxiliar-brainstorm-motor-ds-nlp-llm-ml-v0.md`, `04-visao-refinada-motor-nlp-unificado-v0.md`, `05-roadmap-entregas-sprint-v0.md`, `06-resumo-alinhamento-engml-v0.md` |
| homologação e HITL | `framework-homologacao-hitl-v0.md` |
| P0-29 medido; plano de bumps da lib | `_processo/medicoes/medicao-p0-29-juiz-sem-evidencia-2026-09-16.md`; `nlp-engine-lib/docs/plano-acao-backlog-lib-2026-09.md` |
| decisões de 15/09 sobre config | `_processo/alinhamentos/alinhamento-configuracao-nlp-2026-09-15.md` |
| pauta com o Ops | `_processo/alinhamentos/pauta-minima-ops.md` |
| ciclo de vida de ML, alçadas, versões | `.alt.doc/POPs/POP-IA-001_Ciclo_de_Vida_ML.docx`, `POP-IA-07_Catalogos_e_Schemas.docx`, `POP-IA-08_Edicao_NLP_Platform.docx`, `POP-IA-09_Controle_de_Versoes.docx` |
| portal e contrato com o motor | `fabrica-ia-nlp-platform/.docs/03-sdd-especificacao-funcional.md`, `05-sdd-arquitetura.md` |
| labeling no MLflow: estado real | `fabrica-ia-nlp-platform/plataform/ntb_ia_motor_e2e.py:441-456, 581-590, 721`; `plataform/observability/ntb_ia_ml_labeling.py` |
| porta do LLM na lib | `nlp-engine-lib/src/nlp_engine/nlp_engine/llm_router_backend.py`, `quantitative.py`, `decision_pipeline.py`, `semantic_expand.py` |
| domínios do `data_manager` | `fabrica-ia-nlp-platform/plataform/tools/data_manager/ntb_ia_data_manager.py`, `docs/usage/specs/23-tools-data-manager.md` |
| definições de job com pin | `fabrica-ia-nlp-platform/jobs/definicoes/*.json` |
| PoC legada de captação por prompt | `zOPs/fabrica-ia-plataforma-handover/apps/databricks/dr_captacao/README.md` |
| índice de cards | `_processo/cards/indice-de-cards.md` |

## Anexo B — como o retrato foi medido

**Saídas de produção.** Seis tabelas `diamond_fabrica_ia.<linha>.tb_mod_diamond_<linha>_saida_v0`,
janela `dt_execucao_modelo >= current_date() - 7`, em 16/09/2026. Juiz chamado = `$.llm_called` de
topo do blob; LLM na medida = ocorrências de `"llm_called": true` menos as de topo; embeddings
quebrados = blob contendo `FileNotFoundError`. Consultas via CLI da plataforma, perfil
`nlp-platform`.

**Fontes de M1, M2 e estado.** Metadados de `system.information_schema.tables` e `columns` e
contagens (`count(*)`, `min`, `max` de coluna de data) em `gold_fabrica_ia_dev.fhir`,
`gold_corporativo_ia.workarea`, `gold_corporativo.condicao_saude`, `diamond_ia_hml.fabrica_ia` e
`diamond_fabrica_ia_dev.jornada`, em 16/09. Nenhuma leitura de texto de laudo ou de coluna nominal.

**Legado.** `max(dataExecucaoModelo)` por tabela de `diamond_fabrica_ia.legado`, e `created_by`,
`created`, `last_altered` do `information_schema`. As tabelas são cópia de 05/09; a data mede a
cópia.

⚠️ **Número a reconferir:** 1.371 aparece na medição do P0-29 com dois sentidos, laudos entregues
pela falha no dia do 403 e chamadas sem falha desde 02/09. A coincidência está na fonte; conferir
antes de citar fora deste documento.

## Histórico

| versão | data | o que mudou |
|---|---|---|
| v0 | 15/09/2026 | primeira redação, sobre o `ESTADO.md` de 10/09 |
| v0.1 | 16/09/2026 | retrato de produção medido: seis linhas, juiz em duas, pin aplicado, embeddings em quatro, P0-29 |
| v0.2 | 16/09/2026 | P0 e P1 da revisão cruzada: labeling como esqueleto e não como pronto; porta e invariante como estão no código; estado como saída dos modelos e entrada do orquestrador; E3 no consumo; decisão 1 conforme o POP; decisão 8; DPO como pré-condição; fontes de M1 e M2 verificadas; legado como cópia; Central de Captação; card com título; fases 0 e 1 na onda 1 |
