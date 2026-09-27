# Briefing de diagramação — `nlp_engine` · resposta do §2

> Saída exigida pela §2 da instrução, **antes** de escrever qualquer arquivo: inventário de origem,
> lacunas, perguntas bloqueantes e plano de arquivos.
> Escopo: **a biblioteca `nlp-engine`**. A plataforma tem responsável técnico próprio e já está
> endereçada — este pacote não a descreve, e onde ela é necessária isso está declarado abaixo.

---

## 1. Inventário de origem

| documento | o que é | versão / data | responde por ele |
|---|---|---|---|
| `RELEASE.md` | registro de decisão por versão, `0.9.0` → `0.13.0`, com o que cada uma **não** faz | `0.13.0` · 18/09/2026 | dono da lib |
| `src/.../contracts.py` | `EngineInputRow`, `EngineOutputRow`, `ExmLaudoResultadoPayload` — `TypedDict` nas fronteiras | `0.13.0` | dono da lib |
| `tests/api_surface.json` | superfície pública **declarada**, travada por gate (`api-surface-check`) | gerada | automático |
| `docs/REFERENCIA-API.md` | superfície pública **gerada dos docstrings**, travada por gate (`api-ref-check`) | gerada | automático |
| `docs/REFERENCIA-PARAMETROS.md` | as chaves de configuração que a lib lê, e o piso de versão | `>= 0.13.0` | dono da lib |
| `docs/spec-0.10.0` … `spec-0.13.0` | 7 SPECs: input, output, casos de borda e **o que não faz** | por versão | dono da lib |
| `docs/adr/0001`, `docs/adr/0002` | decisões de arquitetura — ⚠️ **`status: proposto`** | 18/09/2026 | aguarda aval |
| `docs/COMO-USAR.md` · `COMO-CALIBRAR.md` · `GUIA-ORDINAL.md` | guias de uso e calibração | — | dono da lib |
| `docs/IMPLEMENTATION-DATABRICKS.md` | instalação e uso no Databricks | `0.13.0` | dono da lib |
| `docs/plano-acao-backlog-lib-2026-09.md` | mapa de versões, ordem e condições | 18/09/2026 | dono da lib |
| `azure-pipelines.yml` + `scripts/check_release.py` | esteira, feeds por ambiente e gate de release | — | dono da lib |
| `CONTRIBUTING.md` + `Makefile` + `.pre-commit-config.yaml` | gate de sete alvos e hooks | — | dono da lib |
| `tests/chaves_observadas_em_run_real.json` | **21 chaves observadas em 1.500 blobs de produção** — piso versionado do contrato | — | evidência |
| `README.md` | visão, instalação e exemplo mínimo | `0.13.0` | dono da lib |

ℹ️ **Sem versão identificável:** os quatro guias (`COMO-USAR`, `COMO-CALIBRAR`, `GUIA-ORDINAL`,
`CONTRIBUTING`) não carregam versão própria; acompanham a da lib por convenção, não por declaração.

✅ **Zero PHI no material de origem.** Os dois únicos arquivos de dado versionados são
`api_surface.json` (superfície pública) e `chaves_observadas_em_run_real.json`, que — apesar do nome
— é uma **lista de 21 nomes de chave**, sem conteúdo. As fixtures de teste são sintéticas (`SYN`,
`fixture-e2e-001`).

---

## 2. Lacunas de entrada

### Bloqueantes

| # | o que falta | por quê bloqueia |
|---|---|---|
| **B1** | **O contrato do ponto de vista do consumidor não está mergeado.** Vive em `docs/contrato-saida-0.12.1`, no **PR 7228** da plataforma, aberto desde 08/09 sem voto. | É a peça que descreve o contrato para quem **consome**. O `06` sem ela descreve só o lado de dentro. |
| **B2** | **As duas decisões de arquitetura estão em `status: proposto`.** ADR `0001` (aninhamento do pacote e convergência do `src/`) e `0002` (tipos nas fronteiras). | O `09` registraria como decisão o que ainda é proposta. |
| **B3** | **O contrato muda na `0.14.0`.** O card `283648` acrescenta contabilidade de tokens na camada quantitativa; o card `283647` (contrato lib↔plataforma) está com **critério de aceite vazio**. | Congelar hoje é congelar véspera de mudança, e a prancha circula como canônica. |

### Não bloqueantes

| # | o que falta | efeito |
|---|---|---|
| N1 | ~~Nenhuma medição de custo por laudo.~~ ✅ **MEDIDO em 18/09** — ver §6. Resta o que só o ambiente responde: latência com o modelo real de embeddings e o *round-trip* do juiz. | O `07` passa a ter `RNFD` com fonte; dois seguem `[NAO INFORMADO]` **com causa declarada**. |
| N2 | **A lib não tem nenhum diagrama.** A sequência de decisão existe só em prosa, espalhada pelas SPECs. | O `07` é escrito do zero, a partir do código e das SPECs. |
| N3 | **`monitoring/` nunca foi documentado.** São 6 módulos, irmãos do motor dentro do mesmo pacote distribuído. | Se entrar no escopo, é material novo, não compilação. |
| N4 | O `azure-pipelines.yml` declara dois feeds por ambiente, mas **o caminho `livre` da esteira segue sem prova** — só o `publicada` foi exercitado. | Vira ressalva no `09`, não lacuna de desenho. |

---

## 3. Perguntas bloqueantes

✅ **Respondidas em 18/09:** o pacote descreve a **`0.13.0`**, e o `06` é a **API Python**.

⚠️ **Consequência da segunda, a declarar no `00`:** o **contrato de dados de saída fica fora do
pacote**. São coisas distintas — a lib devolve `EngineOutputRow`; quem decide o que vira coluna da
tabela, como o blob é serializado e o que a view expõe é a plataforma. A prancha 5 fala de **função
pública**, não de tabela. O contrato de dados vive em `docs/contrato-saida-0.12.1` (PR 7228 da
plataforma) e no card `283647`, e o pacote aponta para lá em vez de descrevê-lo.

<details><summary>Registro das perguntas originais</summary>

1. **O pacote descreve qual versão?** A `0.12.3` está publicada nos dois feeds e é o que roda em
   produção; a `0.13.0` está em branch **sem PR**. Isso define o `versao_contrato` de todos os dez
   arquivos, e as duas diferem em estrutura interna, não em contrato.
2. **O `06` é a API Python ou o contrato de dados?** São coisas diferentes: a API é `process()` mais
   os três `TypedDict`; o contrato de dados são as colunas que a plataforma grava na tabela de
   saída. A lib não expõe HTTP.
3. **Os campos da `0.14.0` entram como `planejado` ou ficam fora?** São `llm_prompt_tokens` e
   `llm_completion_tokens` na camada quantitativa — mudança de contrato já mapeada, ainda não
   implementada.
4. **`monitoring/` está no escopo?** Ele é distribuído no mesmo wheel e é irmão do motor, mas é
   outra lib pela arquitetura declarada (sem import cruzado, comunicação por `dict`/DataFrame).
5. **As pranchas são da lib isolada ou da lib dentro do fluxo da plataforma?** Se for dentro, o `02`
   e o `07` precisam de material do repositório da plataforma — catálogos, schemas, job e view —
   que está fora deste escopo.

</details>

Seguem abertas a **3** (campos de token da `0.14.0` entram como `planejado`?), a **4**
(`monitoring/` no escopo?) e a **5** (lib isolada ou dentro do fluxo?).

---

## 4. Plano de arquivos — **os 10, sendo 4 com unidade redefinida**

| # | arquivo | decisão | por quê |
|---|---|---|---|
| 00 | `00-indice-e-fontes.md` | ✅ **gerar** | sempre; e é onde os desvios de unidade abaixo ficam declarados |
| 01 | `01-visao-escopo-e-atores.md` | ✅ **gerar** | a lib tem escopo forte e **escopo negativo excepcionalmente rico** — ela não lê arquivo de config, não descobre endpoint nem credencial, não persiste, não conhece Spark. Atores: o runner da plataforma, e os notebooks de bancada |
| 02 | `02-arquitetura-e-camadas.md` | 🟡 **gerar, com a faixa redefinida** | não existe bronze/silver/gold. A "camada" real da lib é o **pipeline de decisão**: tratamento de texto → régua → expansão semântica → critérios quantitativos → juiz LLM. A tabela de camadas passa a ser `etapa \| o_que_decide \| o_que_a_desliga \| como_degrada`. **Desvio declarado no `00`** |
| 03 | `03-inventario-de-objetos.md` | ✅ **gerar, com a unidade redefinida** | ⚠️ `catalogo.schema.tabela` é **exemplo** de solução de dados, não a definição da coluna — o nome dela é `endereco`. A lib tem endereço: `nlp.findings.ulcera`, `nlp.organs.tireoide`, `nlp.ordinal_extraction.systems.ti_rads`. **As oito colunas preenchem sem renomear nenhuma:** `camada` = régua/semântica/quantitativa/ordinal/juiz · `grao` = uma linha por achado declarado · `chave` = o nome canônico · `origem_de` = a config que o declara · `evidencia` = o teste que cobre ou a medição que sustenta |
| 04 | `04-cargas-transformacoes-e-regras.md` | ✅ **gerar — é o coração** | não há *jobs*, mas há o resto e é o núcleo da lib: os **filtros de texto na ordem em que rodam** (a ordem é conteúdo), o mapa de entrada→saída, e as **regras técnicas com consequência medida** — cada `R` aqui tem incidente real por trás |
| 05 | `05-chaves-cobertura-e-lacunas.md` | ✅ **gerar — e é dos mais fortes** | a "ponte de chaves" da lib é a **cascata de decisão**: régua → expansão semântica → juiz, um passo por linha, **com denominador** — que é o que o §3.2 exige e quase ninguém entrega. Temos medido: 81,5% de 7.500 ao juiz · 33 de 44 promoções semânticas sem arbitragem · 36 de 1.032 entregas com âncora ausente. E a cobertura por campo servido existe: `chaves_observadas_em_run_real.json`, **21 chaves em 1.500 blobs de produção** |
| 06 | `06-contrato-de-api.md` | ✅ **gerar — e é o mais importante** | a API é Python, não HTTP, e o template encaixa mesmo assim: `metodo_e_rota` vira a função pública; **os estados de resposta mapeiam direto** — a lib já distingue "não relevante" de "não foi possível decidir", e é exatamente onde ela erra quando erra |
| 07 | `07-sequencia-e-degradacao.md` | ✅ **gerar** | a degradação é o ponto mais documentado da lib, e o mais caro historicamente: queda para `token_overlap`, erro de LLM, `fallback_policy`, âncora ausente. Cada cenário tem número medido |
| 08 | `08-lgpd-seguranca-e-operacao.md` | 🟡 **gerar em versão enxuta** | não há classificação por coluna, máscara, grupo de acesso nem base legal — **a lib não é titular de dado**. O que existe e vale: ela **não persiste**, **não registra texto de laudo em log**, faz `_scrub` da chave de API antes de qualquer mensagem de erro, e declara a origem da credencial (`llm_api_key_origin`). Sem isso o arquivo some e o leitor conclui que ninguém pensou no assunto |
| 09 | `09-codigos-decisoes-e-intencao.md` | ✅ **gerar** | é onde a lib é mais rica: 7 SPECs, 2 ADRs e um `RELEASE.md` que já registra *o que foi decidido · por quê · o que foi descartado*. Em boa parte é **transcrição com procedência**, não redação nova |

**Resultado: os dez arquivos se aplicam.**

⚠️ **Correção a uma primeira leitura deste plano**, que excluía o `03` e o `05`. O erro foi ler
`catalogo.schema.tabela` como *definição* da coluna quando é *exemplo* de uma solução de dados.
Nenhum dos dois some: eles mudam de **unidade**, mantendo **as colunas idênticas** — que é o que
preserva o parsing determinístico exigido pelo §5.

| arquivo | unidade no template | unidade na lib |
|---|---|---|
| `02` | camada de dado (bronze→gold) | **etapa de decisão** |
| `03` | objeto de dado (`catalogo.schema.tabela`) | **unidade de configuração** (`nlp.findings.<x>`) |
| `05` | ponte de chaves entre sistemas | **cascata de decisão**, com denominador |
| `08` | classificação por coluna e grants | **o que a lib não faz** com o dado que atravessa |

Os quatro desvios ficam declarados no `00`, como o §3 manda.

### O propósito, testado prancha a prancha

O objetivo declarado no §1 é *"suficiente, sozinho, para alguém desenhar cinco pranchas e um
relatório de PO sem fazer uma única pergunta de volta"*. Ele se sustenta:

| prancha | a lib preenche |
|---|---|
| 1 · Blueprint | ✅ faixas viram etapas; escopo negativo é rico; operação = esteira, feeds e gate |
| 2 · Fluxo de dados | ✅ é literalmente o que a lib faz — filtros na ordem, transformações, mapa de tipos |
| 3 · Sequência | ✅ a mais forte: cada cenário de degradação tem número medido |
| 4 · Dados do domínio | ✅ **muda de assunto sem perder** — o domínio da lib não é tabela, é **régua clínica**: achados, órgãos, negação, critérios, sistemas ordinais |
| 5 · Endpoints | ✅ traduzida: a rota é a função pública, e os estados de resposta mapeiam direto |

🔴 **A única perda real é o `RNFD` do `07`.** Não há medição de latência nem de throughput da lib.
Entra como `[NAO INFORMADO]` com código, que é o que o §3.1 manda — e é lacuna nossa, não do
template.

---

## 5. Duas condições que eu poria no pacote

1. **O pacote é vista, o repositório é a fonte.** `REFERENCIA-API.md` e `api_surface.json` são
   **gerados e travados por gate** — se o pacote os copiar, diverge na primeira edição e passa a
   existir uma segunda descrição da mesma API. O `00` precisa dizer isso, e os arquivos linkam em
   vez de transcrever.
2. **O contrato entra declarado como em revisão.** `versao_contrato: 0.13.0`, `status: vigente`, e
   uma pendência `P` nomeando os cards `283647` e `283648`. Assim a prancha nasce sabendo qual parte
   está prestes a mudar — em vez de congelar a véspera e virar a versão canônica errada.

---

## 6. RNFD — medido em 18/09/2026

**Fonte:** `scripts/medir_custo_por_camada.py`, na `0.13.0`. Corpus do golden — 365 laudos
sintéticos × 8 configurações, mediana de 3 repetições, com aquecimento. Windows local, Python 3.12.

| código | requisito | valor | medido como |
|---|---|---|---|
| **RNFD-01** | custo de processamento por laudo, **régua pura** | **20,76 ms** | mediana de 3 × 365 laudos |
| **RNFD-02** | custo marginal da camada semântica (`token_overlap`) | **+3,51 ms** | contra a régua pura |
| **RNFD-03** | custo marginal da camada ordinal | **+0,33 ms** | contra a régua pura |
| **RNFD-04** | custo marginal do juiz, **rede estubada** | **+2,65 ms** | contra a régua pura |
| **RNFD-05** | perfil completo | **24,44 ms** — **+18%** sobre a régua | contra a régua pura |
| **RNFD-06** | vazão local | **≈ 48 laudos/s** | 365 laudos em 7,58 s |
| **RNFD-07** | latência com o **modelo real** de embeddings | `[NAO INFORMADO]` · **L** | o modelo vive num Volume, indisponível localmente |
| **RNFD-08** | latência do juiz **com rede** | `[NAO INFORMADO]` · **L** | exige run no ambiente |

⚠️ **As diferenças entre RNFD-02, 03 e 04 estão perto do ruído** (desvio de ±0,5 ms/laudo). Não se
deve afirmar ordem entre as três camadas com estes dados.

✅ **O que é robusto, e é a leitura que importa:**

1. **A régua responde por ~85% do custo local** — 20,8 de 24,4 ms.
2. **O motor não é gargalo em lugar nenhum.** A hepatologia processa 12.184 laudos/dia; a essa
   vazão são **~4 minutos de CPU**.
3. 🔴 **O RNFD real não é o custo do motor — é quantos laudos vão ao juiz.** Um *round-trip* ao
   endpoint é da ordem de segundos, **~100× o processamento local do laudo inteiro**. Isso torna a
   largura da banda de incerteza a variável dominante de custo, e liga este arquivo diretamente ao
   card `283648` `[P0-29]`: a medição registra o juiz acionado em **6.111 de 7.500 laudos (81,5%)**
   numa linha com banda larga.
