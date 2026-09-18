---
titulo: Códigos, decisões e registro de intenção da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - RELEASE.md · 0.13.0 · registro de decisão de 8 versões
  - docs/adr/0001, 0002 · status proposto
  - docs/spec-0.10.0 a spec-0.13.0
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém preenche os blocos de decisão, pendência, lacuna e risco das pranchas e do
  PDF — e entende por que cada escolha foi feita, sem refazer a discussão.
relacionado:
  - 00-indice-e-fontes.md
  - 05-chaves-cobertura-e-lacunas.md
---

# Códigos, decisões e registro de intenção

> **O que este arquivo é:** o registro canônico. Nenhum código nasce em outro arquivo do pacote — os outros citam, este define.
> **O que ele não é:** a explicação técnica de cada item; essa mora no arquivo dono, indicado na última coluna.

## Tabela única de códigos

### Decisões — fechadas

| codigo | tipo | enunciado | impacto | quem_decide | prazo | arquivo_onde_aparece |
|---|---|---|---|---|---|---|
| `D1` | Decisão | **`NaN` é inválido, nunca zero nem "fora de faixa"**, em todos os caminhos de saturação | se sobrevivesse, contaminaria toda comparação a jusante — comparação com `NaN` é sempre falsa | dono da lib | fechado na `0.13.0` | 04 |
| `D2` | Decisão | **Streaming sai de `process()`** e vira bump próprio | quebraria a API; a plataforma já contorna montando lotes externamente | dono da lib | fechado | 01 · 06 |
| `D3` | Decisão | **A lib não descobre conexão**: `base_url` e credencial vêm da config ou do ambiente | permitiu diagnosticar o `403` como divergência de workspace, e não como defeito da lib | arquitetura | fechado | 01 · 08 |
| `D4` | Decisão | **Falha de infraestrutura não vira decisão clínica** | erro de LLM deixou de rebaixar por "medida ausente" | dono da lib | `0.11.0` | 02 · 07 |
| `D5` | Decisão | **A camada semântica não promove trecho negado** | a régua negava e a semântica promovia o mesmo texto | dono da lib | `0.11.1` | 04 |
| `D6` | Decisão | **Âncora ausente não significa "não se aplica"** | o critério sumia do gate e a promoção saía sem conferência — 36 de 1.032 entregas | dono da lib | `0.12.2` | 02 · 07 |
| `D7` | Decisão | **Evidência alternativa pode dispensar o gate** (`waive`), **opt-in** | inerte até uma config declarar; nenhuma declara hoje | dono da lib + negócio | `0.12.3` | 02 |
| `D8` | Decisão | **Lista ao negócio só a partir do fluxo completo calibrado** | `rule_only` e híbrido puxam em direções opostas; homologação não transfere entre perfis | liderança técnica de DS | régua vigente | 01 · 04 |
| `D9` | Decisão | **O juiz filtra, nunca cria relevância** — é invariante da lib | toda inclusão de escopo vira **achado**, não instrução no prompt | arquitetura | vigente | 01 · 06 |
| `D10` | Decisão | **Este pacote é vista; o repositório é a fonte** | divergência se resolve a favor do repositório | dono da lib | vigente | 00 |
| `D11` | Decisão | **O pacote descreve a API Python**, não o contrato de dados | a prancha 5 fala de função, não de tabela | dono da lib | 2026-09-18 | 06 · `BL-D-01` |

### Pendências — em aberto, e mudam escopo ou modelo

| codigo | tipo | enunciado | impacto | quem_decide | prazo | arquivo_onde_aparece |
|---|---|---|---|---|---|---|
| `P1` | Pendência | **ADR 0001 em `status: proposto`** — o aninhamento do pacote é design, e o `src/` converge com o POP-IA-04 | sem o aval, a decisão de estrutura não é decisão | responsável técnico da lib | — | 09 |
| `P2` | Pendência | **ADR 0002 em `status: proposto`** — as fronteiras seguem em `TypedDict` | idem | responsável técnico da lib | — | 06 |
| `P3` | Pendência | **Contabilidade de tokens na camada quantitativa** — `llm_prompt_tokens` e `llm_completion_tokens` | **mudança de contrato de saída**; hoje **221 das 270 chamadas de um dia ficam sem registro** | fronteira lib↔plataforma | `0.14.0` | 03 · 05 · 06 |
| `P4` | Pendência | **O contrato do lado do consumidor não está mergeado** — PR 7228, aberto desde 08/09 sem voto | quem consome não tem documento vigente do que recebe | revisor do PR | — | 00 · 06 |
| `P5` | Pendência | **Card do contrato lib↔plataforma com critério de aceite vazio** | não se fecha o que não tem critério, e não se alinha o que não está declarado | dono do card | — | 00 |
| `P6` | Pendência | **Largura da `uncertainty_band`** — de 7 a 6.111 chamadas/dia na mesma lib | 🔴 **é a variável dominante de custo E de qualidade**; estreitar contém o juiz e **abre** a via semântica | negócio + DS, por linha | `0.14.0` | 05 · 07 |
| `P7` | Pendência | **Saída do `src/`, convergindo com o POP-IA-04** | 3 passos; só o último quebra. Condicionado a plano fechado e **duas versões sem bump não planejado** | dono da lib | sem data | 09 |
| `P8` | Pendência | **Classificação e base legal do dado que a lib processa nunca foram formalizadas** | se exigir tratamento **dentro** da lib, é mudança de contrato nas 6 linhas | DPO / jurídico | 🔴 quanto mais tarde, maior o retrabalho | 08 |
| `P9` | Pendência | **Texto clínico em trânsito para o endpoint de LLM, sem parecer registrado** | a superfície cresce a cada linha que liga o juiz | DPO / jurídico | 🔴 antes de ligar mais linhas | 08 |

### Lacunas de dado

| codigo | tipo | enunciado | impacto | quem_decide | prazo | arquivo_onde_aparece |
|---|---|---|---|---|---|---|
| `L1` | Lacuna | latência com o **modelo real** de embeddings | o `RNFD-07` nasce vazio | run no ambiente | — | 05 · 07 |
| `L2` | Lacuna | latência do juiz **com rede** | o `RNFD-08` nasce vazio, e é o que **domina** o custo | run no ambiente | — | 05 · 07 |
| `L3` | Lacuna | prova do caminho `livre` da esteira | publicação em dois feeds sem prova de um dos ramos | próximo bump | — | 08 |
| `L4` | Lacuna | **`monitoring/` sem documentação própria** — 6 módulos, 6 das 35 entradas públicas, importado por 8 arquivos da plataforma | não há como atestar que o exposto é o pretendido | dono da lib | — | 03 · 06 |
| `L5` | Lacuna | `embedding_model` **não é emitido** no blob | não se prova, pelo blob, **qual** modelo decidiu | bump da lib | — | 05 |
| `L6` | Lacuna | tokens da camada quantitativa | ver `P3` | `0.14.0` | — | 05 |

### Riscos técnicos — regra que, violada, produz dado errado em silêncio

| codigo | tipo | enunciado | impacto | quem_decide | prazo | arquivo_onde_aparece |
|---|---|---|---|---|---|---|
| `R1` | Risco | **`runtime.llm_router` sobrepõe `nlp.llm_router`** | o que a config declara **não é** o que executa; sem erro e sem log | fronteira lib↔plataforma | — | 02 · 03 · 04 |
| `R2` | Risco | **`fallback_policy: positive_in_band` entrega em erro de transporte** | **1.371 laudos entregues pela falha** em 21 e 26/08 | config da especialidade | — | 02 · 06 · 07 |
| `R3` | Risco | **Modelo de embeddings inválido cai em `token_overlap`** | executa perfil nunca homologado. **21.530 laudos em 24 h**, 4 linhas | config + ambiente | — | 02 · 03 · 07 |
| `R4` | Risco | **`llm_router` sem `enabled` ⇒ juiz desligado** | bloco completo com modelo, banda e prompt, e o juiz **não roda** | config da especialidade | — | 02 · 03 |
| `R5` | Risco | **`ambiguity_band` é inerte em `decision_mode: hybrid`** | número na config que não decide nada | config da especialidade | — | 02 · 03 |
| `R6` | Risco | **Valor de chave não reconhecido deixa o bloco inerte** | `llm_router.mode` fora de `llm`/`deterministic` **não chama o juiz** | config da especialidade | — | 02 · 03 · 07 |
| `R7` | Risco | 🔴 **A camada semântica promove sem arbitragem do juiz** | `fl_relevante: 1` com `n_positive_spans: 0`. Medido: **33 de 44** numa linha, **118 laudos em 30 dias** noutra | dono da lib | `0.14.0` | 05 · 06 |
| `R8` | Risco | `negation.direction_default: None` **não** cai no default `left` | escopo de negação diferente do declarado | config da especialidade | — | 03 |
| `R9` | Risco | `segmentation.mode: auto` descarta seções | **86%** descartado na hepatologia | config da especialidade | — | 03 · 07 |
| `R10` | Risco | `aggregation_legend_filter` vai **dentro** do sistema, não no topo | filtro ignorado em silêncio | config da especialidade | — | 03 |
| `R11` | Risco | Alterar a ordem do tratamento de texto sem medir | achado some ou aparece; ninguém audita o que não vê | dono da lib | — | 04 |
| `R12` | Risco | Config com bloco morto | valores plausíveis que ninguém desconfia | revisão de PR | — | 04 |
| `R13` | Risco | Coorte sem a população não mede nada | zero divergência sobre coorte errada é medição vazia | quem mede | — | 04 |
| `R14` | Risco | Número sem denominador e sem data | `76,7%` é ruído | quem mede | — | 04 |

### Bloqueios de entrega e RNFD

`BL-D-01` e `BL-D-02` estão definidos no `05`. `RNFD-01` a `RNFD-09` estão definidos no `07`.
Ambos permanecem definidos **ali** porque são inseparáveis da medição que os sustenta — e este
arquivo os cita, como os demais citam estes.

---

## Plano de versões — entregue, medido, planejado

**Para conclusão e comunicação. Reporta o estado real, não o pretendido.**

### O que está em produção, verificado em 2026-09-18

| linha | engine | config | laudos (4 dias) | último run |
|---|---|---|---|---|
| hepatologia | **0.12.3** | `0.1.13-hep-emb-volume` | 26.164 | 18/09 14:32 |
| cancer_rim | **0.12.3** | `0.6.0-cancer_rim` | 22.036 | 18/09 14:34 |
| reumatologia | **0.12.3** | `0.1.0-reumatologia` | 18.763 | 18/09 14:19 |
| tirads | **0.12.3** | `0.8.0-tirads` | 8.452 | 18/09 14:27 |
| cancer_estomago | **0.12.3** | `0.6.9-cancer_estomago` | 1.261 | 18/09 14:17 |
| transplante_pulmao | **0.12.3** | `0.1.2-pulmao-failover` | 739 | 18/09 14:06 |

✅ **As seis linhas rodam a mesma versão**, declarada como **literal** na definição de cada job —
não como variável. É o que encerrou o período em que produção usava `latest` e a versão se moveu
**quatro vezes em nove dias** sem ninguém tocar no job.

⚠️ **Este pacote descreve a `0.13.0`, que ainda NÃO está em produção.** A `0.13.0` é versão de
estrutura e **nenhum laudo muda de decisão** em relação à `0.12.3` — provado, 2.920 linhas byte a
byte. Para efeito de prancha, o comportamento desenhado vale para as duas.

### As versões

| versão | tema | estado real | natureza |
|---|---|---|---|
| `0.9.x` – `0.11.1` | fundação, observabilidade, negação | ✅ entregues | — |
| `0.11.2` | espaço colado antes de acento | ✅ entregue | 🔴 **fora do plano** — defeito ativo em produção |
| `0.12.0` – `0.12.1` | higiene consolidada, 15 cards | ✅ entregues | neutra sobre a decisão |
| `0.12.2` | âncora ausente não é "não se aplica" | ✅ entregue | 🔴 **fora do plano** — defeito ativo |
| `0.12.3` | evidência alternativa dispensa o gate | ✅ **em produção nas 6 linhas** | 🔴 **fora do plano** — par da anterior |
| **`0.13.0`** | **estrutura interna** | 🟡 **código pronto, em branch SEM PR** | refactor · **não** altera API pública |
| `0.14.0` | juiz não cria relevância + tokens | 🟡 **medido, não especificado** | **comportamento** · muda contrato |
| `0.15.0` | vínculo lesão↔medida | 🔴 **card em refinamento, sem SPEC** | redesenho |
| sem data | estrutura de **pacote** (`src/`, nome do subpacote) | 🔴 condicionada | 3 passos; só o último quebra |

🔴 **Das oito versões entregues, TRÊS entraram fora do plano, por defeito ativo em produção.** Esse
é o número que descreve o regime real da lib — e é o critério que condiciona a reorganização de
pacote: ela só entra na fila depois de **duas versões consecutivas sem bump não planejado**.

### O que falta em cada uma, sem eufemismo

**`0.13.0`** — o código está fechado: quatro cards com todos os critérios atendidos e comentados,
gate de sete alvos verde (1.240 testes, 88,11% por ramo), release-check coerente, golden em quatro
recortes com delta zero. **Falta o que não é código:** o aval nos dois ADRs (`P1`, `P2`), o PR para
a `hml`, o merge e a promoção para a `main` — que é de onde o feed de produção publica.

**`0.14.0`** — a **medição está feita**: 118 laudos entregues como relevantes com o juiz acionado e
**zero span positivo de régua**, numa janela de 30 dias sobre seis linhas. E o escopo cresceu depois
da medição: são **duas vias**, não uma — o juiz promove, **e** a camada semântica promove sem que o
juiz seja consultado (`R7`). **Falta:** critério de aceite no card, que está vazio, e o alinhamento
de contrato — os campos de token (`P3`) mudam a saída, e a régua do time é alinhar **antes** de
implementar.

**`0.15.0`** — o card existe e está **Em Refinamento**. Não há SPEC, e o item é redesenho do modelo
de dados do critério quantitativo: hoje o gate é **documental**, sem vínculo entre lesão e medida,
e por isso se entrega `TR4 - Nódulo (2,1 cm)` num laudo cujo TI-RADS 4 media 0,4 cm. **Especificar
antes de estimar.**

**Estrutura de pacote** — decidida (`P7`), sem data, condicionada a plano fechado e à estabilidade
acima.

### O que isso significa para quem lê a prancha

1. **A lib está estável em produção e pinada** — a versão não se move sozinha desde 15/09.
2. **A próxima entrega é de estrutura e não muda decisão clínica** — risco de merge próximo de zero.
3. **A entrega seguinte muda comportamento E contrato**, e é a que precisa de alinhamento com a
   plataforma antes de existir código.
4. **O regime de defeito ainda não secou** — três de oito versões entraram por correção urgente, e
   é isso que segura a reorganização estrutural.

## Registro de intenção

Uma entrada por decisão estruturante. **Sem isto, a próxima pessoa refaz a discussão inteira.**

### A cascata régua → híbrido → juiz

| | |
|---|---|
| **o que foi decidido** | o juiz só arbitra o que a régua já marcou; nunca cria relevância |
| **por quê** | régua sozinha no `cancer_estomago` entregava 75 laudos com 55 errados — precisão 0,267. O que levou a 0,929 **não foi tirar o juiz**: foi alinhar o prompt à regra de negócio, em seis versões medidas, três revertidas |
| **o que foi descartado** | usar o prompt para **ampliar** escopo. Toda inclusão vira achado na régua, não instrução ao LLM |
| **o que quebra se mudar** | a banda deriva do teto **analítico** de score de laudo sem achado (0,597). Mudar peso, política de score ou régua invalida o corte — e o corte deixa de ser garantia para virar estimativa |

### Falha de infraestrutura não é decisão clínica

| | |
|---|---|
| **o que foi decidido** | indisponibilidade de LLM ou de modelo **não** rebaixa nem promove por si |
| **por quê** | o TI-RADS teve a taxa de entrega **cortada pela metade** quando o endpoint caiu — 6,7% → 3,2% — e os rebaixamentos por falta de medida foram de 48 para 824 num dia. **Nem a linha "só régua" funciona sem LLM** |
| **o que foi descartado** | tratar ausência de medida como negativa clínica |
| **o que quebra se mudar** | volta a classe de defeito que **não gera chamado**: a régua sustenta a taxa e a monitoria não acusa |

### Âncora ausente carrega dois sentidos opostos

| | |
|---|---|
| **o que foi decidido** | critério com `require_measure` cuja âncora não é reconhecida **permanece no gate** |
| **por quê** | antes, ele sumia — e a promoção que ele deveria condicionar saía entregue **sem nunca ter sido conferida**. 36 de 1.032 entregas como categoria pelada |
| **o que foi descartado** | a leitura de que âncora ausente significa "o critério não se aplica" |
| **o que quebra se mudar** | ⚠️ e há um custo assumido: **troca falso positivo por falso negativo no laudo de punção**, que é o caso de maior suspeição clínica. O par correto é a exceção de PAAF, não reverter |

### O contrato se fecha com piso extraído de produção

| | |
|---|---|
| **o que foi decidido** | o contrato de saída tem um **piso versionado**, extraído de 1.500 blobs reais |
| **por quê** | a lacuna fechou em **três ondas**: quatro chaves por auditoria e **três que só apareceram num run real**. **Verificação por fixture é estruturalmente insuficiente** — nenhum perfil escrito à mão esgota o que produção emite |
| **o que foi descartado** | confiar em fixture como prova de contrato |
| **o que quebra se mudar** | volta o cenário em que o consumidor quebra ao ler campo não declarado |

### A estrutura de pacote converge com o padrão, em vez de divergir

| | |
|---|---|
| **o que foi decidido** | o aninhamento `nlp_engine/nlp_engine/` é **design** e fica; o `src/` **sai**, convergindo com o POP-IA-04 |
| **por quê** | o aninhamento é design provado pelo commit de criação — os dois `__init__.py` nasceram juntos e o docstring de topo já descrevia os dois componentes. Quanto ao `src/`: as vantagens existem na literatura, mas **nenhuma está ativa aqui** — instalação *editable* e `pythonpath` fazem o teste ler a árvore, e o layout flat se comportaria igual. **Divergir custaria alinhamento sem entregar nada** |
| **o que foi descartado** | propor exceção ao POP; e também achatar o pacote fundindo motor e observabilidade |
| **o que quebra se mudar** | a migração tem 3 passos e **só o último quebra** (remoção do shim). Fazer tudo como um major prenderia os dois primeiros, que não quebram nada, atrás de um release caro de agendar |
