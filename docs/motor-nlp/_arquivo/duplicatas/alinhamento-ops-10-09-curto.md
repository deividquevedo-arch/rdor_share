# Alinhamento com o time de Ops — pauta

> **Pauta, não ata.** Cada item traz **tópico · evidência · proposta · card sugerido**. Depois da
> reunião vira ata em `_processo/atas/`, com o que passou a valer.
>
> Consolidado em **2026-09-10**, sobre verificação feita nos repositórios, no board e nos catálogos
> de produção. Nenhum item entrou sem evidência conferida.

---

## Regra de organização — um card, um dono

🔴 **Card com responsabilidade dividida não fecha.** Feita a parte de um lado, o card segue aberto
pela parte do outro, e ninguém consegue dizer se está pronto. Onde o trabalho é dos dois, o item
vira **dois cards ligados**, cada um com dono único e critério de aceite próprio.

Vale para os cards existentes que hoje misturam donos — marcados abaixo com **⚠️ separar**.

---

## 0. Equalizar a ata de 21/08 — primeiro item

**Evidência.** `_processo/atas/` está vazio. De 21/08 existem a transcrição bruta da ferramenta e o
`_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md`, que é documento **do nosso lado**. Nenhum
registro acordado entre os dois times diz o que ficou decidido.

**Consequência observada.** "O que foi combinado" é contestável, e foi contestado na definição do
pin de versão em 10/09.

**Proposta.** Fechar a ata de 21/08 com os dois lados, e passar a registrar ata de toda agenda de
interface em `_processo/atas/`.

**Card sugerido:** `[Acordo] Equalizar a ata do alinhamento de 21/08` — dono: quem convocou.

---

# Bloco A — Ops / Fábrica

## A1. Dev depende do feed de produção — a `hml` da lib não tem consumo

**Evidência.** O cluster de dev resolve o `pip` contra `fabrica-ai`, que é o feed de **produção**.
Erro em 09/09: `Could not find a version that satisfies nlp-engine==0.12.2 (from versions: 0.9.4,
0.10.0, 0.10.1, 0.11.2)` — a lista é o conteúdo do feed de prd. O widget `nlp_engine_volume` foi
removido e não há índice extra declarado em nenhum arquivo do repositório da plataforma.

✅ **A wheel existe.** O volume de HML tem `nlp_engine-0.12.1`, `0.12.2` e `0.12.3`, e a esteira da
lib segue publicando lá (`UploadVolume`). O que falta é o consumo.

**Consequência.** Só se valida em dev uma versão que **já está em produção** — o que anula a função
da branch `hml` da lib e obriga a promover sem validar, ou a não validar.

**Proposta.** Religar o consumo por uma das duas vias: restaurar a leitura do volume de HML, ou
declarar `fabrica-ai-hml` como índice extra do `pip` no cluster de dev — **e só de dev**.

**Card sugerido:** `[Plataforma NLP] Restabelecer o consumo da lib em dev` — dono Ops.
⚠️ **separar de `283645`**, onde hoje está enterrado.

---

## A2. Fluxo de branch, publicação e comunicação de mudança de interface

**Evidência.** Card `283645` aberto em 21/08, **estado Novo, sem movimento há 20 dias**. Nesse
intervalo, duas mudanças no caminho de instalação foram para produção: PR 7194 (03/09, troca wheel
por `pip`, 18 arquivos, toca `jobs/ambientes/prod.json`, **descrição vazia**) e PR 7233 (08/09,
troca a policy e remove o volume da configuração do job). **As duas foram identificadas por
tentativa em 09/09, não por comunicação prévia.**

**Proposta.** Registrar o fluxo por escrito, e acrescentar: mudança em caminho de instalação, policy
de cluster, arquivo de ambiente ou contrato é comunicada ao outro lado **antes do merge**.

**Card sugerido:** manter `283645`, com escopo reduzido a fluxo e comunicação — dono Ops.

---

## A3. Verificação de config na esteira

**Evidência.** Não existe etapa de validação de config no CI. Nos 48 `.py` da plataforma o único
sinal de validação é Pydantic nos *loaders*, em tempo de execução. Nenhum PR é barrado por config
inválida.

**Proposta.** Confirmar se há um passo previsto, em que estágio, e o que ele checa. Do nosso lado
isso define o que precisamos cobrir antes de abrir PR.

**Card sugerido:** `[Plataforma NLP] Validação de config no CI — escopo e estágio` — dono Ops.

---

## A4. Filtro `validacao` da view de exportação

**Evidência.** `ntb_ia_motor_e2e.py:487` — `load_validation_rules()` lê `CONFIG_NAV['validacao']` de
`config/exchange/<ambiente>/` e filtra a view de exportação. São **18 arquivos**, 6 especialidades ×
3 ambientes, com regra de escopo e **regra clínica**: hepatologia limitada a `RJ/SP/BA`, transplante
de pulmão a 6–75 anos, quatro linhas com lista branca de `id_unidade`.

🔴 **Falha aberto:** arquivo ausente → warning → `{}` → **view sem filtro nenhum**, em silêncio.

**Consequência.** `fl_relevante = 1` não é o que o negócio recebe, e um critério clínico vive fora
da config clínica, sem revisão clínica.

**Proposta.** Tornar o filtro explícito no contrato de saída; decidir onde regra clínica deve morar;
falhar fechado quando o arquivo faltar.

**Card sugerido:** `[Plataforma NLP] Filtro de validação da view: transparência e falha fechada` —
dono Ops.

---

## A5. Monitoramento não registra nenhuma métrica de LLM

**Evidência.** As colunas são `total`, `relevantes`, `relevance_rate`, `confidence_*` e
`exames_distintos`. No TI-RADS a taxa ficou em **3,17% → 3,21% enquanto 4.703 chamadas ao LLM
falhavam** — a régua sustenta o número e o alerta não dispara.

**Proposta.** Acrescentar chamadas, erros e tokens, por linha e por dia.

**Card sugerido:** `[Plataforma NLP] Monitoramento sem métrica de LLM` — dono Ops.

---

## A6. `dt_execucao_modelo` em UTC, view filtra por data local

**Evidência.** A gravação é em UTC e a view de exportação filtra pela data local (BRT). Run entre
21:00 e 00:00 produz **view vazia, sem erro**. O agendamento das 04:00 está fora da janela, então
produção não sofre hoje.

**Proposta.** Uniformizar o fuso entre escrita e leitura.

**Card sugerido:** `[Plataforma NLP] dt_execucao_modelo em UTC e view em data local` — dono Ops.

---

## A7. Entrada recebe o documento RTF cru

**Evidência.** `exm_laudo_texto` vem de `laudo_original`. Em 4.321 laudos do TI-RADS, **116 (2,7%)
são o documento RTF inteiro** e ocupam **63,4 dos 68,6 MB do dia**; o maior tem 815 KB. O mesmo
struct traz `laudo_transformado`, com o texto limpo — vazio em 158 dos 4.321.

**Proposta.** Avaliar `coalesce(laudo_transformado, laudo_original)`, com custo medido antes.

**Card sugerido:** `[Plataforma NLP] Entrada recebe RTF cru havendo texto extraído` — dono Ops.

---

## A8. Provisionamento de schema

**Evidência.** `reumatologia` não existe em `diamond_fabrica_ia`. O schema `nlp_engine` não existe
em `gold_fabrica_ia`, que tem apenas `fhir` e `information_schema`.

**Proposta.** Provisionar os dois.

**Card sugerido:** `300348`, nomeando quais schemas — dono Ops.

---

# Bloco B — DS / nós

## B1. `embedding_model` aponta para volume do workspace antigo

**Evidência**, medida em 10/09 — as três linhas que declaram embeddings rodam em `token_overlap`:

| linha | laudos | com `FALLBACK:FileNotFoundError` | % |
|---|---|---|---|
| hepatologia | 5.172 | 5.108 | **98,8%** |
| cancer_estomago | 201 | 201 | **100%** |
| tirads | 1.568 | 1.348 | **86,0%** |

**Consequência.** A config declara `decision_mode: hybrid`; o que executa é régua mais sobreposição
de tokens. **Produção roda um perfil que nunca foi homologado**, e a monitoria não pega (ver A5).

**Proposta.** Caminho por ambiente, como já foi feito com o `base_url` do LLM no PR 7135.

**Card sugerido:** `298600` ⚠️ **separar** — a parte de config é nossa; o provisionamento do schema
em prd é do Ops e vai para A8.

---

## B2. `gold_filter` não seleciona punção

**Evidência.** O exame que originou o relato do negócio sobre o TI-RADS **não chega ao motor**:
`data.filters.gold_filter.keywords` seleciona por tireoide e pescoço, e *punção aspirativa por
agulha fina guiada por ultrassonografia* não casa nenhum termo. São **67 punções citando TI-RADS 4
em 16 dias barradas na entrada**, contra 36 rebaixadas pelo gate. Custo de incluir: **+492 exames em
16 dias**, ~31/dia, sobre linha que processa ~3.600/dia.

⚠️ Filtro de entrada é invisível para A/B local — só se mede rodando, e isso depende de A1.

**Proposta.** Incluir os termos, com medição em dois dias distintos.

**Card sugerido:** `[NLP Engine] gold_filter não seleciona punção` — dono DS.

---

## B3. SPEC 27 contradiz o código

**Evidência.** Card `299238` em estado **Novo e sem dono**. É a dependência declarada para
destravar o PR 7228, que está com voto `-10` desde 08/09.

**Proposta.** Atribuir, levar ao refinamento e fechar com os ajustes acumulados.

**Card sugerido:** `299238`, com dono — DS.

---

# Bloco C — acordo entre os dois

## C1. Isonomia de gate — verificação e matriz de impacto dos dois lados

**Evidência.** PR 7228, **adição pura de 89 linhas de documentação em um arquivo**, está com voto
`-10` de revisor obrigatório e pedido de história e refinamento desde 08/09. PRs 7194 e 7233, que
**alteraram o mecanismo de instalação em produção**, foram mergeados com descrição vazia ou igual ao
título, e um revisor. O `ci.yml` da esteira compartilhada registra que **nenhum job do CI bloqueia a
entrega** — `continueOnError` em todos — e que os gates de lint foram removidos na v2.0.0.

**Proposta.** Uma régua só, valendo para qualquer repositório da fábrica, o nosso incluído: definir
o que exige história, refinamento, verificação e matriz de impacto. Toda alteração da lib já passa
por comunicação, refinamento e medição de delta; submetidas ao mesmo processo, as mudanças de 03/09
e 08/09 teriam sido medidas antes, e a divergência apareceria no teste.

**Card sugerido:** `[Acordo] Régua única de gate para mudanças de interface` — dono: acordo, com um
representante de cada lado.

---

## C2. Contrato de entrada e de saída

**Evidência.** A **saída** tem piso versionado de 21 chaves, extraído de run real. A **entrada** não
tem contrato nenhum: nem coluna de origem declarada (ver A7), nem tipos, nem obrigatoriedade. Campos
novos do blob (`waive`, `gate_waived_by`, `gate_waived_error`, tokens da extração quantitativa)
aguardam aval.

**Proposta.** Declarar e versionar os dois lados do contrato.

**Card sugerido:** `283647` ⚠️ **separar em dois** — contrato de entrada, dono Ops; contrato de
saída, dono DS. Ligados entre si.

---

# Fora da pauta

**Definição de "validado" ao trocar versão** — proposta escrita em
[`procedimento-promocao-de-versao.md`](procedimento-promocao-de-versao.md), a levar ao time. O pin
em si está na história `301938`.

**Uso de agentes na revisão de código** — 🔒 **tema sensível, sem card.** Levar ao conhecimento do
time como nota ao final da agenda, com cautela. Não há diretriz comunicada por PO, PMO ou Head, e as
frentes envolvidas são lideranças técnicas de mesmo nível.

---

# O que já fechou do nosso lado

- ✅ Defeito da esteira que pulava a publicação em produção — corrigido.
- ✅ `0.12.2` e `0.12.3` publicadas nos dois feeds e validadas.
- ✅ **O LLM voltou a funcionar em produção** — 270 chamadas em 10/09, zero erro.
- ✅ Migração de reumatologia validada ponta a ponta, paridade de 99,42% em cinco medições.
- ✅ 15 cards de higiene da `0.12.x` em *Pronto para QA*, com 67 evidências medidas.
- ✅ Lista de versões por linha entregue — história `301938`.
- ✅ Feature `298598` atualizada: 8 versões entregues, `0.13.0` com 2 de 6 cards.

---

# Resumo

**13 itens** — 8 do Ops · 3 nossos · 2 de acordo.

🔴 **Três bloqueiam trabalho hoje:** A1 (consumo em dev), B1 (embeddings) e C2 (aval dos campos
novos).

⚠️ **Quatro cards precisam ser separados por dono:** `283645` (A1 sai), `298600` (B1 × A8), `283647`
(C2 vira dois), `300348` (nomear os schemas).
