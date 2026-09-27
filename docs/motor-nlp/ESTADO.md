# Estado das frentes

> **Documento vivo.** É a única fonte de "onde cada coisa está agora". Carregado em toda sessão
> via `@` no `CLAUDE.md`, então sobrevive à compactação.
>
> **Regra:** aqui vai **estado** (o que está feito, o que falta, de quem depende). Fato durável e
> lição aprendida vão para a memória (`/memory`). Se uma linha aqui não muda há meses, ela é fato —
> mova para lá. Se uma memória tem data e "estado atual", ela é estado — mova para cá.
>
> Atualizado em **2026-09-25**.
>
> 🧭 **Mapa de navegação:** [[README|índice do motor-nlp]] — por linha de cuidado, por frente da lib
> e por tema de plataforma. **História do dia** em `_processo/diario/`; **fato durável** na memória
> do projeto. Este documento é o **retrato**, não o histórico.

---

## 🏖️ FÉRIAS — 26/09 a 12/10, retorno em 13/10/2026

O que fica **parado por decisão**, não por esquecimento. Nada abaixo precisa de ação de terceiro
para permanecer seguro: produção segue nas seis linhas com a `0.12.3` pinada, e nenhum pin muda.

| pendência | estado | quem |
|---|---|---|
| PR da config `0.2.0-reumatologia` | branch pushada, **PR não aberto** | nós, na volta |
| adjudicação clínica dos 37 só-legado da reumatologia | artefatos prontos, fora do git | médico |
| `305875` — avalizar a chave `waive` | **Novo, sem responsável** | Ops |
| promoção das 4 linhas de HML para PRD | PR `hml → main` + schema em prd | nós + Fábrica |
| PR 7428 (neuroimunologia) | aprovado; falta work item e revisores | Leandro |
| próstata e doenças biliares | **SPECs escritas**, start na sprint | Leandro |

🔴 **Cards em *Comprometido* para o retorno:** `358081` — *[NLP Engine] Pinar a 0.15.4 e fechar os
defeitos que o pin destrava* · `358082` — *[Plataforma NLP] Alinhar contrato de entrada e saida e a
SPEC 27 com o time de Ops*. Os dois, mais o `306066`, carregam a tag de board **`v0.15.4`** e estão
vinculados entre si — o motivo de bloqueio é o mesmo.

⚠️ **Riscos vivos durante a ausência, todos já conhecidos e nenhum novo:** a hepatologia em produção
entrega **6,7%** do que a mesma config produz com embeddings funcionando (`305810`) · o
`cancer_colon` roda `${nlp_engine_version}` e pode trocar de versão sozinho · a reumatologia em
produção está **2,7 pontos abaixo** do filtro homologado, pelo escape do `\b`.

---

## nlp_engine (lib) — `nlp-engine-lib`

**`0.11.1` na `hml`**, tags `v0.9.0`–`v0.11.1` publicadas. Gate: ruff, format, mypy, **628 testes**,
`release-check`.

- `0.10.0` **observabilidade do LLM e empacotamento** — `llm_router_mode` distingue os 3 motivos ·
  `llm_input_chars` · `llm_api_key_origin` · `py.typed` · `setup.py` removido. **Fecha os 4 P1.**
- `0.10.1` **legenda ordinal descendente** deixa de promover. Defeito 1 do card `285305`.
  Critério passou a aceitar corrida monotônica de ±1 que **toque** a categoria mínima.
  Medido: 53 de 314 entregas removidas (16,9%) em run de dev de 20.648 laudos.
- `0.11.0` **falha de infraestrutura não vira decisão clínica** — `require_measure` não rebaixa
  quando a medida faltou por erro de LLM · promoção semântica sem arbitragem do juiz não entrega ·
  a queda para `token_overlap` deixa de ser silenciosa.
- `0.11.1` **a camada semântica deixa de promover trecho negado.** A régua negava e a semântica
  promovia o mesmo texto. Reusa `is_negated_in_sentence_plain`; a âncora que faltava é construída
  com `tokenize_sentence_norm`. Direção default **`left`**, não `both`. Card `298597`.

🔴 **`main` está em `0.11.2`; a `hml` está em `0.12.1`.** Toda a `0.12.x` só existe na `hml`.
ℹ️ **Merge `hml → main` leva a `0.12.0` e a `0.12.1` juntas.** É higiene, não muda decisão, e a
não-regressão está provada (1.075 casos, sha256 idêntico) — o pacote não é opaco.
🔴 **É o que falta para produção poder pinar `0.12.1`:** o feed de prd publica a partir da `main`.
✅ **O feed publica a partir da `main`** — verificado no `azure-pipelines.yml`: `main` → feed
`fabrica-ai`, `hml` → `fabrica-ai-hml`, trigger nas duas. ✅ Confirmado em 08/09: produção rodou a
engine **`0.11.2`** em 07 e 08/09, que é a versão da `main`.
ℹ️ Os **dois destinos são deliberados**: o `UploadVolume` passou a depender do `PublishPyPI`, então
ou os dois recebem a mesma wheel, ou nenhum recebe. Não há duplicidade a resolver — resta apenas
provar o caminho `livre` no próximo bump.

✅ **Validação da `0.11.1` concluída.** Coorte de 4.508 laudos de hepatologia (18/08) contra o run
de referência de 01/09: **803 → 797 relevantes, 6 perdidos, 0 ganhos**. Zero score subiu — a mudança
só remove. ⚠️ A nota de release afirmava impacto zero e **isso não se sustentava**; corrigida.
ℹ️ Medição na branch `docs/0.11.1-impacto-medido`, ainda sem push.

✅ **`0.11.2` ENTREGUE NA `hml`** (PR mergeado), e é a versão que está na `main`.
Defeito P0 **ativo em produção**: `to_plain.py:173` apagava o espaço entre palavra de 1 a 4 letras
e palavra iniciada por acento. `sem` tem 3 letras → `semúlceras` vira **um token** e o negador
some. **6 laudos entregues em 04/09 no ca-estômago dizendo o oposto** (`Tumor; Úlcera` num laudo
que diz *"sem úlceras ou tumorações"*).

- **A função nunca acertou:** 9 regras medidas uma a uma sobre 616 laudos → **825 junções, zero
  legítimas**; as 5 que a justificavam (`çã o`→`ção`) **nunca dispararam**. Removidas 4, mantidas 5.
- **Impacto medido nas 4 linhas** (motor 2× por linha, LLM desligado dos dois lados):
  `cancer_estomago` 17 → **11** (−6, 35%) · `tirads` 47 → 47 · `hepatologia` 5 → 5 ·
  `cancer_rim` 2 → 2. ⚠️ Pré-condição verificada: o texto tratado mudou em 105, 135 e 257 de 400
  nas três que deram zero — sem isso o zero seria medição vazia.
- **Efeito nos dois sentidos:** também faz achado **sumir** (`de úlcera`→`deúlcera`), já registrado
  em 27% dos laudos com úlcera. A correção recupera esses.
- ⚠️ **As homologações das 4 linhas foram feitas sobre texto com o defeito** — recall 0,600 /
  precisão 1,000 do ca-estômago inclui estes FP e precisa ser reavaliado.
- Card `299423` (P1). SPEC `docs/spec-0.11.2-espaco-colado-antes-de-acento.md`. 16 testes onde
  havia **zero**, 4 mutantes mortos.

## `0.12.1` — ✅ ENTREGUE NA `hml`, 15 CARDS EM *PRONTO PARA QA*

Tag `v0.12.1` → `c4af996`. **Os 15 cards da higiene fechados**, comentados com evidência e movidos
no board (`253573`–`253594`). Gate de **sete alvos**: ruff, format, mypy strict, api-ref,
api-surface, doctest, cobertura — **1.104 testes, 87,59% por ramo**.

**Nenhum laudo muda de decisão, e isso foi PROVADO:** harness diferencial de 25 configs × 43 textos
= **1.075 casos, sha256 idêntico** entre `HEAD` e `origin/hml`, com venv por árvore. Smoke em dev
com **4.507 laudos de hepatologia**: zero divergência em `fl_relevante` e `findings`.

- ✅ **A lib passou a emitir log.** Não emitia em lugar nenhum. Nove pontos de fallback contam
  **e** registram na mesma chamada — separados, um dos dois envelhece.
- 🔴 **O contrato declarava a menos, e a lacuna só fechou em TRÊS ondas.** Sete chaves emitidas e
  não declaradas: quatro achadas por auditoria, e **três só apareceram no blob de um run real**
  (`llm_prompt_tokens`, `llm_completion_tokens`, `semantic_evidence`).
  ⚠️ **Verificação de contrato por fixture é estruturalmente insuficiente** — nenhum perfil escrito
  à mão esgota o que produção emite. `tests/chaves_observadas_em_run_real.json` (21 chaves de 1.500
  blobs) virou piso versionado.
- 🟡 **`main` × `hml` da plataforma com o Diego (17/09)** — 59 commits pendentes na `hml` e 15
que só existem na `main`. É o que segura o TI-RADS com as colunas novas em produção.

🔴 **A `0.12.0` do FEED é inutilizável.** Um `409 Conflict` publicou no feed antes da correção do
  contrato, e o feed é **imutável**: mesmo número, conteúdo diferente do Volume. Daí a `0.12.1`.
  ✅ A esteira foi corrigida — o upload ao Volume passou a **depender** da publicação no feed, e o
  `release-check` julga pela **tag**, porque o agente do CI não tem `az` autenticado.
  ⚠️ **O caminho `livre` da esteira segue sem prova**: o build 8418 exercitou só o `publicada`.
  Conferir o Volume logo após o próximo bump.
- 🟡 **Doc do consumidor em revisão — PR `7228`**, `docs/contrato-saida-0.12.1` → `hml`, revisor
  **Diego**, sem voto. Adição pura de 89 linhas, um arquivo, sem conflito. É o par que faltou na
  `0.9.x` e derrubou o TI-RADS.

## `0.12.2` ✅ EM PRODUÇÃO · `0.12.3` ✅ PUBLICADA, CONFIG SEGURADA

✅ **PRs 7243 e 7244 mergeados.** `main` em `e3e821e`, `version = "0.12.2"`.
🔴 **A publicação em `fabrica-ai` foi PULADA, e os dois deploys passaram VERDES.** O gate da
esteira decidia pela **tag**, que é única no repositório, enquanto os feeds são **dois**. A
`v0.12.2` existia desde a publicação em `fabrica-ai-hml`, então o build da `main` concluiu
"já publicada". **Produção segue na `0.11.2`, com o defeito P1 ativo.**
ℹ️ A `0.11.2` está no feed de prd porque foi promovida em 07/09, **antes** deste gate (PRs
7224–7227, de 08/09). A `0.12.1` e a `0.12.2` são as primeiras promoções depois dele.
✅ **PUBLICADA em `fabrica-ai`** depois do PR 7246. Produção passa a instalá-la (usa `latest`).
✅ **VALIDADA NO AMBIENTE** — run em dev de 02/07, 1.052 laudos, `engine_version` confirmado
`0.12.2`. Contra a baseline `0.10.1` do mesmo dia: **3 rebaixados `1 → 0`, ZERO `0 → 1`**, os
três com `require_measure_no_anchor` e os três **PAAF com `TR4` pelado**. Pré-condição: 94 laudos
exercitaram o caminho.
ℹ️ A baseline é `0.10.1` e não `0.11.2`, mas a atribuição se sustenta: `require_measure_no_anchor`
só existe a partir da `0.12.2`, e nenhuma outra mudança se materializou na coorte.
ℹ️ **Esta coorte é a baseline da `0.12.3`:** o aceite dela é o espelho — os 3 PAAF voltam
(`0 → 1`) e nenhum dos outros 1.049 muda.
⚠️ Um run anterior, de 18/08, deu **zero divergência**: os 9 `TR4` pelado daquele dia não são PAAF
e as outras condições do gate coordenado os protegem. Coorte sem a população não mede nada.

🟡 **Correção pronta e pushada:** branch `fix/gate-do-feed-por-destino`, commit `898e6dc`, da
`hml`. O feed de destino passa a decidir (`ARTIFACT_FEED_NAME`), a consulta usa o índice `pip`
com a credencial do `TwineAuthenticate` — funciona no agente, que não tem `az` —, e o
`TwineAuthenticate` roda **antes** do release-check. Medido: `fabrica-ai` ia de `publicada`
para **`livre`**; `fabrica-ai-hml` segue `publicada`.
⚠️ **Precisa ir à `main` para destravar**, e é o merge lá que publica a `0.12.2` em prd.

✅ **PR 7240 mergeado**, `5588d5c` na `hml`, **tag `v0.12.2`** publicada. Feed `fabrica-ai-hml` OK.
🟡 **Promoção para a `main` partida em DOIS PRs**, para o Ops poder pinar sem arrastar a mudança de
comportamento: **PR 1** `release/0.12.1-para-main` (`0a3c1e0`, 94 arquivos) e **PR 2** `hml → main`
(10 arquivos, só a `0.12.2`). Os dois abertos, aguardando Diego, João e Gabriel.
🟡 **SPEC `0.12.3`** escrita — o gate dispensado por evidência alternativa (`waived_by_text`).
Branch `docs/spec-0.12.3-gate-dispensado`, commits `0b6d72b` e `531376d`, **sem push**.
🔴 **A `0.12.3` NÃO fecha o caso do negócio sozinha:** o exame reportado nunca chega ao motor —
o `gold_filter` não seleciona `punção aspirativa por agulha fina guiada por ultrassonografia`.
São **67** punções citando TI-RADS 4 em 16 dias, contra 36 rebaixadas pelo gate. A causa maior é
**config**, não biblioteca.
**Não pushada.** Card `300200` (Defect P1). SPEC no repo.

**Corrige defeito ATIVO em produção**, reportado pelo negócio em 08/09 como "TI-RADS entregando
sem achado". Critério `gate_relevance` com `require_measure: True` cuja âncora não é reconhecida
**sumia do gate** — e a promoção ordinal que ele deveria condicionar saía entregue sem nunca ter
sido conferida. Medido: **36 de 1.032 entregas** como `TR4` pelado.

🔴 **O gatilho é o TIPO DE EXAME: laudo de PAAF cita a categoria TI-RADS sem escrever "nódulo".**
Em 25/08–09/09, PAAF tem **36 `TR4` pelado em 613 laudos** contra 21 em 20.153 dos demais exames.
⚠️ **A atribuição anterior ao encoding corrompido estava errada** — o mojibake se concentrou nos
mesmos laudos de PAAF (40 de 46), e a correlação foi tomada por causa. A janela do A/B não tem
**nenhuma** ocorrência de `U+FFFD` e os quatro rebaixam assim mesmo. O mojibake é real, é da
montagem da entrada, e **parou depois de 03/09** (24 em 26/08, 21 em 31/08, 1 em 03/09, zero desde).

⚠️ **Não é regressão, é premissa.** A SPEC da `0.11.0` §1.3 classificou os três `skipped_*` como
"não se aplica", e está correta nos termos dela. Faltou prever que âncora ausente carrega **dois
sentidos opostos**: o achado não existe, ou o achado existe e não foi reconhecido.

- Gate de sete alvos: **1.113 testes, 87,56%** por ramo. **6 mutantes mortos**, reconfirmados
  depois da refatoração que o `C901` impôs (a mudança levou a função a 16; extraí
  `_registrar_ancora_ausente` em vez de silenciar com `noqa`).
- ⚠️ **A combinação "sem âncora + `require_measure`" tinha ZERO cobertura** — por isso o defeito
  passou. O teste existente usa critério que não declara `require_measure`.
- 🔴 **Troca falso positivo por falso negativo no laudo de PUNÇÃO** — o caso de maior suspeição
  clínica, o nódulo já selecionado para PAAF. ⚠️ **A fuzzy lexical NÃO é o par:** não existe
  `nódulo` corrompido para casar, a palavra não foi escrita. O par correto é a **exceção de PAAF**
  proposta pelo negócio (categoria vale sem evidência de tamanho quando há punção), hoje anotada
  em *A refinar* — e que esta correção promove a dependência direta.
- ✅ **A/B FECHADO, aceite atendido.** 4.321 laudos do TI-RADS em produção (07–09/09), motor duas
  vezes no mesmo processo, LLM e embeddings desligados dos dois lados: **4 rebaixados `1 → 0`
  (0,97% de 411 entregas), ZERO promovidos, 4 de 4 com `require_measure_no_anchor`**. Pré-condição:
  **306** laudos exercitam o caminho (alvo ≥ 30). Os quatro são `TR4` pelado, com `id_exame`
  sequencial. `release-check` coerente e **6 mutantes** reconferidos mortos.
  ⚠️ Quatro tentativas anteriores foram descartadas por defeito do harness — comparar contra
  produção (que chama LLM na camada quantitativa), ler o `llm_called` de topo em vez do
  por-critério, tratar erro de rede como fim-de-dados, e estourar o teto de 25 MB por resposta.
  ℹ️ **Isto mede o delta de decisão, não o ambiente.** Falta o run em dev pinando a `0.12.2` — o
  que exige a versão publicada no feed de dev, ou seja, o PR para a `hml` primeiro.

### `0.12.3` — o par da `0.12.2`

✅ **Mergeada (PR 7248), tagueada `v0.12.3`, publicada nos DOIS feeds.** `main` em `bc1f052`.
🟢 **Inerte:** `waive` é opt-in e nenhuma config a declara — provado no ambiente, run de 02/07 com
a config `0.8.0` deu **zero divergência** contra a `0.12.2`.

**Medido duas vezes, por caminhos independentes, mesmo número:**

| | A/B local | pelo runner |
|---|---|---|
| promovidos `0 → 1` | **3** | **3** |
| rebaixados `1 → 0` | **0** | **0** |
| com `gate_waived_by` | 3 de 3 | 3 de 3 |
| dispensas aplicadas | 28 | 28 |

ℹ️ **28 dispensas mudaram 3 decisões** — dispensar um critério só importa onde o gate ia rebaixar.
Os 11 de outros exames que só *mencionam* punção não alteram nada.

🔴 **PR de config SEGURADO.** Branch `tirads/feature/waive-paaf` (`f8a16d7`, config `0.9.0-tirads`)
pushada, **PR não aberto**: chave nova de config e dois campos novos no blob impactam o processo do
Ops e exigem alinhamento prévio. Proposta registrada no card `283647`.
✅ **Guardada em dois lugares (11/09):** a branch (`f8a16d7`) e uma cópia em
`docs/motor-nlp/_versoes-estaveis/ntb_ia_tirads_config_0.9.0-waive-paaf.py`. O resíduo do braço de
baseline do A/B, que revertia o arquivo para `0.8.0` no working tree, foi **descartado** — a árvore
do repo da plataforma está limpa e o commit segue íntegro.
⚠️ **A regra foi quebrada nesta entrega** — `waive`, `gate_waived_by` e `gate_waived_error`
entraram na lib antes do alinhamento. Exposição real é zero (nada emite sem config), mas o
procedimento é alinhar antes. Memória ampliada para cobrir **chave de config**, não só saída.

⚠️ **O `gold_filter` segue sem punção, e é a causa MAIOR:** 67 exames citando TI-RADS 4 em 16 dias
nunca chegam ao motor, contra 36 rebaixados pelo gate. Sem medição e sem card — filtro de entrada
só se mede rodando.

## `0.13.0` — ✅ EM PRODUÇÃO NOS DOIS FEEDS · 🟡 NENHUMA LINHA A EXECUTA AINDA

✅ **Mergeada e promovida.** PR 7357 (`→ hml`) e **PR 7362 (`hml → main`, 21/09 12:51)**. `main` em
`243a622`, versão `0.13.0`, tag `v0.13.0` sobre `01c6363` e ancestral da `main`.
✅ **Publicada nos DOIS feeds** — `fabrica-ai-hml` e **`fabrica-ai` (produção)**.
🟢 **O gate do feed funcionou nos dois destinos** — era o que falhou na `0.12.2`, quando a
publicação em prd foi **pulada com deploy verde** porque o gate decidia pela tag (única) e os feeds
são dois. Os dois ramos agora foram exercitados: `livre` em hml e publicação efetiva em prd.
**Fecha a lacuna `L3`.**

🟡 **Disponível não é em execução:** as seis linhas pinam **`0.12.3` literal** nas definições de
job. **Nenhuma roda a `0.13.0`**, e por decisão — ver o plano de bumps.

✅ **VALIDADA EM AMBIENTE (21/09) — delta zero em 4.172 laudos reais.**
📄 `_processo/medicoes/validacao-0.13.0-em-ambiente-2026-09-21.md`. `cancer_rim`, janela de 07/08, dev, mesma
branch e mesma config dos dois lados; **a única variável foi a versão da lib**. Zero divergência em
**catorze campos** — seis de decisão e oito da trilha.
🟢 **Pré-condição: `[sentence_transformers]` em 4.172 de 4.172**, zero `token_overlap`, zero
`FileNotFoundError`. **Primeira vez que a camada semântica roda com modelo real nesta linha**, e
primeira vez que o **Model do Unity Catalog** é exercitado em `hml`.
🟢 **Prova o que o golden local não alcançava:** o singleton do spaCy **sob o driver do Spark** —
o `253579` corrigiu uma condição de corrida, e `segmentation_coverage` idêntico em 4.172 de 4.172 é
a evidência de que o pipeline chegou completo a todas as threads.
⚠️ **A validação rodou DEPOIS do merge** (12:51 → 15:13), não antes, contrariando a recomendação
escrita na própria descrição do PR. Registrado como aconteceu.

🔴 **Achado do caminho, e vale card:** o **resgate de pendentes não filtra por data**
(`nlp_ia_02_input.py`) — lê a tabela de entrada inteira e traz tudo que não está
`processado = true`, em qualquer janela. `include_pending` tem default `True` e **não tem widget**.
Havia **43.036 pendentes** acumulados em dev, e eles inflavam uma janela de 22.281 para **60 mil**.
O sintoma é **volume inexplicado, não erro**. É irmão do `298596` e entra no mesmo card.

## `0.14.0` — ✅ NOS DOIS FEEDS · ⚠️ NÃO ADOTAR NA HEPATOLOGIA ANTES DO `full_doc`

📄 SPEC `nlp-engine-lib/docs/spec-0.14.0-juiz-nao-cria-relevancia.md`. PR **7370** (`→ hml`) e o
PR `hml → main`, os dois mergeados em 21/09. `hml` em `6c373b5` com a **tag `v0.14.0`**; `main` em
`7b06e05`, `version = "0.14.0"`. Gate: **1.249 testes, 88,16%** por ramo.
✅ **Publicada nos DOIS feeds, verificado no próprio feed e não no deploy verde** —
`fabrica-ai` (prd) e `fabrica-ai-hml`. **Segunda vez seguida** que o gate por destino funciona nos
dois ramos.

**A invariante do `[P0-29]` passou a ser APLICADA, não declarada:** nenhum laudo sai com
`fl_relevante = 1` e `n_positive_spans = 0`, **por nenhuma via**.

🔴 **Eram DUAS vias e o card `283648` só descrevia uma.** Além de o juiz promover, **a camada
semântica promove e o juiz nunca é consultado**: em `hybrid`, `fl = 0` com
`semantic_score >= similarity_threshold` vira `fl = 1`, e a arbitragem só ocorre **dentro** da
banda. **Nenhuma banda fecha as duas** — estreitar para conter a via A **abre** a via B.

✅ **A correção é um STEP próprio — `guard_evidence` —, não extensão da reversão existente.**
A reversão da `0.11.0` (`decision_pipeline.py:719`) vivia **dentro** do ramo em que o juiz rodou
**e** condicionada a `llm_error`: cobria *"o juiz foi chamado e falhou"*, não *"o juiz nunca foi
chamado"*, que é o caso corrente.
🔴 **E colar a guarda ao juiz teria comportamento diferente nas duas ordens do pipeline:** na ordem
`legacy` **três promoções rodam depois** dele (`ordinal`, `quantitative`, `semantic`); na `target`
— **default desde a `v0.6.0`** — o juiz é o penúltimo. A guarda entrou como **penúltimo passo das
duas ordens**, onde a decisão já está pronta. Invariante de saída se verifica na saída.
✅ **Arbitragem confirmada SUSTENTA a promoção semântica** — o que se proíbe é a semântica entregar
sozinha. A primeira versão revertia mesmo com o juiz tendo arbitrado; quem pegou foi um teste da
`0.11.0`, e a exemção `juiz_arbitrou` é parte do desenho.
✅ **Promoção com evidência PRÓPRIA é exemptada** — `ordinal_promotion`, `ordinal_only` e
`quantitative_promote` não dependem de span léxico.

✅ **Os itens de contrato**, e são o que a plataforma precisa: **`semantic_promoted`** no topo do
payload — emitido **condicionalmente**, só quando verdadeiro — e `llm_prompt_tokens` /
`llm_completion_tokens` no bloco `quantitative.<criterio>`, com os **mesmos nomes do caminho do
juiz**, para quem soma o custo do dia somar **uma** coluna.
ℹ️ **`llm_promoted` é estado interno, NÃO vai ao payload** — quem torna a via do juiz isolável é o
`decision_source`, que passa a `llm_promotion_without_rule_evidence` quando a guarda age.
ℹ️ **Provedor sem `usage` deixa os campos AUSENTES, não zero** — zero diria que a chamada não custou.

✅ **Impacto medido no golden contra a `v0.13.0`: 102 rebaixados, ZERO acrescidos** — 24 pela via
semântica, 78 pela via do juiz. Perfis sem semântica e sem juiz: **delta zero**.
⚠️ **Não é estimativa de produção:** o corpus usa `similarity_threshold` 0,10 para **forçar** a via
B; produção usa 0,80 a 0,92. A medição em coorte real (`CA5`, `CA6`, `CA7`) segue pendente.

✅ **NÃO-REGRESSÃO PROVADA EM AMBIENTE (21/09) — delta zero em 4.172 laudos.**
📄 `_processo/medicoes/validacao-0.14.0-em-ambiente-2026-09-21.md`. `cancer_rim`, 07/08, executada **por CLI**
(`databricks jobs submit`, run `25032614221567`, cluster `ic-fabrica-ia-dlq`), mesma coorte da
`0.13.0` — a baseline já estava gravada, então custou **meia corrida**. Zero divergência em onze
campos; `[sentence_transformers]` em 4.172 de 4.172.
🟢 **Controle negativo que vale:** **13 promoções `llm_router_llm_positive`** atravessaram a guarda
**intactas**, todas com `n_positive_spans > 0`. Mas o caminho corrigido **não foi percorrido** nessa
linha — zero promoções semânticas, zero critérios quantitativos. É controle, não medição.

## `CA5` MEDIDO — ⚠️ leitura CORRIGIDA em 22/09, ver a seção da causa raiz adiante

📄 `_processo/medicoes/medicao-ca5-p0-29-coorte-dirigida-2026-09-21.md`. A/B **por `id_exame`**, não por
janela: run `470903865428642`, dois braços sequenciais, notebook de bancada
`plataform/ntb_ia_bancada_p0_29`, **41 laudos** da hepatologia (eram 36 em 16/09).
🟢 **Pré-condição: o braço baseline REPRODUZIU o defeito em 41 de 41** — `fl = 1` com
`n_positive_spans = 0`, juiz chamado em 41, zero erro, modelo semântico real em 41.

🔴 **A `0.14.0` remove 4 dos 41 — 9,8%. Não 41.** Zero acrescidos.

| grupo | `semantic_score` | o que aconteceu |
|---|---|---|
| **37 mantidos** | 0,795 a 0,995 | ≥ `similarity_threshold: 0.78` → **a semântica promoveu**; o juiz confirmou → `juiz_arbitrou` → **a guarda EXEMPTA** |
| **4 revertidos** | 0,686 a 0,773 | < limiar → **o juiz** levou `fl` de 0 a 1 → `llm_promoted` → **revertidos** |

✅ **O campo novo fez o trabalho dele:** `semantic_promoted` saiu em 37 de 41 e o diagnóstico coube
em **uma consulta**, sem delta entre runs. É o `CA3`.

🔴 **O `CA1` e o `CA2` da SPEC se contradizem, e a implementação seguiu o `CA2`.** O `CA1` diz
*"nenhum laudo sai com `fl = 1` e `n_positive_spans = 0`, por nenhuma via"*; o `CA2` diz que a via
semântica entrega **quando há arbitragem**. Não podem valer juntos.
⚠️ **E a exemção não foi imposta pelo teste que a motivou.** `test_juiz_responde_e_decide_normalmente`
(`0.11.0`) verifica que o `decision_source` **não é** `semantic_promotion_unarbitrated` — **não**
verifica que o laudo é entregue. Um rótulo distinto para *"arbitrado, porém sem evidência de régua"*
satisfaria o teste **e** reverteria os 37.

ℹ️ **A leitura abaixo foi CORRIGIDA em 22/09** — ver a seção da causa raiz. O que segue descreve o
que foi medido, não a conclusão:
🔴 **A pergunta que parecia decidir:**
- **não conta** — a invariante é *o juiz filtra, nunca cria relevância*, e a tabela do próprio step
  classifica parecença e opinião do LLM como não-achado. Duas não-evidências somadas seguem não
  sendo evidência. **Remove 41 de 41.**
- **conta** — a cascata é régua → semântica **alarga** → juiz **estreita**; reverter esvazia o
  `decision_mode: hybrid` nas linhas sem casamento léxico. **Remove 4 de 41.**

⚠️ **O número não reproduz produção:** o A/B rodou com **modelo real** de embeddings nos dois braços,
e a mesma linha em produção cai em `token_overlap` em 99,3% dos laudos — com outro backend os
`semantic_score` mudam e a partição 37/4 muda junto.
ℹ️ **Isto também explica o golden:** lá os 24 rebaixados pela via semântica caíram porque os perfis
do corpus **não têm juiz** para arbitrar. Com juiz ligado, não caem.

✅ **`CA4` FECHADO na `tirads` (run `130126794270375`).**
📄 `_processo/medicoes/medicao-ca4-tokens-camada-quantitativa-2026-09-21.md`. 120 laudos, 1.080 critérios
quantitativos, **162 chamadas ao LLM**: `0.13.0` com **zero** token registrado, `0.14.0` com
**162 de 162**. Somam 158.141 de prompt e 18.929 de completion — **976,2 e 116,8 por chamada**.
🟢 **Zero mudança de decisão:** 59 entregues dos dois lados. O item é aditivo, como declarado.
🟢 **Controle positivo não previsto:** o único laudo com `fl = 1` e `n_positive_spans = 0` nos dois
braços tem `decision_source: ordinal_promotion` e **a guarda o deixou intacto** — categoria RADS é
achado clínico declarado, e está na lista de exemções. Nenhuma medição em ambiente tinha tocado
esse ramo.
ℹ️ Os 976,2 por chamada ficaram perto dos **1.004,1** do juiz da hepatologia, o que sugere **prompt
truncado** — o tamanho do laudo não governa a contagem linearmente. **Não serve de base para
estimar outras linhas.**

**Critérios do `283648`:** ✅ `CA2` `CA3` `CA4` `CA5` `CA6` · 🔴 `CA1` **não atendido** como
redigido · 🟡 `CA7` depende da decisão de régua.

## 🔴 CAUSA RAIZ CORRIGIDA (22/09) — a régua estava cega por SEGMENTAÇÃO, não por parecença

📄 `_processo/medicoes/medicao-ca5-p0-29-coorte-dirigida-2026-09-21.md`, adendo 2. O run
`222180888862290` persistiu **qual termo da régua casou e com que trecho**, e derruba a leitura
anterior.

| score | termo da régua | trecho do laudo |
|---|---|---|
| **0,995** | `hepatopatia crônica` | **"Hepatopatia crônica"** |
| 0,948 | `doença hepática` | "- Doença hepática gordurosa" |
| 0,773 | `circulação colateral` | **"Circulação colateral periesplênica"** |
| 0,752 | `doença hepática` | "Esteatose hepática" |

🔴 **Não é sinônimo por parecença — é o termo LITERAL da régua**, na parte do laudo que a régua
não viu.

| | |
|---|---|
| a camada semântica recebe | `st.treated` — o laudo **INTEIRO** (`decision_pipeline.py:558`) |
| a régua recebe | o texto **segmentado** |
| dos 44 casos do `[P0-29]` | **44 de 44 com perda de segmentação** |
| cobertura mínima | **0,006** — a régua viu **0,6%** do laudo |

✅ **A hepatologia é a ÚNICA das sete linhas com `mode: auto`** — `cancer_colon`,
`cancer_estomago`, `cancer_rim`, `reumatologia`, `tirads` e `transplante_pulmao` usam `full_doc`.
E é a única com `similarity_threshold: 0.78`; as outras vão de 0,80 a 0,92. **Sempre foi a
segmentação.** A banda explica o juiz ser *alcançado*; não explica a ausência de evidência.

### O que isso cancela

- 🔴 **Subir o limiar para 0,92 está CANCELADO** — apagaria achado literal. A proposta nasceu de
  analogia com o `cancer_rim`, não de evidência; o que precisava ser olhado era **o que casou**.
- 🔴 **A guarda da `0.14.0`, aplicada à hepatologia hoje, remove VERDADEIRO POSITIVO** — 2 de 2
  nesta coorte. A lógica está certa; **a premissa falha**: `n_positive_spans = 0` quer dizer *"a
  régua não achou"*, e estava sendo lido como *"não há achado no laudo"*.
- ✅ **O `300202` deixa de ser higiene e vira a CAUSA RAIZ do `283648` na hepatologia.**

🟢 **Zero dano em produção** — nada está pinado, as seis linhas rodam `0.12.3`, e a decisão de
21/09 segura o pin até a `0.15.0`. Foi exatamente o que ela comprou.

### 🔴 Ordem da passada única da hepatologia

**`segmentation.mode: full_doc` PRIMEIRO, guarda de evidência depois.** Invertido, a guarda
rebaixa o que a régua deveria ter achado — e o efeito seria lido como "a correção funcionou".

### A conclusão NÃO transfere — a `0.14.0` tem alvo real

**Ateromatose usa `full_doc`** (config `0.2.3`): lá a régua enxerga o documento inteiro, e as
**33 promoções semânticas em 44** do `0.2.1`, mais o juiz acionado em **6.111 de 7.500** no `0.2.0`,
são promoção sem evidência **de verdade**. A guarda está certa naquele caso. A linha já desligou a
semântica na `0.2.2`.

### `emit_as_finding` — reenquadrado

`embeddings.emit_as_finding` (`semantic_expand.py:447`) é **opt-in, default `False`, inerte nas
sete configs**. Ele **incrementa `n_positive_spans`** e materializa o termo casado como finding.
ℹ️ **Não é só um risco à guarda** — é o que faltaria para o achado semântico virar visível e
mensurável: hoje a promoção sobe `fl` e deixa `findings` **vazio**, então `measure` e `vet` a
jusante **não enxergam** o achado e o `require_measure` não tem o que conferir.
⚠️ **Mas com a régua cega ele mascararia o defeito** em vez de corrigi-lo. Ordem: `full_doc`
primeiro. Decisão de 21/09 mantida: **não ativar agora**.

⚠️ **Artefatos de bancada a apagar quando o card fechar:** notebook
`plataform/ntb_ia_bancada_p0_29` no workspace e as tabelas `tb_bancada_*` em
`diamond_fabrica_ia_dev`.

## `0.15.x` — ✅ `0.15.3` VALIDADA EM COORTE REAL · 🟡 `0.15.4` PRONTA, NÃO PUSHADA

📄 SPEC `nlp-engine-lib/docs/spec-0.15.0-vinculo-lesao-medida.md`. Card `306034` — *[NLP Engine]
TI-RADS entrega a medida do nódulo errado: não existe vínculo*.

| versão | tag | estado |
|---|---|---|
| `0.15.0` | `c4b4cae` | 🔴 **nove defeitos.** Não pinar |
| `0.15.1` | `6cc3e20` | 🔴 **sete dos nove ainda presentes.** Não pinar |
| **`0.15.2`** | **`38e6857`** | 🔴 rebaixa TR4 real por `categoria_ausente` — 73 em 14.710 |

🔴 **O feed é IMUTÁVEL: as três ficam lá para sempre.** Quando a decisão de pin chegar, `0.15.0` e
`0.15.1` **não podem ser candidatas**. Está escrito no `RELEASE.md` e nos PRs.

**A régua, em duas linhas:** todo nódulo entregue vem com a dimensão e a classificação **que são
dele**; exceção única é o laudo de punção, onde qualquer TR4 vale sem tamanho.

🔴 **Tamanho do defeito original, em 13.264 laudos reais:** 2.729 dos 4.771 com nódulo têm mais de
um (57%). **408 entregas em 15 dias** carregam medida de laudo multilesão sem vínculo verificado.

### 🔴 A `0.15.1` foi publicada e SERIA um desastre — 64 entregas falsas em 15 dias

A/B em coorte real (TI-RADS, 08–22/09, 14.939 laudos, dois braços de 121 min): **117 rebaixados e
65 PROMOVIDOS**. Adjudicação por leitura: **rebaixamentos 5/5 corretos, promoções 5/5 FALSAS**.

**A causa dominante é a LEGENDA do ACR** — a linha `TR4 (4-6 pontos) … PAAF: ≥ 1,5 cm` tem
categoria **e** medida, e o vínculo por linha casa as duas. 48 das 65.
🔴 **E a lib já calculava quais menções são legenda** (`legend_indices`, desde a `0.10.1`). **O
vínculo consumiu a lista crua.** Não era lógica errada: era reuso errado.

### ✅ `0.15.2` — validada contra 775 laudos REAIS, nove defeitos corrigidos

📄 `_processo/medicoes/investigacao-vinculo-lesao-medida-2026-09-23.md`. Seis estratos: os que mudaram no
A/B, estruturado, legenda, PAAF, controle de TR4 e controle geral.

🟢 **O que torna a medição possível sem depender do ambiente:** os valores que o extrator LLM **de
fato devolveu** ficaram gravados no braço `0.14.0`. O harness os **reproduz**; os dois modos rodam
**no mesmo processo**, com a config real `0.8.0-tirads`.
🟢 **Fidelidade: 773 de 775 (99,7%)** — o modo "sem vínculo" reproduz o `fl` gravado.

| | |
|---|---|
| promoções falsas **eliminadas** | **64 de 64** |
| promoções **novas** | **0** |
| rebaixamentos mantidos | **107 de 111** (os 4 têm **cisto TR4 ≥ 1 cm**; o gate protege certo) |
| contra a `0.14.0` | **123 rebaixados, ZERO promovidos** |
| golden contra a `v0.14.0` | 8 rebaixados, zero promovidos, seis caminhos exercitados |

**Os nove:** fusão de linhas independentes · `Localização` como biometria · 🔴 **`VR: < 30 cm/s`
lido como 30 cm** (existe desde a `0.15.0`) · bloco engolindo a legenda · ponto inicial · linha de
recomendação · biometria fundida em sentença · `categoria_ausente` votando a favor · **menção de
categoria sem dono de achado**.

🔴 **Os dois últimos são o gate COORDENADO**, que só rebaixa quando **todos** os critérios dão
`False`. `Cistos colóides … 1,2 cm. **ACR TI-RADS: 1**` fazia o critério do cisto proteger um
nódulo **TR4 de 0,7 cm**. A régua é *"TR4 só aprova com nódulo/cisto ≥ 1 cm"* — **da lesão TR4**.

✅ **Três correções foram REMOVIDAS por redundância**, com os 8 laudos reais como oráculo de
mutação: 4 mortos, 3 sobreviveram. Sobreviver ali é código sem propósito demonstrado.

Gate de sete alvos: **1.307 testes, 88,29% por ramo**, **11 mutantes mortos**.

### ✅ `CA4`/`CA5` FECHADOS EM COORTE REAL — e a coorte reprovou a `0.15.2`

📄 run `260243524255126` (TERMINATED SUCCESS, ~131 min), TI-RADS em dev, `0.14.0` × `0.15.2` no
mesmo processo. **14.710 laudos pareados por `id_exame`** — os dois braços rodaram em dias
diferentes, então 1.558 órfãos ficaram **fora** do delta, não somados a ele.
🟢 **Pré-condição perfeita:** zero campo novo no braço baseline · **1.288** com vínculo estabelecido ·
1.225 com **duas ou mais** candidatas · 287 `categoria_ausente` · 9 `sem_medida_no_laudo`.

🔴 **194 rebaixados, ZERO promovidos — e 73 deles são um DEFEITO NOVO, não a correção.**

| via do rebaixamento | laudos | veredito |
|---|---|---|
| vínculo estabelecido, medida da lesão < 1 cm | **121** | ✅ é a correção do card `306034` |
| `measure_lesion_skipped: categoria_ausente` | **73** | 🔴 **defeito da `0.15.2`** |

**Os dois números que estavam abertos, fechados:**
- **`CA5`** — dos 34 rebaixados "unilesão por menção", **33 têm dois ou mais nódulos no texto**
  (multilesão de fato). **Um só é unilesão de verdade**, e é rebaixamento **errado**.
- **Causa não atribuída: ZERO.** Os 194 se repartem integralmente nas duas vias acima.

### 🔴 `categoria_ausente` REBAIXA, e é a terceira vez que a mesma classe passa

`quantitative.py:1640-1643` grava **`met = False`** quando nenhuma lesão é vinculada à categoria.
Mas *"nenhuma lesão foi vinculada"* carrega **dois sentidos opostos**: a lesão não existe (a
categoria veio da legenda), ou **a lesão existe e o parser não soube nomeá-la**.

Medido nas linhas que carregam a categoria, nos 73: **44 têm a categoria numa linha que também
traz a medida** — o vínculo deveria ter funcionado ali. Casos textuais lidos:
`medindo 1,1 x 1,7 cm. ACR TI-RADS 4.` · `N4 - Nódulo … 2,3 x 1,6 x 1,4 cm. (TI-RADS - TR:4)` ·
`Formação quase totalmente sólida … 1,6 x 1,2 cm. (TI-RADS 4)`. **São TR4 reais acima de 1 cm.**

🔴 **É a mesma classe da `0.12.2` (âncora ausente) e da `0.15.0` (vínculo ausente), pela terceira
vez.** O `_gate_met_de` enumera na própria docstring **três** causas de `met is None` e protege
duas. `categoria_ausente` é a **quarta**, da mesma natureza, e nem chega lá — grava `False` antes.

### ✅ `0.15.3` EM PRODUÇÃO NOS DOIS FEEDS (23/09) · 🔴 SEM TAG

📄 SPEC `nlp-engine-lib/docs/spec-0.15.3-categoria-orfa.md`. PRs **7397** (`→ hml`) e **7398**
(`hml → main`), os dois mergeados. `hml` em `54e3a8c`, `main` em `e7b6355`, as duas em `0.15.3`.
✅ **Publicada nos DOIS feeds, verificado no próprio feed** (API de packaging), não no deploy
verde. Builds 8686 e 8687, os dois `succeeded`. 🟢 **Terceira vez seguida** que o gate por destino
funciona nos dois ramos.
✅ **Tag `v0.15.3` criada e pushada em 24/09** — `4614613`, anotada, sobre **`54e3a8c`** (merge do
PR 7397 na `hml`), confirmada pela REF. ⚠️ A indicação anterior de `e7b6355` estava errada: a
convenção do repositório é a tag apontar para o **merge na `hml`**, não para a promoção à `main` —
`v0.15.1` → `6cc3e20`, `v0.15.2` → `38e6857`. A esteira publica no merge e a tag é **manual**;
quem acusou a falta foi o `release-check`.
Gate contra a árvore **mergeada** (`git diff` de código vazio contra a `main`): **1.311 testes,
88,30% por ramo**, e o `release-check` só reprova na tag.

🔴 **`met = None` cego NÃO servia** — ressuscitaria o defeito 8 da `0.15.2`. São **TRÊS** estados:

| estado | efeito |
|---|---|
| a categoria gateada **nem aparece** no laudo | **rebaixa** — não há nada a atribuir |
| aparece e **outro critério** a atribuiu | **rebaixa** — ausência verificada nesta espécie |
| aparece e **ninguém** a atribuiu | **protege** — limite do parser |

🔴 **O primeiro estado foi achado pelo GOLDEN, e a primeira versão da correção não o tinha:**
protegia laudo cujo `TR4` nem existia e entregava nódulo de 0,4 cm — **6 em 754**.

✅ **Golden contra a `v0.15.2`, mesmo script dos dois lados: UMA decisão muda**, e é o laudo de
categoria órfã (`0 → 1`). **Zero rebaixados.** Os oito perfis sem camada quantitativa ficam byte a
byte idênticos; os 30 blobs que mudam trazem **só os dois marcadores novos**, com zero valor
preexistente alterado — conferido, não suposto. **6 mutantes, 6 mortos.**

🔴 **O golden tinha DOIS pontos cegos, os dois fechados:** não havia laudo de categoria órfã no
corpus, e **o perfil quantitativo declarava a âncora sem `text`**, enquanto produção declara
`'text': r'n[oó]dul'`. Sem o `text` o recorte por região fica desligado e o caminho nunca rodava —
**perfil mais fraco que o real não mede**.

⚠️ **Contrato:** `require_measure_categoria_orfa` e `require_measure_categoria_ausente`;
`met` passa de `False` a `None` nos blocos protegidos. `REFERENCIA-PARAMETROS` atualizada no
mesmo commit.
🟡 **Falta a medição em coorte real** — o espelho da `0.15.2`: os 121 por vínculo estabelecido
permanecem, e os 73 por `categoria_ausente` são reavaliados.

🔴 **Nenhuma das `0.15.x` publicadas é pinável, e o feed é imutável.**

### ✅ `0.15.3` VALIDADA EM COORTE REAL — acertou o alvo, e revelou o próprio defeito

📄 run `109956348322922` (TERMINATED SUCCESS, 134 min), TI-RADS em dev, **16.014 pares**.
✅ **150 promovidos, ZERO rebaixados**, 150 de 150 com marcador, **zero sem causa declarada**, e
**zero divergência** no caminho do vínculo estabelecido.
✅ **A três versões, em 14.658 trios:** os **121** rebaixamentos do card `306034` seguem de pé
(**121 de 121**, zero regressões) e os **73** indevidos foram **recuperados integralmente**.
A partição `121 / 73` reproduz **exatamente** a medida no A/B original.

🔴 **Mas 65 das promoções são defeito da própria `0.15.3`.** A guarda de órfandade protegia mesmo
quando o laudo já refutava: das 143 órfãs com braço de comparação, **76 protegem com razão (E)**,
**65 têm o máximo do documento abaixo do limiar (F, maior observado 0,98 cm)** e **2 não medem
nada (F2)**. **Quase metade da classe.**

### ✅ `0.15.4` EM PRODUÇÃO NOS DOIS FEEDS, TAGUEADA E VALIDADA EM COORTE REAL

📄 SPEC `nlp-engine-lib/docs/spec-0.15.4-refutacao-independe-da-atribuicao.md`. Branch
`fix/0.15.4-refutacao-independe-da-atribuicao`, commit `5c408c9`, da `hml`.

**A regra que faltava já existia desde a `0.15.1`:** *se o maior nódulo do laudo não alcança o
limiar, nenhuma alcança*. O erro foi tratá-la como propriedade **daquele ramo** em vez de
propriedade **da decisão** — o mesmo padrão que a `0.14.0` já tinha nomeado.

✅ **Feita por RPI/SDD, e é o que muda:** parte de uma **tabela de estados completa com contagem
medida em cada célula**. As duas que deram zero (`D` erro de infra, `H` critério composto) estão
declaradas como **sem população nesta coorte**, cobertas por teste — célula sem contagem é célula
não verificada.

| evidência | |
|---|---|
| golden contra a `v0.15.3` | **ZERO promovidos**, 4 rebaixados — só `F` e `F2`, nos dois perfis |
| perfis sem camada quantitativa | byte a byte idênticos |
| oráculo de mutação | **12 mutantes, 12 mortos** (6 novos + os 6 da `0.15.3`) |
| gate de sete alvos | **1.315 testes, 88,32% por ramo**, `release-check` coerente |

⚠️ **Contrato:** `measure_lesion_maximo_documento`. Não é higiene — a `0.15.3` apagava o valor
**antes de persistir**, e por isso o payload não distinguia `E` de `F`: **o defeito era invisível
na saída, não só no código.**

🔴 **Dois erros meus no caminho, os dois pegos por instrumento e não por leitura:** um mutante
sobreviveu porque o caso de teste não discriminava, e **um golden foi gerado enquanto o oráculo de
mutação reescrevia a fonte** — resultado plausível, contra código nenhum. Armadilha 12 registrada.

✅ **ENTREGUE:** PRs **7399** (`→ hml`) e **7401** (`hml → main`), `main` em `cd79880`, build 8698
`succeeded`. **Publicada nos DOIS feeds, verificado no próprio feed.** Tag **`v0.15.4`**
(`7641671`) sobre `0718868`, o merge na `hml` — confirmada pela REF.

### ✅ DELTA PONTA A PONTA MEDIDO — `0.12.3` × `0.15.4`, a coorte inteira

Run `866752815785087` (134 min). A versão **que roda em produção** executada sobre a mesma janela e
config da `0.15.4`. **Não há trecho estimado nem encadeamento de medições vizinhas.**

| 16.021 pares | |
|---|---|
| **rebaixados** | **133** |
| **promovidos** | **ZERO** |
| taxa | 5,85% → 5,02% |
| **`findings` diferente** | **277** — 165 perderam a medida, 108 mudaram, 4 ganharam |

ℹ️ Os **277** são o número que o navegador de fato lê, e não existia em lugar nenhum antes.

📄 **Documento de entrega para o Ops publicado** — organizado por *o que muda para quem consome*.
Destaque: **o campo da medida pode vir vazio onde antes vinha preenchido** (165 laudos), e é o
único ajuste do lado do consumidor.
🔴 **Contrato de ENTRADA levantado (faltava):** só **`nlp.lesion_linking.janela`** é chave nova, e é
opcional. A **`waive`** já existe na `0.12.3` e nenhuma config a declara — inerte, mas muda o
processo do Ops se for ligada, e a config que a ativaria está segurada.

### 🔴 Documentação da plataforma desatualizada — verificado no repo

`specs/27-config-especialidade` afirma *"Nada neste pipeline lê `runtime`"*, e
`ntb_ia_loader.py:105-109` **lê e sobrescreve** o `nlp.llm_router`. O `boas-praticas/02` Passo 6
repete (*"sem efeito técnico"*). Três documentos usam **`0.9.4`** como referência. E há **zero**
menções aos 14 campos novos. Card `299238` é o acumulador.

### 🔴 `cancer_colon` — os DOIS problemas, e é a única

```
cancer-colon-batch   api_cancer_  NAO DECLARA disabled   ${nlp_engine_version}
as outras seis       api_*        declara                0.12.3
```

Sem `disabled`, rodar em homologação **posta inferência no sistema real de Navegação**. Alçada
nossa, uma linha em `cancer-colon-batch.json`.

✅ **`CA8` FECHADO — run `879276610970353`, 133 min, 16.021 pares:**

| | |
|---|---|
| **promovidos `0 → 1`** | **ZERO** |
| rebaixados | **70**, e **zero** fora da classe de órfãs |
| os **121** do card `306034` | **121 ainda rebaixados, 0 regressões** |
| célula `E` (protege com razão) | **78** seguem entregues |
| células `F`/`F2` | **65** passam a rebaixar |
| controle do vínculo | **0** divergências |

🟢 **78 + 65 = 143, a partição fecha.** A taxa cai de 874 para 804 relevantes — exatamente os 70.
🟢 **`engine_version = 0.15.4` em 16.021 de 16.021**, mesma config dos dois lados.
ℹ️ Previsto `E`=76 / `F`+`F2`=67; medido 78 / 65. A previsão usava o braço `0.14.0` como **proxy**
do máximo; dois laudos caíram do outro lado da fronteira. Variância do extrator, não de regra.

🔴 **Três afirmações erradas minhas durante o deploy:** disse que o gatilho `individualCI` não
tinha disparado. Tinha — em **1 segundo**; o build levava 24 min e eu julguei aos 8. O
`az pipelines build list` **só devolve concluídos** por padrão. Memória gravada.

🔴 **A recomendação de fundo segue de pé, e é trabalho de lib:** o vínculo usa `anchor.text` como
**segundo vocabulário de lesão**, e ele diverge da régua — nenhum dos dois cobre `Formação` nem
`Imagens ovaladas`. A lib já calcula os spans dos achados e os **DESCARTA**
(`process_rule_based` devolve só contagens). Expô-los é a mesma correção que a `0.15.0` fez com o
`OrdinalMention`.

🔴 **`CA6` por terceiro segue aberto** — a adjudicação foi de quem escreveu o código, e foi ela que
achou os nove defeitos da `0.15.1` **e** este décimo.

## 🔴 Nada se pina até a `0.15.0` — decisão de 21/09

Produção segue na **`0.12.3`** durante todo o ciclo de bumps. **Um** alinhamento de contrato com o
MLOps no fecho, cobrindo `0.13.0` a `0.15.0`, e só então a decisão de pin. Três rodadas de
alinhamento viram uma, e o risco durante o ciclo é **zero** porque nada é adotado — as seis
definições de job pinam `0.12.3` literal. 📄 `docs/plano-acao-backlog-lib-2026-09.md` §4.1.

🟡 **Plano de bumps** — card `298598` — *Fabrica IA/NLP Engine - Plano de bumps da biblioteca*.
Fila: ✅ `0.12.x` → ✅ `0.13.0` (em produção nos dois feeds) → ✅ `0.14.0` (na `hml`) →
🟡 `0.15.0` (SPEC aberta) → alinhamento de contrato → decisão de pin.

## Resíduos da `0.12.x` — ainda abertos

- 🔴 **CA4 do `253594` `[P3-28]`: 9,4s contra o alvo de 5s.** Era 42s; os três checks de projeto
  inteiro foram para o `pre-push`. ✅ **Decisão: não medir em outra máquina** — o alvo de 5s é
  exemplo no card, e o `mypy` **fica** no commit. O que falta é amostra, e não será perseguido.
- 🟡 **`253590` CA8** — validação por terceiro, não verificável por quem escreveu.
- 🟡 **`253586` `[P2-20]`** — fixtures compartilhadas; é refinamento.

⚠️ **Branches sem push:** `docs/plano-e-specs-ops` e `docs/0.11.1-impacto-medido`. Conteúdo
absorvido; descartar não perde mais nada — a SPEC da `0.13.0` já vive na branch de trabalho.

## Tireoide V2 — ✅ ENTREGUE EM HML

**PR aprovado e mergeado** (PR 7071, `005735e`). Config **`0.8.0-tirads`** na `hml`, exige
`nlp_engine >= 0.9.3`. Branch de origem: `tirads/feature/v2-sem-sangue`.

- **Escopo entregue:** só TR5 e TR4 ≥1 cm (`ordinal_only`), sangue **fora da captação e da
  promoção**, juiz LLM desligado, `findings` no formato `TR4 - Nódulo (1,8 cm)`.
- **Impacto medido:** 8,03% → 2,90% de apontamentos no mesmo dia (~64% menos). Volumetria de
  ordem de grandeza: ~94 laudos e ~62 pacientes/dia. ⚠️ **medida sobre UM dia** — se o Natan
  precisar de número firme, rodar a janela 17/06–11/07.
- ✅ **A dedup da VIEW foi corrigida** (PR 7075): ela reprojetava o histórico inteiro a cada
  `CREATE OR REPLACE` e reenviava laudo já entregue. Agora filtra por `dt_execucao_modelo`.
  Isso reduz muito a necessidade de filtrar `config_version` na view do João — sobra só o dia
  da transição, se o job noturno rodar a `0.1.0` e a `0.8.0` no mesmo dia.
- ✅ **O job noturno passou para a `0.8.0-tirads`.** Verificado: produção em 21/08 com
  `0.8.0-tirads` + engine `0.9.4` — 31.418 laudos, 997 relevantes.
- ✅ **A quebra de 20/08 na etapa `nlp_config` não se repetiu.** A linha roda em produção em
  todos os dias agendados desde 21/08, e em 07–08/09 já na engine `0.11.2`. Nada a investigar.
  ⚠️ Se voltar: **NÃO achatar o `findings` para `list[str]`** — destruiria `regex`, `exclude`,
  `unless` e `skip_organ_gate`, e o pipeline voltaria a rodar **errando em silêncio**.
- 🔴 **CORRIGIDO O ENTENDIMENTO (27/08): sair só com a categoria NÃO é questão de exibição.**
  Os **4 casos que saem como `TR5` puro são 4/4 falso positivo** — o TR veio da **legenda do ACR**
  no rodapé, não de achado. Os `TR4` puros são legítimos. A nota anterior ("melhoria de qualidade,
  não de recall") estava errada.
- 🟡 **Card `285305`** (Defect, P1) — dois defeitos consolidados, ambos na lib.
  ✅ **Defeito 1 CORRIGIDO** na `0.10.1` (tag publicada, na `main`). Produção rodou `0.10.0` em
  02/09 por ser a versão publicada no momento da execução; o próximo noturno pega a `0.10.1`.
  ✅ **Defeito 1 MEDIDO em 03/09, por A/B real** — coorte de 2.177 laudos (02–03/07), dois motores
  na mesma execução: **156 → 133 entregas, 23 removidas (14,7%), zero acrescentadas**. Causalidade
  provada: **23 de 23 mencionam ACR e PAAF**, nenhum removido sem legenda. Convergiu com os 16,9%
  da medição simulada de 01/09. Produção pegou a `0.10.1` em 03/09.
  ℹ️ A medição foi sobre 2 dias, não sobre 17/06–11/07: a baseline daquela janela se perdeu no drop
  das tabelas de dev em 03/09. **Recuperação por `UNDROP` descartada** — o A/B de 2 dias é
  metodologicamente superior ao planejado (dois motores reais em vez de simulação) e o número
  convergiu. Se o negócio exigir a janela cheia, custa dois runs.
  🔴 Falta o **defeito 2** (medida associada ao nódulo errado) — é a `0.15.0`.
  Detalhe original dos dois:
  **(1) legenda ACR** — `_legend_exclude_ids` só reconhece corrida **ascendente por +1 começando no
  rank 0**; a legenda desse emissor é `TR5→TR1`, descendente. 5 categorias, passa o `min_run=4`, e
  escapa. Medido: **6 de 18 pacientes (33%)** no arquivo RJ de 27/08. Provado ponta a ponta com a
  config de produção.
  **(2) medida do nódulo errado** — o critério pede o **MAIOR nódulo do laudo**, não o que é TR4.
  `gate_mets` é por critério, não por menção: não existe vínculo lesão↔medida. Entregamos
  `TR4 - Nódulo (2.1 cm)` num laudo cujo TI-RADS 4 media 0,4 cm. **2 dos 7 TR4** do arquivo.
- 🟡 **A REFINAR (03/09, sugestão do Natan): TR4 valeria sem evidência de tamanho, quando houver
  PAAF.** Hoje o TR4 exige medida (`require_measure: True` + `gates_ordinal_promotion`). A proposta
  é uma **exceção**: PAAF como gatilho alternativo ao tamanho.
  Perguntas em aberto para amanhã: a lib já expressa "gate dispensado quando outro achado está
  presente", ou é feature nova? Qual a volumetria — PAAF é raro e nem todo laudo com PAAF vem sem
  medida, então o ganho precisa ser dimensionado antes de virar trabalho.
  Prioridade **abaixo** dos defeitos em curso. Candidato a delegar.
- ⚠️ **Antes de corrigir, rodar a janela 17/06–11/07** para dimensionar. Fazer **depois da `0.10.0`**,
  para o antes/depois ter uma variável só.
- ℹ️ **O arquivo que o Natan monitora vem de HML, não de produção** — conferido exame a exame.
- ✅ Backup das 3 variantes de escopo em `_versoes-estaveis/` + matriz no cabeçalho da config.
- ℹ️ **V3 (sangue) segue especificada e guardada** na branch `tirads/feature/v3-sangue`.
  Reativar = duas chaves + devolver as palavras-chave de captação.

## Transplante de pulmão

V1 **entregue** (2026-08-06, card 246669). V2 **especificado e parado**.

🔴 **Em produção a linha entrega ZERO** (26/08: 2.699 laudos, 0 relevantes). A config tem
`findings: {}` — não existe caminho léxico, toda relevância vem de `on_met: promote` nos critérios
quantitativos, que dependem do LLM. Com o 403, nada promove e o run fecha em sucesso.
🔴 **`dt_agendada` VAZIO** — não está no agendamento noturno; a única execução em prd foi manual.

🔴 **V2 NÃO autorizada — em backlog até liberação do Natan.** Não trabalhar nela sem esse aval.

- 🟡 Quando for liberada: o bloqueio "promoção sempre vence o gate" foi **parcialmente resolvido**
  na `0.8.5` (`gates_ordinal_promotion`), mas ali a blindagem levantada é a **ordinal**. Falta
  avaliar se o pulmão precisa também de `on_met: demote` para expressar contraindicação — hoje um
  paciente com VEF1 < 30% **e** FEVE < 40% seria encaminhado sendo contraindicado.

## Hepatologia — 🟡 TRABALHO CONSOLIDADO, NÃO FATIADO (decisão de 16/09)

🔴 **Nada de hepatologia se toca isoladamente.** A linha **não está de fato em PRD**: falta o fluxo
complementar depois do processamento, que a plataforma ainda não resolveu. O levantamento do que
falta — estrutura necessária, processos excepcionais com fluxos distintos — é escopo do card
`303791` *Plano de Migração algoritmos final*. Tudo entra **de uma vez só**, quando esse estudo
fechar.

**Fila acumulada para essa passada única:**

- 🔴 **`300202` — `segmentation.mode: auto` é a PRIMEIRA da fila, e virou causa raiz.**
  Única linha assim; descarta IMPRESSÃO/CONCLUSÃO. `segmentation_coverage` < 1,0 em **3.867 de
  4.507**, e nos casos do `[P0-29]` a cobertura mínima é **0,006** — a régua vê **0,6%** do laudo.
  **44 de 44 casos do `283648` são explicados por ela**: o termo literal da régua está no laudo, na
  parte descartada, e quem o reencontrou foi a camada semântica, que recebe o texto inteiro.
  No ca-rim a mesma correção recuperou **+25 laudos em 6 dias**.
  ⚠️ **Tem de vir ANTES da guarda de evidência da `0.14.0`** — invertido, a guarda rebaixa o que a
  régua deveria ter achado. Exige A/B, e agora com população definida.
- `embedding_model` — aponta para o Volume do workspace antigo e falha em **99,3%** dos laudos em
  produção (card `305810`).
- `uncertainty_band: [0.35, 0.65]` — piso abaixo do teto analítico 0,597, causa dos **36** casos
  correntes do P0-29 (ver `0.13.0`/`283648`).
- `fallback_policy` — `keep_current` no `nlp` e `positive_in_band` no `runtime`, que vence.
- limpeza dos blocos mortos e **ajuste das colunas da view** de exportação.
- promoção da config **calibrada** local: a que está em prd ainda não é a standard plus.

## Câncer de estômago — ✅ EM PRODUÇÃO · 🔴 O FILTRO DE ENTRADA PERDE 55% DAS ENDOSCOPIAS

📄 `_processo/medicoes/medicao-endoscopia-colonoscopia-repositorio-2026-09-17.md`. Medido em 17/09 sobre
`gold_corporativo_ia.corporativo.tb_gold_mov_exame`, janela de 12 meses (27/08/2025 a 26/08/2026).

🔴 **O `gold_filter` deixa de fora mais laudo legível do que traz: 124/dia contra 106/dia.**

| grupo | exames | /dia | legíveis | /dia |
|---|---|---|---|---|
| é EDA e **passa** o filtro | 79.968 | 219 | 38.545 | **106** |
| é EDA e o filtro **NÃO pega** | **96.729** | 265 | **45.390** | **124** |
| o filtro pega e **não é** EDA | 21 | 0 | 20 | 0 |

⚠️ **O filtro não lê o laudo** — aplica `rlike` sobre **`proced_descricao`**
(`GoldFilterBuilder.keyword_column`, default que o runner não sobrescreve).
✅ **E é preciso**: só 21 exames em um ano entram sem ser EDA. O problema é inteiramente de recall.
**94% da perda está em três descrições** que usam nomenclatura TUSS e não escrevem "digestiva alta":
`endoscopia com biopsia e/ou citologia` (61.902), `endoscopia` (19.314), `endoscopia com biopsia e
teste urease` (12.009).
🔴 **E dos 219/dia que entram, só 106 têm texto legível** — os outros 113 são ponteiro ou vazio, e o
motor roda sobre nada.
ℹ️ Mesma classe do `gold_filter` do TI-RADS que não seleciona punção, com ordem de grandeza outra.
🟡 **Ampliar exige medir o custo em volume antes** — a régua de filtro de entrada pede os dois
sentidos. **Sem card**: filtro de entrada é nossa alçada (POP-IA-08).

### O repositório clínico, para referência

12 meses: **colonoscopia 126.351** (346/dia, 48,6% legíveis) · **endoscopia alta 176.697** (484/dia,
47,5% legíveis). **Menos da metade tem laudo legível** — o resto é apontamento para outro sistema
(25–28%) ou ausência de laudo (24–26%).

## Câncer de estômago — ✅ EM PRODUÇÃO desde 2026-09-04

Config **`0.6.9-cancer_estomago`** (gate da úlcera isolada), engine `0.10.1`. PRs 7166 e 7187
mergeados pelo João.

**Primeiro dia em prd (04/09):** 151 laudos · 6 relevantes (3,97%) · **zero entregue sem achado**
(eram 36) · zero erro de LLM. O gate rebaixou **5 de 11** laudos com achado léxico.

🔴 **MAS os 6 relevantes eram FALSO POSITIVO** — todos pelo defeito do espaço colado, corrigido na
`0.11.2`. Com a correção seriam **11 relevantes em vez de 17** na coorte de 333 laudos.

🔴 **O `gold_filter` deixa de fora 55% das EDA do repositório, e a exclusão NÃO é intencional.**
A SPEC §2 declara o universo como *"entram: endoscopia digestiva alta (EDA)"* e apresenta o
filtro como a implementação disso — o único risco que ela registra é o oposto (sem filtro, a
entrada foi de 4.818.237 laudos). É lacuna de implementação, não recorte de escopo.
**Medido em 12 meses (27/08/2025–26/08/2026):** `endoscopia com biopsia` traz **+76.533 exames**
(209,7/dia) e **+36.991 legíveis** (101,3/dia); `endoscopia com cromoscopia` traz +246/+132.
**A entrada legível vai de 105,3 para 207,0 laudos/dia.** 🟢 **Custo: 213 exames não-EDA em um
ano** (0,6/dia) — precisão da ampliação **99,72%**. Seguem fora 43.641 exames (11.870 legíveis).
⚠️ **Ampliar invalida a comparação com a homologação** (recall 0,600 / precisão 1,000 é contra o
corpus estreito) e a taxa de 3,97% de produção. **Próximo passo: um dia em dev com o filtro novo**,
medindo volume, chamadas ao juiz, taxa e tempo de run. **Sem card** — alçada da especialidade.
📄 `cancer_estomago/medicao-ganho-gold-filter-2026-09-17.md` e
`_processo/medicoes/medicao-endoscopia-colonoscopia-repositorio-2026-09-17.md`.

ℹ️ **Zero chamadas ao juiz**, por duas causas distintas: 145 laudos abaixo do piso da banda
`[0,60; 0,97]`, e 1 dentro da banda que saiu `skipped_deterministic` porque o `quantitative_gate`
já tinha decidido. Não é defeito — é o desenho da `0.6.9`. ⚠️ Acompanhar: se o juiz nunca for
chamado, produção roda comportamento diferente do homologado (lá foram 345 chamadas em 10.783).

⚠️ A taxa de 3,97% está acima dos 0,70% da janela de homologação. Parte é o defeito do espaço;
o resto pode ser diferença de corpus. Reavaliar depois da `0.11.2`.

## Câncer de estômago — histórico do PR 7102

Branch `cancer_estomago/feature/migracao-plataforma`, commit `e371036`, pushado.
Exige `nlp_engine >= 0.9.4`. SPEC: `cancer_estomago/spec-negocio-cancer-estomago-v1.md`.

**Medido em 10.783 laudos / 61 dias (01/05–30/06):** recall **0,600** e precisão **1,000** no lote
de 37 do negócio · **75 relevantes = 1,2/dia** · **zero** entregue sem achado (eram 36) · juiz
chamado 345 vezes (eram 3.199) · run de 45 min (eram 169).

- ✅ **Cascata regra → expansão → juiz** implementada pela banda `[0.60, 0.97]`. O teto de score de
  laudo sem achado é **analítico** (0,597; medido 0,5876 idêntico em dois runs), então o corte é
  garantia, não estimativa. ⚠️ Revalidar se mudarem pesos, política de score ou régua.
- ⚠️ **O recall de 0,600 é o TETO contra esse gabarito, não limitação da régua.** A anotação do
  Targa é **anterior** à decisão sobre úlceras: cinco dos 15 relevantes dele ficam fora por decisão
  posterior, e um era promoção do juiz sem evidência.
- ✅ **Carol sem pendências** (08/09).
- ✅ **Targa: o retorno virá do que está rodando em PRD**, não de lote para homologar (08/09).
  Seguem em aberto, para quando houver massa: MALT em seguimento conta como progressão? achado
  maligno fora do estômago entra?
- ✅ **Schema `cancer_estomago` provisionado** — verificado em 08/09 em `diamond_fabrica_ia_hml`
  **e** em `diamond_fabrica_ia` (prd).
- Arquivos gerados em `Desktop/Rede D'Or/` (fora do git, têm texto de laudo).


### PR 7102 — estado em 21/08

Branch `cancer_estomago/config-0.6.1-hml`, **config `0.6.2`**, commits `598fb04` → `fe4cd55` →
`08f9c7b`. **Um arquivo, adição pura.**

- 🔴 **Diego votou `Rejected`** e só ele altera. A revisão dele referenciava a `0.6.1`; a `0.6.2`
  e a `08f9c7b` fecharam **todos** os itens de código.
- Único aberto: **compliance/DPO**, que vale para as três especialidades.
- ⚠️ Nome da branch diz `0.6.1` e o conteúdo é `0.6.2` — não renomear, quebra o PR.
- Card do PR: `283567` (**sem dono atribuído**).

## Câncer de rim — Leandro

✅ **Ativou o juiz LLM e subiu para >91%** (2026-08-18). Já tinha resolvido a segmentação com
`full_doc`; os 3 avisos dele viraram 0.8.3/0.8.4, e o da versão do motor se resolve com a migração
(branch `cancer_rim/feature/migracao-config-motor`).

- ❓ **Confirmar qual métrica subiu.** O juiz estava desligado porque **derrubava 3 a 8 pacientes
  confirmados** — precisão já era ~92% com ele. Se os >91% forem precisão, a pergunta que decide é
  se o **recall** se manteve em 1,000. Em rastreio, precisão comprada com paciente perdido é
  regressão, não ganho.
- O diagnóstico escrito para ele (`docs/motor-nlp/cancer_rim/diagnostico-config-cancer-rim.md`)
  ainda não foi enviado; se ele alinhou o prompt à v0.5 por conta própria, parte dele já venceu.

## Reumatologia — ✅ VALIDADA EM DEV, PR 7231 AGUARDA REVISÃO

Card `299111` em *Pronto para QA*, comentado com a evidência. Branch
`reumatologia/feature/migracao-plataforma`, três commits, **6 arquivos, 2.386 linhas, adição
pura**. Revisores João e Diego.

✅ **Run completo em dev e envio ponta a ponta**, 08/09: 3.865 laudos, 24 relevantes em
**9 pacientes**, e **5 arquivos entregues** (SP, BA, DF, RJ, PE), conferidos no Excel.
⚠️ **A validação local não substituía isto** — ela prova a régua e não toca runner,
`gold_filter`, `column_map`, view nem envio. Seis bloqueios só apareceram rodando.

**Paridade em CINCO medições independentes, 99,158% a 99,508%, ZERO ganhos em todas.**
A do ambiente: **99,423%** em 3.810 pares. As divergências são falso positivo do legado.

✅ **As 57 são falso positivo do legado**, enumeradas pela evidência que ele mesmo gravou. Três
vias: frase negada ou de normalidade (*"sem erosão óssea"*, *"fáscia preservada"*) · **cabeçalho
metodológico** (*"Lesões elementares AVALIADAS: ... erosão óssea..."* — o que foi procurado, não
achado; 12 dos 35 casos de 25/08) · achado de outra doença (fratura de escafoide como
`sacroileite`). **A migração não perde recall — remove falso positivo**, e a causa é a negação.

- ⚠️ **A fonte é a branch `hml` do legado, commit `7293729`.** Nem a `main` (2024) nem a cópia em
  `fabrica-ia-plataforma` (05/2026) servem: são anteriores ao PR 6893, que removeu a região
  craniana do filtro. A primeira tentativa usou a cópia local errada.
- ⚠️ **`skip_organ_gate: True` REPRODUZ o legado, não relaxa.** Com `force_full_doc` ele chama
  `strict_organ_filters=False` e não aplica A/B/C. Sem a chave, 3 dos 5 achados somem.
- ℹ️ O insumo do exchange **estava no legado**: 43 colunas do `dic_col_names`, 4 listas suspensas,
  e 605 unidades com 4 grupos de destinatários no `unidades.json` do datalake do workspace antigo.
- ✅ **Filtro de entrada calibrado nos dois sentidos, com custo medido:** 82 → **92 de 92
  relevantes** por +2,6% de volume, e exames vasculares fora do escopo excluídos a custo
  zero. ℹ️ `doppler` fica no escopo: excluí-lo custaria **32 dos 92** — é padrão em
  ultrassom reumatológico.
- 🔴 **O legado não tem config de `prd`** — só `dev` e `hml`. O `prd` usa a lista de `hml`.
  Confirmar antes de promover.
- 🟡 **Achado para a plataforma, sem card ainda:** `dt_execucao_modelo` é gravado em **UTC**
  e a view de exportação filtra pela data **local (BRT)**. Quem rodar entre 21:00 e 00:00
  fica com a view vazia, **sem erro**. O agendamento das 04:00 está fora da janela, então
  produção não sofre — a única defesa hoje é a regra do checklist.
- 🔴 **Schema `reumatologia` só existe em `dev`.** É do time da Fábrica criar; sinalizado ao Ops
  junto com o PR, por procedimento próprio — **não** vai na descrição do PR.

## 🔴 Reumatologia — O FILTRO DE ENTRADA PERDE 18,6%, E 14% DISSO É DEFEITO (25/09)

Medido replicando **os dois filtros sobre a mesma gold**, `id_exame` distinto, 30 dias
(25/08–23/09). Não é comparação de tabela de saída — é o recorte que cada filtro faz na fonte.

| recorte | exames | /dia útil |
|---|---|---|
| legado **com** região craniana | 151.986 | ~6.400 |
| legado **sem** craniana (régua pós-PR 6893) | **95.294** | ~4.100 |
| **plataforma hoje** | **77.554** | ~3.400 |
| plataforma com o escape corrigido | 80.104 | ~3.500 |

🟢 **Os −49% contra o legado completo são 37% DECISÃO:** a região craniana (`crânio`, `cabeça`,
`face`, `intracranian`, `mastoid`) vale **56.692 exames** e foi removida de propósito no PR 6893.

🔴 **Contra a régua vigente: −18,6%**, ou **17.740 exames em 30 dias (~591/dia)**.

### 🔴 O defeito, e ele explica a diferença para o que foi homologado

O `\b` do regex do `gold_filter` vira **BACKSPACE** no literal SQL, não *word boundary* — o termo
`\bp[eé]s?\b` **não casa nada**. Medido: **2.550 exames em 30 dias (+3,29%)**.

| | razão contra o legado sem crânio |
|---|---|
| plataforma **hoje** | **81,4%** |
| plataforma com escape corrigido | **84,06%** |
| **documentado na migração** | **84,3%** |

**O número homologado só se reproduz com o escape funcionando.** Produção está **2,7 pontos abaixo
do que foi medido e aprovado**, e a correção é **uma barra a mais** no literal.
ℹ️ Mesma classe da memória `regex-em-config-armadilhas-de-escape`, agora com custo medido.

### O resto do −18,6%, e é decisão registrada

A plataforma filtra sobre **uma** coluna (`proced_descricao`); o legado sobre **três**
(`proced_descricao_ajustado`, `dsc_codigo`, `cod_procedimento`) unidas por `OR`. E o
`UPPER(trim(tp_procedimento)) IN ('IMG','IMA')` **não é reproduzido** — divergência 5 do cabeçalho
da config. Decisões registradas, **nunca medidas** até agora.
⚠️ Também sumiram na tradução as exclusões `paaf`, `punção`, `biop`, `bióp`, que o legado tinha.

### ✅ Rastreabilidade RESOLVIDA — repositório canônico clonado (25/09)

`IAAzureDatabricksReumatologia` clonado do Azure DevOps para `Projects/` (não rastreado).
ℹ️ **Os dois commits citados na documentação não divergiam:** `7293729` é a **ponta da `hml`** e
`b200286` é o **pai** — o merge do PR 6893.
🟢 **O `CONFIG` da `hml@7293729` é byte a byte igual à cópia de maio/2026.** A única diferença em
todo o notebook é o caminho do modelo de embeddings (PR 7210, fix de SSL). O Data Card lista os
mesmos termos. **A régua está confirmada por três fontes independentes.**

### 🔴 CAUSA RAIZ DA QUEDA DE ENTREGA: vocabulário idêntico, CAMADA removida

85 → 47 pacientes em 7 dias (−44,7%). Pareado por `id_exame`, 25–31/08: **17.881 exames,
99,54% de concordância**, **82 `1 -> 0`** e **1 `0 -> 1`**.
Dos 82: `artrite reumatoide` 73 · `sacroileite` 7 · `espondilite`+`psoriasica` 2.

🔴 **A explicação por negação CAIU.** Os 82 têm **zero span positivo E zero span negado** — a régua
nova não leu e recusou, **não encontrou nada**. Controle: o campo existe em 18.502 de 18.502, com
75 laudos com span positivo e 20 com negado na mesma janela.
🔴 **O único `0 -> 1` é falso positivo NOSSO** — `reumatoide` casou na linha de **indicação
clínica**, num laudo que nega erosão, sinovite e tenossinovite.

**O que o legado de fato busca** (os termos são SEMENTES, não literais):

```
candidatos = noun_chunks + n-gramas 1..3 do PRÓPRIO laudo
portão lexical: >=2 tokens e >=1 token OU raiz em comum com as sementes
score = 0.75 * embedding(MiniLM multilingual) + 0.25 * fuzzy(SequenceMatcher)
entra se score >= 0.65, até 24 termos novos por semente
```

Portão lexical reproduzido sobre os 82 laudos reais:

| termo do laudo | laudos | vira | pela semente |
|---|---|---|---|
| `erosao ossea` | **40** | `artrite_reumatoide` e `sacroileite` | `erosoes osseas marginais` |
| `erosoes osseas` | **28** | idem | idem |
| `edema osseo` | 24 | `sacroileite` | `edema osseo sacroiliaco` |
| `estruturas osseas` | **19** | `artrite_reumatoide` | `erosoes osseas marginais` |
| `osteofitos marginais` | **13** | `espondilite` e `artrite_psoriasica` | `sindesmofito marginal` |
| `lesoes elementares avaliadas` | **12** | `nodulo_reumatoide` | `lesao nodular reumatoide` |

🔴 **`osteofitos marginais` (degenerativo) vira espondilite (inflamatório)** pelo token `marginal`;
**`lesoes elementares avaliadas`** é o cabeçalho metodológico já apontado na homologação, e entra
pelo token `lesao`; **`estruturas osseas`** é boilerplate.
⚠️ O portão lexical é **necessário, não suficiente** — quem decide é o score ≥ 0,65, ainda não
reproduzido (sem `sentence_transformers` no ambiente).

**A plataforma roda `use_embeddings: False` e `llm_router.enabled: False`** — casamento literal.
Dispara em **75 de 18.502 laudos (0,4%)**. **A migração não estreitou o vocabulário: removeu o
motor que o alargava.** E a linha está **em produção com perfil parcial**, contra a régua de que
nenhuma lista vai ao negócio a partir de `rule_only`.

🔴 **Os 82 são 40 LAUDOS DISTINTOS em 36 pacientes** — há duplicação de `id_exame` sobre o mesmo
texto, mesma data e mesmo paciente (três blocos de **12x**). Toda triagem tem de ser sobre texto
distinto; sobre os 82 crus, o grupo "sem achado" aparece inflado 25 contra 3.

**Triagem sobre os 40 distintos** (negação à esquerda do termo):

| grupo | laudos | pacientes |
|---|---|---|
| erosão / sinovite / sacroileíte **afirmativa** | **25** | **24** |
| só derrame / edema ósseo / sindesmófito | 9 | 8 |
| **só tenossinovite** — termo AUSENTE da régua nova | 3 | 2 |
| nenhum achado afirmativo | **3** | 3 |

🔴 **Só 3 de 40 não têm nada afirmativo.** A hipótese "o legado era quase todo falso positivo"
**não se sustenta**: a precisão declarada dele é 76%, o que preveria ~10 FP em 40, e o que se vê
é 3 vazios mais 9 inespecíficos. **O peso vai para perda de recall real.**

🔴 **Pendente e decisivo: adjudicação clínica dos 82.** Artefatos com laudo completo e coluna de
veredito em `Desktop/Rede D'Or/reumato-laudos-25a31-08-2026.csv` (fora do git).

### ⚠️ O Data Card está desatualizado em três pontos

`Downloads/Data+Card+-+Reumatologia.docx`. ✅ Seeds, achados, tabelas, periodicidade e endpoint
conferem com o código.
🔴 **Escopo:** descreve `crânio`, que o PR 6893 removeu em 23/07/2026.
🔴 **Métricas:** acurácia 93%, recall 100%, precisão 76%, F1 0,86 — todas de **26/01/2026**, seis
meses antes do PR 6893.
🔴 **Não menciona a expansão semântica** — apresenta seeds e palavras-chave como se fossem o que é
procurado. **É a premissa com que a migração foi feita e homologada.**

### ✅ `IN ('IMG','IMA')` é INERTE · 🔴 a perda está na COLUNA ÚNICA, e é vocabulário

Dos **77.554** exames que a plataforma traz em 30 dias, **77.554 (100%) já são `IMG`/`IMA`**.
Não reproduzir a cláusula custa **zero nos dois sentidos** — **divergência 5 fechada**.

🔴 **A divergência 4 perde 470 exames em 30 dias (~15,7/dia)**, todos `IMG`/`IMA` — mas a causa
não é o número de colunas, **é vocabulário**: as palavras estão em `proced_descricao`, só não
constam da lista de região.

| grupo | exames | veredito |
|---|---|---|
| corpo inteiro / esqueleto ósseo | **165** | 🔴 falta `corpo inteiro` (só há `corpo total`) |
| pelve | **110** | 🔴 falta `pelve` (só há `bacia`); a sacroilíaca está ali |
| outros (inclui `pé`) | 108 | a triar |
| craniana | 59 | ❌ removida de propósito no PR 6893 |
| procedimento guiado | 28 | ❌ não é laudo diagnóstico |

✅ **Acrescentar `corpo inteiro` e `pelve` recupera ~275 exames (~9/dia) sem tocar no runner.**

### ✅ CORREÇÃO ESCRITA E PUSHADA — config `0.2.0-reumatologia` · 🟡 PR NÃO ABERTO

Branch `reumatologia/fix/filtro-e-regua`, commits `1c869d3` e `9321cce`, **confirmada pela REF**.
**O PR não foi aberto** — fica para 13/10.

| mudança | medido em 30 dias |
|---|---|
| escape do `\b` corrigido | **+2.550 (+3,29%)** |
| `corpo inteiro` no vocabulário de região | +431 |
| `car[oó]t[ií]d` / `art[eé]ri` / `venos` excluídos | −372 |
| `paaf`, `pun[cç][aã]o`, `bi[oó]ps` devolvidos | −78 |
| **líquido** | **+2.531** — 77.554 → **80.085** |

Régua: **`artropatia inflamatoria`** acrescentada a `artrite_reumatoide` — recupera 4 dos 37.

🔴 **`pelve` NÃO entrou.** Mede ~110 exames, mas a primeira estimativa isolada deu **+33.228** e só
caiu ao medir junto com o resto do filtro. Fica como recomendação medida, **não** como mudança
aplicada — correção e ampliação não entram no mesmo PR.

⚠️ **A cadeia do escape tem quatro elos:** fonte `.py` com quatro barras → valor Python com duas →
literal SQL → regex `\b`. Errar qualquer um é **silencioso**.

### ✅ O RELATÓRIO 1:1 PARA O HEAD — legado × motor

📄 `reumatologia/resumo-migracao-legado-x-motor-2026-09-25.docx`. Base: **16.986 laudos únicos**
que os dois lados processaram na mesma janela.

| | legado | motor |
|---|---|---|
| laudos entregues | **95** | **80** |
| pacientes | 84 | 66 |
| por dia | 13,6 | 11,4 |

**Concordam em 58. Discordam em 59** — 37 só o legado, 22 só o motor.
Dos **37**: **24 falso positivo**, **11 achado real**, 2 sem veredito. Dos **22**: 19 pelo termo
novo, 1 falso positivo nosso, 2 sem explicação.

⚠️ **Três números meus caíram por mistura de unidade ou de base** — exames somados contra laudos
únicos no funil, uma estimativa com regra ajustada em outra população, e um 52 que era 53.
**Refazer a cadeia numa unidade só** foi o que fechou.

✅ **O legado está DESLIGADO na fonte, não só no espelho** — verificado em
`hive_metastore.ia.tb_diamond_mod_reumatologia_saida` no lake 1, com o perfil reautenticado:
último exame 31/08, última execução 02/09, 792.660 linhas, idêntico ao espelho.

## Reumatologia — ✅ EM PRODUÇÃO · 🟡 PR DO EXCHANGE AGUARDA OPS

✅ **A linha entrou em produção e o legado foi desligado.** Schema `reumatologia` provisionado em
`diamond_fabrica_ia`, com as quatro tabelas e a `vw_mod_diamond_reumatologia_export_v0`.

🔴 **O PR 7287 NÃO CHEGOU A PRODUÇÃO — verificado em 17/09.** Produção sai da `main`
(`groupPrd: fabrica-ia-lib-main`), e a `main` **não tem o 7287**: está **59 commits atrás** da `hml`,
com **15 commits próprios** que nunca voltaram. O arquivo de prd lá segue no formato antigo, sem
`findings`.
🔴 **Então o defeito continua ativo:** o job roda todo dia em prd (1.591 laudos em 17/09, 694 com
`findings` preenchido) e **entrega a coluna de achado vazia**. Avisado ao Diego, que vai promover.

✅ **TI-RADS TAMBÉM NO PADRÃO — PR 7287 mergeado na `hml` em 15/09 às 21:28** (`cf281e7`), quatro commits.
Os três ambientes do TI-RADS passaram a 22 colunas, 3 ocultas, `colunas_manuais` vazio e sem
`descriptografia`. **Reumatologia e TI-RADS são os dois exemplos do formato final na árvore.**
🔴 **E corrigiu um defeito ativo:** a coluna do achado em produção apontava para
`classificacao_rads`, que a view materializa como `CAST(NULL AS STRING)` — era entregue **vazia em
100% das linhas**. Medido: 0 de 89 não-nulos na view, contra 2.370 de 2.370 relevantes com
`findings` preenchido. Validado em dev em 15/09: 55 registros, achado preenchido em 55 de 55.

🟡 **PR aberto** — branch `reumatologia/feature/exchange-colunas-navegacao` (`769d891`), três
commits, da `hml`. **O arquivo de navegação passa a entregar só as 19 colunas da view**, no formato
do `cancer_rim`, definido pelo negócio como referência.
ℹ️ **A referência passa a ser a própria `reumatologia`** — é o `cancer_rim` com as colunas vazias
removidas, e é o formato a clonar nas próximas linhas.

**Entram** `convenio`, `plano`, `medico_solicitante`, `crm_solicitante`, `uf_crm_solicitante`.
**Sai o bloco manual inteiro** — as 27 colunas herdadas do legado e as três listas suspensas que só
elas usavam. Os três arquivos caem de ~640 para ~165 linhas.

🔴 **`medico_solicitante` e `crm_solicitante` chegam CIFRADOS da view** — 165 de 173 registros em
base64. Entraram em `descriptografia`. ⚠️ **`count()` conta cifrado como preenchido**: a primeira
verificação reportou "100% preenchido" e estava certa no número e errada no sentido. Quem denunciou
foi o `descriptografia` do `cancer_rim`, que declara os mesmos dois.

✅ **Validado em dev com envio ponta a ponta** e conferido com o negócio em 11/09.
ℹ️ O layout de coluna não varia por regional — os quatro arquivos que chegaram provam as 19 colunas.

🟡 **Um arquivo (RJ) não gerou e-mail**, e a causa está fora do nosso lado: os cinco POSTs
retornaram `202` no mesmo run, com o mesmo destinatário. `202` é aceite assíncrono, não entrega.
⚠️ **Dois achados que não viraram card, por decisão:** o nome do arquivo particionado não carrega
hora (`{now:%Y_%m_%d}`, contra `%Y%m%d_%H%M%S` no ramo sem partição), então re-run no mesmo dia
colide no mesmo caminho e o que acontece na colisão é decisão do Logic App; e a assinatura SAS desse
Logic App está **hardcoded** em `runs/ntb_ia_onedrive.py`. O primeiro é da ferramenta compartilhada
`tools/data_exchange` e mexer altera o nome do arquivo de **todas** as linhas — não entra de carona
num PR de coluna.

## ⚠️ O ESPELHO DO LEGADO PAROU — e isso NÃO prova que o legado parou

Medido em 23/09 no schema `diamond_fabrica_ia.legado`, do catálogo novo.

🔴 **O que foi medido é o ESPELHO, não a fonte.** O legado de verdade vive no
**`hive_metastore` do workspace antigo — o "lake 1"** —, e **não foi consultado**: o perfil
`adb-2013197995950192` está com **refresh token inválido**, e o `hive_metastore` visível do
workspace novo é o dele, não o do lake 1.

| linha | última no espelho | plataforma |
|---|---|---|
| **ateromatose** | **893 laudos em 23/09** | **670 em 23/09 (hml)** |
| DII | 02/09 | PR 7383 aberto |
| doenças biliares | 02/09 | sem tabela |
| neuroimunologia | 24/08 | sem tabela |
| reumatologia | 02/09 | 3.466 (prd) — migrada, legado desligado |
| cancer_colon | 01/09 | 365 (hml) — em migração |

⚠️ **Duas leituras possíveis, e o dado não separa:**
1. as linhas pararam de processar — **lacuna de cobertura clínica**; ou
2. seguem rodando no lake 1 e **só o espelho deixou de ser alimentado** — dívida de observabilidade.

🔴 **Não afirmar a primeira sem checar a segunda.** A versão anterior desta seção afirmava
*"três linhas pararam e não foram substituídas"*, e isso **não está provado**.

**O que fecha:** reautenticar o perfil do workspace antigo
(`databricks auth login --profile adb-2013197995950192`) e consultar `hive_metastore.ia`.

🟡 **O que se sustenta sem o lake 1:** a **ateromatose aparece nos dois lados no mesmo dia**, com
volumes e taxas diferentes (1,3% × 6,1%). É o cenário do PR 7275, mergeado em 18/09. **Confirmar
com o Lucas** se a duplicidade é deliberada e tem prazo.

## 🔴 QUATRO LINHAS PARADAS EM HML — medido em 25/09

Verificado nos dois catálogos, tabela de saída por linha. As quatro **rodaram em 25/09** e param ali.

| linha | laudos em HML | relevantes | em PRD |
|---|---|---|---|
| **doenca_inflamatoria_intestinal** | 71.784 | 789 | 🔴 não existe |
| **tumor_osseo** | 58.831 | 661 | 🔴 não existe |
| **ateromatose_coronariana** | 17.719 | 821 | 🔴 não existe |
| **cancer_colon** | 9.839 | 671 | 🔴 não existe |

✅ **As quatro estão COMPLETAS na `hml`** — config, `jobs/definicoes`, `jobs/clusters` e as três
navegações (dev, hml, **prd**). A `main` tem **só as seis de produção**. Falta **um PR `hml → main`**
e o **schema em prd**, que é do time da Fábrica.

⚠️ **A nota anterior de que `cancer_colon` era a única sem `disabled` estava errada.** Conferido nas
dez definições: as **seis que já estão em PRD** declaram `disabled` nas duas tasks de exchange; as
que ainda não estão declaram zero — e isso é a **regra documentada** no próprio
`tumor-osseo-batch.json` (*"linha nova nasce sem `disabled`; ADICIONAR no dia da promoção"*). O
`doenca_inflamatoria_intestinal` já tem na task `api_`, que é a que posta na Navegação real.
🔴 **O desvio real do `cancer_colon` é outro e continua de pé:** `"${nlp_engine_version}"` em vez do
literal, e foi por isso que rodou `0.15.1`, `0.14.0` e `0.12.3` em três dias.

⚠️ **Ao promover, a checagem é a mesma das seis:** acrescentar `disabled` nas tasks de exchange
**no mesmo commit**, senão homologação passa a postar na Navegação real.

ℹ️ **`reumatologia` não tem schema em HML** — está só em prd. Não bloqueia nada hoje, mas tira a
linha de qualquer validação em homologação.

## Migração dos algoritmos legados — 🟡 DUAS NESTA SEMANA

**Nossa fila (14/09):** `doencas_biliares` e `neuroimunologia` **até 18/09**, depois **nódulo
pulmonar**, **endometriose** e **birads**. ℹ️ **Ateromatose saiu da nossa fila — está com o Lucas.**

✅ **SPECs de PRÓSTATA e DOENÇAS BILIARES escritas em 25/09**, para o Leandro dar o start na sprint
seguinte. 📄 `prostata/spec-migracao-prostata-pirads-v0.md` e
`doencas_biliares/spec-migracao-doencas-biliares-v0.md`.

| | próstata (PI-RADS) | doenças biliares |
|---|---|---|
| universo medido (30 d) | **896 RM de próstata**, 30/dia | **97.818**, 3.261/dia |
| régua | categoria ordinal, **zero léxico** | 6 categorias, 41 termos |
| trabalho de lib | **nenhum** — `systems` é 100% config | nenhum |
| já medido | bancada 06/26: **98,99%** de concordância, MCC **0,968** | — |

🔴 **A próstata é a linha mais barata da fila e a maior alavanca por esforço:** o legado dela é da
geração **anterior** ao `CONFIG` — regex puro, sem `organs` e sem `findings` —, e **apaga a legenda
cortando 385 caracteres fixos** depois de um marcador. É exatamente o que a
`aggregation_legend_filter` faz de forma genérica desde a `0.10.1`.
🟢 **Filtro do legado medido: recall de 84,3%** (1.067 de 1.265 exames que citam PI-RADS em 30 dias).
O termo que sustenta isso é `pelve` — rende só 3,7% sozinho, mas é por onde entra o laudo de próstata
descrito como pelve. **Faltam ~198 em 30 dias**, e é a primeira medição a refazer.
🔴 **As biliares são 3.261 exames/dia — segunda maior linha da fábrica**, atrás só da hepatologia.
Cluster, custo de LLM e tempo de run se decidem **antes** de começar.

🟢 **PRs 7321 (João) e 7275 (Lucas) APROVADOS — falta só o merge, nesta ordem: João, depois
Lucas.** Decisão do usuário em 17/09: **não postar comentário nem ampliação por hora**. O
comentário de aprovação do 7275 está redigido em `_processo/revisoes-pr/comentario-pr-7275-aprovacao.md` e
**não foi publicado**. Sem card de `pause_status` por hora — refina depois.

✅ **PR 7275 — TERCEIRA REVISÃO em 17/09: aprovado, com uma condição de ordem.** Ponta `7901021`,
config `0.2.3`. Sete commits novos, `0.2.0` → `0.2.3`, **com medição**. 📄
`_processo/revisoes-pr/auditoria-pr-7275-terceira-revisao.md`.
🔴 **Dois achados dele viram evidência para o `283648`:** o `0.2.0` mostrou o juiz chamado em
**6.111 de 7.500 (81,5%), todos sem achado** — o P0-29 numa segunda linha; e o `0.2.1` mostrou a
**semântica promovendo 33 de 44 sem passar pelo juiz**, via que a banda não alcança.
✅ **Desligar a semântica está certo** — ele refinou duas vezes antes.
🟡 **Condição única: merge depois do PR 7321** (João), porque a `0.2.3` aponta o `embedding_model`
para o Model do UC, que só o `ConfigLoader` daquele PR resolve. Inerte hoje (`use_embeddings: False`).
🔴 **Correções minhas na revisão:** o sync não bloqueia (`mergeStatus: succeeded`, zero conflito);
o `pause_status` literal é padrão das **oito** definições, não desvio dele; e a precisão de 0,625
**não decide nada** — é `5/8` contra `5/9`, um laudo, IC 0,31–0,86.
ℹ️ **Alinhamento do usuário com o Lucas (17/09):** ele estava migrando e ajustando régua ao mesmo
tempo, e não confia na última régua do legado. Orientação: **rodar o e2e em dev sobre a janela
congelada, adjudicar os divergentes e julgar plausibilidade** — não perseguir alvo numérico; e
`pause_status` igual ao da reumatologia.

ℹ️ Histórico das duas primeiras revisões e das orientações: `_processo/revisoes-pr/auditoria-pr-7275-ateromatose.md`
e `_processo/revisoes-pr/orientacao-pr-7275-ateromatose.md`.

Padrão a reaproveitar, produzido pela reumatologia: clonar a branch `hml` do repo legado (nunca a
cópia local), gerar a config programaticamente do `CONFIG`, e medir paridade contra a saída gravada.

✅ **Os dois legados são o MESMO motor** — 45 linhas diferentes em 5.210. Varia `TARGET_ORGAN`, os
nomes de tabela e um punhado de termos. **Uma extração serve as duas**, e também as três seguintes.

- 🔴 **O dicionário de órgãos é compartilhado e carrega vocabulário estrangeiro.** No notebook do
  biliar, `colon` aparece **77 vezes** e `rim` **26** — mesmo padrão do ca-cólon. Remover muda
  resultado: medir, não limpar no olho.
- ⚠️ **A segmentação difere:** `FORCE_FULL_DOC_FOR = {"neuroimunologia"}` contra conjunto **vazio**
  no biliar. Mesma classe do `mode: auto` da hepatologia, que descarta 86%. Não clonar uma na outra.
- 🔴 **Dependência de terceiro com prazo:** os schemas `doencas_biliares` e `neuroimunologia`
  precisam ser provisionados pelo time da Fábrica, em **dev e prd**. Na reumatologia a ausência em
  prd virou bloqueio na hora de promover.

## Neuroimunologia — ✅ PR 7428 APROVADO (25/09), aguarda work item e revisores

PR **`7428`** (Leandro), alvo `hml`. **Reavaliado depois da correção dele e aprovado, sem
bloqueante.** O ponto levantado na primeira passada — misturar **11 divergências lidas
clinicamente** com **6 projetadas pela régua** — foi resolvido por inteiro na descrição revisada.

Pendências, do lado dele: vincular o work item `299110` e atribuir revisores.

🔴 **Condição de PROMOÇÃO a prd, não deste PR:** a linha precisa da camada semântica para levar o
score à banda do juiz — **0,74 sem ela** —, e isso depende do card `305810` — *[Plataforma NLP]
Modelo de embeddings sem caminho válido em produção*. Perfil parcial não entrega lista ao negócio.

ℹ️ É a primeira das três que o Leandro toca em sequência: **neuroimunologia → próstata → doenças
biliares**. As duas seguintes já têm SPEC escrita.

## Board — consolidação de 25/09 para o retorno

**Fechados:** `306034` · `283648` · `298598` · `283644` · `300202` · `283647` · `299238`.

**Abertos em *Comprometido (Sprint Backlog)*, com a tag de board `v0.15.4`:**

| card | tipo | o que carrega |
|---|---|---|
| `358081` — *[NLP Engine] Pinar a 0.15.4 e fechar os defeitos que o pin destrava* | Defect | pin · `runtime` · segmentação da hepatologia |
| `358082` — *[Plataforma NLP] Alinhar contrato de entrada e saida e a SPEC 27 com o time de Ops* | User Story | contrato · SPEC 27 · o aval do `305875` |

🔗 **Vinculados entre si e ao `306066`** — *[NLP Engine] TI-RADS rebaixa o laudo de puncao com TR4
sem medida* —, porque o motivo de bloqueio é o mesmo. O `305875` (*Avalizar a chave `waive`…*)
ficou ligado ao `358082`, que é onde o aval vive.
🔴 **O `305875` segue `Novo` e sem responsável**, e é ele que destrava o `306066`.

⚠️ **Dois aprendizados de board, verificados:** `Task` **não aparece** no kanban de Stories — o
`358082` nasceu Task e virou User Story por `System.WorkItemType`; e a coluna *Comprometido* não se
alcança escrevendo o campo de Kanban, alcança-se pondo o **estado** em `Planejado`.

## DII — 🟡 PR 7383 ABERTO, APROVADO COM TRÊS AJUSTES

✅ **PR `7383` aberto em 22/09** — `[DOENCA_INFLAMATORIA_INTESTINAL] Migrar a regua legada para a
plataforma: config 0.2.2, navegacao e job`. Alvo `hml`, **6 arquivos, 1.090 adições, ZERO
deleções**, `mergeStatus: succeeded`, merge-base na ponta da `hml`. Revisor: Deivid, sem voto.

✅ **Parecer entregue em 23/09: aprovar.** Nenhum bloqueante. Três ajustes: confirmar o
`id_linha_navegacao` com o João · remover as citações a `contexto/dii/…` (quatro arquivos que **não
existem** na árvore) · vincular o card `298553`.

🔴 **TRÊS APONTAMENTOS MEUS CAÍRAM ao conferir o código dele:**
1. **`descriptografia`** — ele removeu e está certo: a view já entrega em claro
   (`nlp_ia_06_view.py:37-41`; `boas-praticas/10` §2.5 diz *"não há bloco de config para isso"*).
   🔴 **Quem carrega config morta é `reumatologia` e `tumor_osseo`** — item nosso.
2. **`ambiguity_band` dentro de `embeddings`** — é onde `config_loader.py:129` lê. Correto.
3. **`runtime`** — eu tinha **pedido que ele removesse**, na orientação escrita. As **7 configs**
   declaram, e a decisão de 15/09 é não mexer antes do alinhamento. **A orientação estava errada.**
   Não peço reversão: ele fez na ordem certa (declarou `enabled: False` antes de remover).

🔴 **Condição de promoção a prd, não deste PR:** o perfil é `rule_only`, e *nenhuma lista vai ao
negócio a partir de perfil parcial*. Em hml só ele recebe; em prd são 5 destinatários.

⚠️ **E a dependência que ele declara mudou:** ele planeja ligar semântica e juiz *"depois que a
`0.15.0` estabilizar"*. **Ela não estabilizou** — nove defeitos, e a série tem três versões no feed.

### Medição anterior, que segue válida

Branch `doenca_inflamatoria_intestinal/feature/migracao-config-motor` (`3e6362e`), config
**`0.2.1-doenca_inflamatoria_intestinal`**, sem conflito com a `hml`. Dono: Leandro.

✅ **O caminho de config pura resolveu — a feature na lib deixou de ser necessária.** O `document_vet`
expressa a régua: `soft_findings: ['recomendacao_colonoscopia']` + 13 `normality_phrases` com
boilerplate de laudo de colonoscopia. O termo `colonoscopia` vira achado próprio no laudo de imagem
e é rebaixado no laudo de colonoscopia, **pelo conteúdo do próprio laudo**.

**Config única**, `full_doc`, janela 6, 7 regex da auditoria, os dois ramos unidos. Os dois configs
de 09/09 e as três navegações de `dii_colonoscopia` foram removidos.

| | legado | atual `0.2.x` |
|---|---|---|
| relevantes encontrados | 127 de 206 | **205 de 206** |
| marcados errados | 4 | 17 |
| **F1** | 0,754 | **0,958** |
| **MCC** | 0,771 | **0,958** |

Medido em **18.480 laudos** de 3 dias, lib pinada em `0.12.3`, com 101 divergências auditadas.
✅ **E2E em dev:** 31.609 laudos; plataforma × bancada dá **3 decisões diferentes em 17.494 (0,02%)**.
✅ **`gold_filter` medido nos dois sentidos com custo:** +218 exames, todos `IMG`; `entero` solto
**descartado** por trazer +3.600 culturas.
ℹ️ Dos 5 relevantes do legado não marcados, **4 são fuzzy do próprio legado** — o denominador foi
depurado antes de calcular a perda.

🟡 **Em fecho pelo Leandro (17/09).** Pendência única: **o bug de acesso da view** — é o
mesmo grant que aparece no PR 7275 e no TI-RADS (`USE CATALOG security` + `EXECUTE` em
`security.prd.rdsl_decrypt`), tratado como item único nas dívidas transversais.

📄 **Parecer em `dii/dii-parecer-avaliacao-01.md`** e orientação ao dono em
`dii/orientacao-dii-leandro.md`: **ok para abrir o PR, com quatro ajustes**.
🔴 **O principal é LIGAR a camada semântica e o juiz, não remover o bloco** — `rule_only` é estágio
de desenvolvimento e nenhuma lista vai ao negócio a partir de perfil parcial. São **três chaves em
dois arquivos** (`use_embeddings`, `llm_router.enabled`, widget `embedding_enable`), e **a branch
não traz definição de job** — só 4 arquivos, sem `jobs/definicoes/` nem `jobs/clusters/`, então não
há onde declarar o widget.
⚠️ O bloco do juiz está **incompleto** (falta `model`, `uncertainty_band`, `max_input_chars`,
`prompt_system`, `specialty_context`); `ambiguity_band` é **inerte** em `hybrid`; e
`similarity_threshold: 0.80` é frouxo contra os **0.92** do `cancer_rim`, que é a referência.
🔴 **O prompt do juiz sai dos deltas, não de suposição.** Dos 17 FP, **13 são régua** — 8 negação a
distância, 3 anatomia fora do trato, 2 recomendação de RM; sobram os **4 de referência ao passado**
mais o que a camada semântica promover, população que não existe em `rule_only`. Ordem: corrigir a
régua → híbrido sem juiz → isolar as promoções pelo **delta por `id_exame`** (⚠️ `decision_source`
sai `hybrid` em todos os laudos que passam pela camada, não isola) → escrever o prompt com as
condições lidas → ligar o juiz e medir o que remove. Procedimento em
`dii/orientacao-dii-leandro.md` §1.5, com a matriz de **quatro corridas, uma variável cada**.
Os outros três: cabeçalho explicando o `document_vet` · remover o `runtime` **no mesmo commit** da
ligação do juiz (senão o `runtime` vence e o juiz fica desligado em silêncio) · e **uma
verificação**: `negation.direction_default` declarado como `None` seta `_default = None` em vez de
cair no `left` da lib, e pode explicar 8 dos 17 FP.
🔴 **Ligar muda o perfil e invalida as métricas atuais** — remedir na **mesma janela dos 18.480** e
levar ao negócio só as **discordâncias** contra a corrida `rule_only`.

⚠️ **O gabarito de 206 é derivado da própria comparação** entre as duas réguas. Não invalida, mas o
F1 é contra esse gabarito — os 96 laudos que o atual marca e o legado não vão para revisão do
negócio.

---

## Câncer de cólon — 🟡 LEVANTAMENTO PARA MIGRAÇÃO

Roda no **legado**: `fabrica-ia-plataforma/apps/databricks/colon/`, no **workspace antigo**, sobre
**`hive_metastore.ia`** — fora do Unity Catalog. **Sem juiz LLM.** Agenda convocada pelo Natan.

- ✅ A régua legada já tem um `CONFIG` **no mesmo formato que o `nlp_engine` espera** (`negation`,
  `organs.<x>.seeds/regex`, `findings`, `semantic`). Migração é **tradução de config**, não
  reescrita — e a régua real do cólon é pequena: ~53 termos, 44 seeds, 14 regex, mais o bloco DII.
- 🔴 **Não existe baseline.** O notebook de monitoramento não calcula nenhuma métrica de acerto.
- 🔴 **Gabarito desconhecido** — existe `dev_tb_diamond_mod_colon_saida_conferencia`; confirmar se é
  gabarito clínico ou fila operacional.
- 🔴 **Vocabulário estrangeiro embutido:** a régua geral carrega **reumatologia** (97 seeds) e a de
  colonoscopia carrega **hepatologia** (39 achados, LI-RADS). Remover muda resultado — medir.
- 🟡 **Duas réguas divergentes** (geral × colonoscopia): `polipo` (18 termos) só existe na geral.
- Schema `cancer_colon` **já existe em dev**. Convenção confirmada: linhas oncológicas levam o
  prefixo `cancer_`.
- ⚠️ Volumetria do legado **não medida** — o perfil do workspace antigo está expirado.

## Randomização — ✅ MÉTODO VALIDADO · 🟡 DECISÃO DE PERCENTUAL FECHADA EM 23/09

📄 `docs/randomizacao/` — parecer, resumo para refinamento e as ferramentas reexecutáveis.

✅ **O sorteio foi verificado de forma independente**, reimplementado em Python puro sobre
**1.000.000 de CPFs**: entrega 4,9907% no corte de 5% · subir de 5% para 10% **não realoca
ninguém** · determinístico · independente de atributo (maior |z| = 2,13). **Não há razão para mexer.**

✅ **DECISÃO: 10%, estudo ÚNICO e AGREGADO.** Medido em 23/09 sobre a saída de produção:

| | |
|---|---|
| rollout onda 1 | ca_estomago · cancer_rim · reumatologia · tirads · transplante_pulmao |
| pacientes encaminhados/ano | **19.077** (52,3/dia) |
| controle a 10% | **1.908** (~5,2/dia) — detecta **14%** |
| controle a 5% | 954 — detecta 19% |

🔴 **Nenhuma linha isolada conclui sozinha:** a maior, o TI-RADS, detecta **18% mesmo com 10%**. As
outras precisariam de efeitos de 31% a 84%. **Isso precisa estar na ata** — se alguém esperar
resultado por linha, a expectativa está errada desde o desenho.
ℹ️ A recomendação anterior de 5% foi calculada sobre os **91.773 agregados do backtest**; estas
cinco linhas entregam 19.077. **A base mudou, a recomendação mudou com ela.**

⚠️ **Condição operacional:** **12,8% dos CPFs começam com zero**, o *"erro número um em produção"*
da própria biblioteca. Monitorar a taxa de casamento desde o dia 1.
🔴 **Precede a operação: ética, LGPD e formalização.** Braço de controle é paciente **encontrado e
deliberadamente não navegado** — decisão institucional, não técnica.
🟡 Onda 2 — `cancer_colon`, `ateromatose`, `tumor_osseo`, `dii` — **ainda sem tabela de saída em
produção**; quando entrarem, o poder melhora sem custo de desenho.

## Contexto do paciente — card 280008

Estudo e desenho **concluídos** (doc macro, drawio, nota de review, 2 comentários no card).
🔴 **Pendente: SPEC da fase 0** — exclusão/refutação no escopo do laudo. É o critério que fecha o card,
e não depende de nenhuma das 6 decisões em aberto.

## Revisão de config das especialidades — régua de 2026-09-03

🔴 **Config não passa com bloco morto.** Régua fixada, valendo para qualquer PR de especialidade.
Referência: `cancer_rim`. Regra canônica em `.claude/rules/motor-nlp.md`.

O runner lê `specialty_id`, `config_version`, `model_version`, `data`, `nlp` e **`runtime.llm_router`**.
Não lê `catalog`, `monitoring`, `distribution`, nem interpola `{catalog}`/`{run_id}`.

⚠️ **A SPEC 27 §6.1 está ERRADA** ao afirmar que nada lê `runtime` — `ntb_ia_loader.py:105-113`
faz `self.llm_router.update(runtime_llm)`, e o `runtime` **sobrescreve** o `nlp`. Foi o que
derrubou a primeira subida do ca-estômago. O guia `boas-praticas/02` Passo 6, em compensação, já
pede a coerência entre os dois blocos.

### PR `7159` — ca-rim (Leandro) — ✅ PRONTO PARA APROVAR

`7587020`. Config valida no `load()`: juiz **ligado** (`enabled: True` explícito), 27 termos,
negação 51 frases `direction: left`, threshold `0.92`, `findings` 5, `organs ['rim']`.

**Cinco pedidos, todos atendidos:** comentário de calibragem com os valores efetivos ·
`embedding_model` no Volume novo (`gold_fabrica_ia_hml/.../MiniLM-L12-v2`) · removidos `catalog`,
`monitoring` e `distribution` · removido `gold_filter.mode` (não é lido) · cabeçalho corrigido +
**changelog de `0.4.0` a `0.6.0`**, com o registro de que não existe `0.5.2`.

ℹ️ O comentário dele documenta que `ambiguity_band` é **inerte** em `decision_mode: hybrid` —
confirmado em `decision_pipeline.py:555`, único uso. Eu estava impreciso ao dizer "três números e
só um vale".
ℹ️ **Vira a referência** para limpar as outras especialidades.

### PR `7191` — ca-cólon (Lucas) — 🟡 aguardando ajustes

Config valida: 13 findings, 148 termos, negação 64 frases com direção **por achado**. **Zero PHI**
nos notebooks e no RUNBOOK.

🔴 **`nlp.llm_router` não declara `enabled`** — juiz DESLIGADO (correto para a paridade com o
legado) mas a config declara `mode`, `model`, `uncertainty_band` e `prompt_system` completo, e
quem lê conclui o contrário. É o `283644` espelhado. Pedido: `'enabled': False` explícito.
🟡 Pedidos: remover blocos mortos · `catalog` aponta para `diamond_ia_hml` (workspace **antigo**) ·
**cabeçalho** (é o único dos seis configs sem nenhum) · **changelog** no arquivo, incluindo o
`match_rate` de paridade, que se perde com a remoção dos notebooks.
ℹ️ **Sem LLM neste ciclo**, por paridade. O juiz entra no próximo.
ℹ️ Os dois `assert` do backtest dele — wheel instalada contra o pin e `config_version` carregada
contra a esperada — são o padrão que vale copiar.
ℹ️ O CONFIG dele termina com `assert` travando `soft_findings`, a lista de achados e
`document_vet.enabled`. Contraria a SPEC 27 (§7 diz "dicionário literal e nada mais") **para
melhor** — registrado no card `299238`.

## Pin da versão por linha — ✅ APLICADO (verificado em 15/09)

✅ **As seis definições de job declaram `"nlp_engine_version": "0.12.3"` — literal, não a variável —
em `main` e em `hml`.** Verificado em 15/09. Produção rodou a `0.12.3` nas seis linhas.
✅ **RECONFIRMADO em 23/09 na execução real:** `engine_version = 0.12.3` em **15.329 de 15.329
laudos**, nas seis linhas. Nenhuma pegou a `0.13.0`, a `0.14.0` nem a `0.15.x`.
🔴 **MAS o `cancer_colon` declara `${nlp_engine_version}`, não o literal** — e por isso rodou
**`0.15.1` em hml em 23/09**, `0.14.0` em 22/09 e `0.12.3` em 21/09. **A versão mudou três vezes em
três dias.** É o `latest` movendo a linha sozinho, em forma nova. Uma linha no
`cancer-colon-batch.json`, alçada nossa (POP-IA-08). **Sem card.**
**Sai da pauta com o Ops.** A história `301938` — *[NLP Engine] Fixar versão da nlp_engine por linha*
— está com João Marcelo, em *Em Refinamento*, e vira confirmação.
ℹ️ O mecanismo escolhido foi o literal na definição do job, sem tocar em `jobs/ambientes/`, que segue
em `latest`. Era a pergunta A9 da pauta, e a resposta é sim.

### Histórico da divergência

🔴 **Produção roda `latest`, e isso moveu a versão QUATRO vezes em nove dias** — verificado no
`engine_version` das tabelas de saída em `diamond_fabrica_ia`:

| linha | 02/09 | 03–04/09 | 05–09/09 | **10/09** | laudos em 10/09 |
|---|---|---|---|---|---|
| hepatologia | 0.10.0 | 0.10.1 | 0.11.2 | **0.12.3** | 5.172 |
| tirads | 0.10.0 | 0.10.1 | 0.11.2 | **0.12.3** | 1.568 |
| cancer_estomago | 0.10.0 | 0.10.1 | 0.11.2 | **0.12.3** | 201 |
| transplante_pulmao | 0.10.0 | 0.10.1 | 0.11.2 | **0.12.3** | 96 |

✅ **As quatro linhas ativas executaram a `0.12.3` em 10/09, sobre 7.037 laudos.**

🟡 **Divergência com o Ops sobre o número do pin.** A proposta do Ops é `0.9.4`, por ter sido a
versão vigente no alinhamento de semanas atrás; a nossa é `0.12.3`, por ser a que já executa.
**O reenquadramento que decide:** pinar `0.12.3` tem **delta zero** — congela o que roda hoje.
Pinar `0.9.4` **é** a mudança, três minors para trás, e é ela que precisaria de refinamento.

⚠️ **A `0.9.4` CARREGA em todas as configs atuais** — `0.6.9-cancer_estomago` declara
`>= 0.9.4` e `0.8.0-tirads` declara `>= 0.9.2`. **Não há incompatibilidade a alegar**, só regressão
medida. Afirmar quebra seria overclaim.

🔴 **O que a `0.9.4` reativa:** o P0 do espaço colado (`0.11.2`, medido 17 → 11 em 400 laudos, com
6 laudos entregues em 04/09 dizendo o oposto) · a legenda ACR descendente (`0.10.1`, 23 de 156
entregas) · a âncora ausente fora do gate (`0.12.2`, 36 de 1.032) · falha de infra virando decisão
clínica (`0.11.0`) · semântica promovendo negado (`0.11.1`).
⚠️ **E o argumento que fecha:** antes da `0.12.1` **a lib não emitia log em lugar nenhum**, e a
monitoria não tem coluna de LLM. A régua sustenta a taxa — 3,17% → 3,21% no TI-RADS enquanto 4.703
chamadas falhavam. **Regressão dessa classe não gera chamado**, então "se quebrar abro ticket" não
cobre o risco.

ℹ️ **Escopo do pin, verificado nos schemas de `diamond_fabrica_ia`:** 4 linhas ativas · 3 com schema
provisionado e sem saída ainda (`cancer_colon`, `cancer_rim`, `tumor_osseo`) · **`reumatologia` não
tem schema em prd** e precisa ser provisionada antes de entrar.
**Regra proposta:** linha nova adota a versão pinada vigente, salvo especificação explícita **com
medição** que a justifique.

✅ **São SEIS linhas em produção desde 14/09** — `cancer_rim` e `reumatologia` entraram.
Em 15/09: hepatologia 12.184 · reumatologia 7.877 · cancer_rim 6.399 · tirads 3.410 ·
cancer_estomago 669 · transplante_pulmao 257.

✅ **Feature `298598` (plano de bumps) atualizada** — parava na `0.11.1`. Passa a registrar as 8
versões entregues, a `0.13.0` com 2 de 6, a ampliação do escopo da `0.14.0` com a contabilidade de
tokens, e os **três desvios do plano** (`0.11.2`, `0.12.2`, `0.12.3` entraram por defeito ativo).

## PR 7234 — NPS na esteira da fábrica (Lucas) — 🟡 REVISADO, COMENTÁRIO NÃO POSTADO

📄 **`_processo/revisoes-pr/revisao-pr-7234-nps.md`** — três passadas mais o cruzamento com a revisão de Ops.
Repositório `IAAzureDatabricksNPS`, branch `nps/feature/esteira-fabrica`.

🔴 **Um bloqueante, de uma linha:** `nps/src/eval/acuracia.py` levanta `NameError` na primeira
chamada. **A raiz é que nenhum teste cobre a árvore `nps/`**, que é justamente a que a esteira
publica — a suíte existente é da outra árvore.

**As duas revisões medem contra réguas diferentes, e ambas se sustentam.** A de Ops mede prontidão
para **produção** (13 itens, ~4-5 sprints, *não pronto*); o PR tem alvo **`hml`**, cria o job
**pausado** e exclui do escopo ligar o job e promover. Aplicar o dimensionamento ao merge confunde
as duas.

✅ **O cruzamento não abriu item novo — 5 complementam, 3 corrigem, 3 concordam.** Toda afirmação
foi conferida na árvore publicada, não aceita pelo texto.

- **Complementam:** o `NameError` · o **determinismo com porta de saída em runtime** (`top_k` sai do
  payload no 400 e o run segue sem ele, guardado por estado global mutável com o job a 4 workers) ·
  a localização do dado de paciente em log · a amostra de **179** trechos, que não distingue 93,3%
  de 89,4% (IC ~±4,5 pontos) · a árvore publicada sem teste.
- **Corrigem:** `workers=4` **não é hardcode**, é default de widget · **duas das quatro colunas de
  PII são `cast(null as string)`**, sempre nulas — mascará-las não produz efeito · são **4** cópias
  do mapa de catálogos, não 3, e a que faltou é `nps/serving/ntb_ia_nps_exporta_consumo.py`.
- **Concordam:** `/mnt/` no caminho publicado — e é **leitura em runtime**, não constante residual ·
  `mergeSchema: 'true'` na escrita principal · duplicação de `enderecos.py`.

⚠️ **Um item foi registrado como achado próprio e não era:** o `mergeSchema` consta do item 9 de
Ops, com a mesma recomendação. Corrigido no documento. O de log concretiza os itens 10 e 12.

🟡 **Comentário redigido e revisado, aguardando decisão de postar.** Só o bloco *complementa*, em
caráter complementar — os itens de concordância não vão.

## Plataforma / MLOps

✅ **CUSTO DA LIB MEDIDO em 18/09** — `scripts/medir_custo_por_camada.py`, 365 laudos × 8 perfis,
mediana de 3 repetições. **Régua pura 20,76 ms/laudo · perfil completo 24,44 ms (+18%) · vazão
≈48 laudos/s.** Marginal: semântica +3,51 · ordinal +0,33 · juiz com rede estubada +2,65 ms.
⚠️ As três marginais estão **perto do ruído** (±0,5 ms/laudo) — não afirmar ordem entre elas.
✅ **A régua é ~85% do custo local, e a lib NÃO é gargalo em lugar nenhum:** os 12.184 laudos
diários da hepatologia são **≈4 minutos de CPU**.
🔴 **O que domina é rede, e rede é função da banda.** `[NAO INFORMADO]` seguem a latência com o
modelo real de embeddings e a do juiz com rede — as duas exigem run no ambiente.


🟡 **O pipeline lê o laudo em RTF cru, e existe um irmão em texto limpo** (medido 09/09).
`exm_laudo_texto` vem de `proced_laudo_exame_original`, derivado de
`proced_lista_exames.laudo_original` — e o guia `boas-praticas/04` §5.2 **manda** esse candidato ser
o primeiro. Para uma fatia dos exames esse campo é o documento RTF inteiro: começa em `{\rtf1\ansi\ansicpg1252`, vem numa **única linha**, e o maior tem **814.685 caracteres** — 482 mil dígitos contra 389
espaços, payload hexadecimal de imagem embutida.
✅ **A Gold TEM o texto extraído:** o mesmo struct traz `laudo_transformado`, com acentuação correta
(`TOMOGRAFIA COMPUTADORIZADA DO PESCOÇO`), em 1.391 a 3.404 caracteres nos cinco maiores.
⚠️ **Mas ele não serve sozinho:** está vazio em **158 de 4.321** laudos — provavelmente a razão de o
guia preferir o `original`. O candidato a avaliar é o `coalesce`, com custo medido antes.

| linha | laudos | em RTF | maior |
|---|---|---|---|
| TI-RADS | 4.321 | **116** (2,7%) | 815 KB |
| hepatologia | 5.374 | **231** (4,3%) | 1.039 KB |
| **cancer_estomago** | 753 | **105** (13,9%) | 33 KB |
| transplante_pulmao | 286 | **25** (8,7%) | 512 KB |

No TI-RADS esses 116 ocupam **63,4 dos 68,6 MB** do dia — 92% do volume de texto sai de 2,7% dos
registros. ℹ️ **Não há perda de decisão comprovada:** o tratamento limpa a marcação e sobram ~1.419
caracteres legíveis (77 dos 116 mencionam tireoide). Controlando por tipo de exame, tireoide em RTF
entrega 2 de 46 (4,35%) contra 201 de 2.776 (7,24%) em texto puro — com n=46, compatível com a taxa
normal. O custo é de processamento e robustez: qualquer leitura em lote da coluna estoura o teto de
25 MB por resposta, e foi o que interrompeu duas medições. 🟡 **Sem card ainda**; é distinto do
`300201` (duplicação `2n+1`).


✅ **O LLM VOLTOU A FUNCIONAR EM PRODUÇÃO — verificado em 10/09.** No run noturno: **270 chamadas
nas quatro linhas, ZERO `llm_error`**. Fecha o item que estava aberto desde 27/08.

ℹ️ Histórico: em 27/08 eram **8.058 tentativas e zero sucessos**, em toda a história da tabela,
com `403 — Invalid access to Org: 7405607882166874`. Produção roda em `adb-7405605001346204` e o
`base_url` estava fixo no outro workspace — o token do contexto do notebook é sempre do workspace
onde o job roda. Corrigido no PR 7135 (URL por ambiente), e a nota dizia "ainda SEM TESTE" até
agora.

🔴 **A contabilidade de tokens cobre só UMA das duas origens de chamada.** `llm_prompt_tokens`,
`llm_completion_tokens`, `llm_input_chars` e `llm_api_key_origin` são escritos apenas por
`llm_router_step` — o caminho do **juiz**. A **extração quantitativa de medida** chama o LLM e não
registra nenhum deles; o bloco `quantitative.<criterio>` traz `kind`, `met`, `on_met`, `value`,
`unit`, `threshold`, `evidence` e `llm_called`, e nada de token.

Medido em 10/09:

| linha | juiz | chamadas | tokens por chamada | no dia |
|---|---|---|---|---|
| **hepatologia** | ativo | 49 | **1.004,1** (990,1 + 14,0) | **49.202** |
| tirads | desligado | 143 | — | — |
| transplante_pulmao | — | 71 | — | — |
| cancer_estomago | — | 7 | — | — |

**221 das 270 chamadas do dia não têm contabilidade nenhuma.** ⚠️ Não é estimável por regra de
três: o input do juiz da hepatologia mede 2.000 caracteres, e os laudos de TI-RADS vão de ~1.500 a
815 KB.
✅ **INCLUÍDO NO ESCOPO DA `0.14.0`** — decisão do usuário, para puxar **um alinhamento só** com o
Ops em vez de abrir uma terceira rodada de mudança de contrato. É inclusão de escopo, não algo que
já estivesse no card `283648`.

🔴 **Noturno falhando por lote vazio** (27/08): `nlp_config`/`input`/`persisters` com sucesso e
`process` com `ValueError: Nenhum laudo recebido`, nas 3 linhas agendadas, nas duas tentativas.
Casa com a dedup por `id_exame` sem `config_version`. Comunicado ao João.

🔴 **A tabela de monitoramento não tem NENHUMA coluna de LLM** — só total, relevantes, taxa e
confiança. O `alert_threshold_relevance_drop` não pega falha de LLM: no TI-RADS a taxa ficou
3,17% → 3,21% enquanto 4.703 chamadas falhavam, porque a régua sustenta o número.

- ⚠️ **Dedup da entrada usa `id_exame` puro**, sem `config_version` — janela processada uma vez fica
  bloqueada. Falha em silêncio (lote vazio com sucesso). ✅ Resolvido na prática pelo widget
  **`reprocess_enable`** (só dev) — o contorno de `model_version` de bancada está obsoleto.
- `persist_input` é **código morto** — declarado, lido, nunca consumido.
- ✅ Schemas provisionados: `cancer_estomago`, `cancer_colon`, `cancer_rim`, `tumor_osseo` em **hml**
  (21/08). Produção segue com hepatologia, tirads e transplante_pulmao.

### Runs de bancada em dev estão caros — card `298596` (02/09)

🔴 **`limit_rows` não isola coorte.** O teto é aplicado **depois** da união da fila, cuja ordem é
inéditos → pendentes → **reprocessados por último**. `df_queue.limit(N)` pega as N primeiras, então
a coorte a remedir fica sempre fora do teto. O run fecha **com sucesso sem tocar a coorte**.

- Medido em 02/09, hepatologia: fila de **110.777** para medir 6.398; depois de limpar pendentes,
  fila de **62.266** com 55.868 inéditos. Os 1.000 primeiros gravados tiveram **zero id em comum**
  com a coorte.
- ✅ **Contorno:** janela de **um dia integralmente processado** zera os inéditos. Com 18/08,
  `gold=4508 | ineditos=0 | fila=4508` — **14× menos LLM**. ⚠️ Depende de coincidência: nos outros
  dias da mesma coorte sobrariam 1.203 e 2.765 inéditos.
- ✅ **Backlog de 104.379 pendentes em dev na hepatologia LIMPO** (02/09). Era resíduo de run
  interrompido; as outras três especialidades tinham **zero**.
- `include_pending` tem default `True` e **não tem widget** — não dá para desligar pela UI.

## Alinhamento com a plataforma — 2026-08-21

Duas agendas (42min + 2h24) depois da queda do TI-RADS em produção. **Mapa completo em**
[`_fundacao/design/mapa-gaps-lib-plataforma-2026-08-21.md`](_fundacao/design/mapa-gaps-lib-plataforma-2026-08-21.md)
— 15 gaps com dono, prioridade e solução.

🔴 **A validação em dev de versão que só existe na `hml` é IMPOSSÍVEL hoje** (medido 09/09, ao
tentar rodar a `0.12.2`). O cluster de dev resolve o `pip` contra o feed **`fabrica-ai`**, que é o
de **produção**, alimentado pela `main`:

| feed | alimentado por | versões de `nlp-engine` |
|---|---|---|
| `fabrica-ai-hml` | `hml` | 0.9.4 · 0.10.0 · 0.10.1 · 0.11.0 · 0.11.1 · 0.11.2 · 0.12.0 · 0.12.1 · **0.12.2** |
| **`fabrica-ai`** | `main` | 0.9.4 · 0.10.0 · 0.10.1 · **0.11.2** |

O erro é `Could not find a version that satisfies nlp-engine==0.12.2 (from versions: 0.9.4, 0.10.0,
0.10.1, 0.11.2)` — a lista é exatamente o conteúdo do feed de produção.
⚠️ **O Volume deixou de ser rota — mas NÃO por falta de wheel.** Verificado em 10/09: o volume
`gold_fabrica_ia_hml/.../nlp_engine_lib/` **contém `0.12.1`, `0.12.2` e `0.12.3`**, e a esteira da
lib segue publicando lá (`UploadVolume`). O que parou foi o **consumo**: o widget
`nlp_engine_volume` foi **removido** no PR 7233 e não há índice extra declarado.
ℹ️ A afirmação anterior — "o Volume parou na 0.9.4" — **estava errada**. Há lacuna entre `0.9.4` e
`0.12.1`, mas as três últimas versões estão lá. **A correção é de consumo, não de publicação.** Isso também responde o item
que estava aberto sobre o caminho `livre` da esteira: o Volume deixou de ser caminho de instalação.
🔴 **O fluxo acordado em 21/08 — "PR para `hml` publica no volume de dev para validação nossa" —
ficou sem implementação quando a instalação passou de wheel para `pip` em 03/09.** Ninguém notou
porque desde então não se tentou validar uma versão que existisse só na `hml`.
✅ **A correção é de MLOps e é pequena:** declarar `fabrica-ai-hml` como índice extra do `pip` no
cluster/policy de **dev** — e só de dev. Sem isso, ou se promove sem validar, ou não se valida.

🟡 **A instalação da lib passou de wheel para `pip`** (PR 7194, João, 03/09), com feed privado.
`latest` ou vazio instala sem pin; versão específica vira `nlp-engine==<versão>`. ⚠️ Muda a
conversa sobre fixar versão por especialidade — o mecanismo agora existe.

**Decisões que MUDARAM e valem a partir de agora:**

- 🔴 **Produção deixa de usar `latest`.** Cada especialidade declara qual versão da lib usa; `latest`
  fica para o time de DS.
- 🔴 **Fluxo de branch:** PR para `hml` publica a wheel no volume de **dev** (validação nossa);
  PR para **`main`**, com code review de Diego/João/Gabriel, publica em **hml e prd**.
- 🔴 **Todo PR na lib passa por code review**, com matriz de impacto na descrição.

**Cards criados em 21/08:** `283644` (juiz por contorno, em execução) · `283645` (fluxo, Diego) ·
`283646` (DPO, em execução) · `283647` (contrato) · `283648` (`[P0-29]` juiz sem evidência).
`282904` (o incidente) **encerrado**.

**P0/P1 sem card, e são nossos:** doc do consumidor + mensagem de erro · vazamento de memória no
`process()` · lib não emite log no caminho NLP.

## O bloco `runtime` e o contrato — 🟡 PLANO ESCRITO, AGUARDA REVISÃO DO USUÁRIO

📄 **`_processo/alinhamentos/alinhamento-configuracao-nlp-2026-09-15.md`** — pedido de acordo com o time de
plataforma. 🔴 **Nada se altera nas configs antes desse alinhamento**, para o trabalho entrar no
backlog deles com capacity.

🔴 **O `runtime.llm_router` SOBREPÕE o `nlp.llm_router`** (`ntb_ia_loader.py:104-113`), e é o
resultado que o motor lê. A SPEC 27 §2.1 e §6.1 afirmam o contrário.

**O que o `runtime` sobrepõe hoje** — 5 chaves em 3 configs; as outras três já são coerentes:

| config | chave | em `nlp` | executa |
|---|---|---|---|
| hepatologia | `enabled` | ausente → `False` | **`True`** |
| hepatologia | `api_key_env` | ausente | `DATABRICKS_TOKEN` |
| hepatologia | `fallback_policy` | `keep_current` | **`positive_in_band`** |
| tirads | `enabled` | ausente | `False` |
| transplante_pulmao | `enabled` | **`False`** | **`True`** |

🔴 **Na hepatologia não é só liga/desliga** — `fallback_policy` decide o comportamento quando a
chamada ao LLM falha. Medido em 15/09: **156 chamadas ao juiz**, `llm_router_mode: llm` em 12.184
de 12.184 laudos. No transplante a config declara `False` e o juiz roda.

✅ **O bloco deve sair, por ter perdido a função:** existia para sobreposição via widget, e
**nenhum dos 15 widgets do runner o alimenta**. Só `runtime.llm_router` é lido — `runtime.profile`
está em 5 das 6 configs e nunca é consultado.

🔴 **A ordem importa:** declarar o efetivo no `nlp` **antes** de remover o bloco. Inverter desliga o
juiz na hepatologia e muda a política de falha, sem erro e sem log.

**A SPEC 27 tem CINCO divergências com o código** — `runtime` · `gold_query` do transplante (já
corrigido) · lookbehind "sem caminho por config" (está em produção) · nomes dos catálogos
(`diamond_ia_*` contra `diamond_fabrica_ia_*`) · "dicionário literal e nada mais".
⚠️ **E o `boas-praticas/02` Passo 6 repete o erro** — é o guia que se segue ao criar linha nova, e
instrui a preencher `runtime` com `enabled: False` "por documentação". Card `299238`.

ℹ️ **O PR 7228 está parado desde 08/09 esperando exatamente esta história** — o retorno registrado
pede "abrir uma história e levar para o próximo refinamento marcando o que deve ser mudado".

## Alinhamento com o Ops — pauta consolidada em 2026-09-10

📄 **`_processo/alinhamentos/alinhamento-ops-2026-09-10.md`** reúne **19 itens em 7 temas**, cada um com
evidência medida, o que se pede e quem decide. **Três bloqueiam trabalho hoje:** o índice
`fabrica-ai-hml` em dev, o pin por especialidade, e o aval dos campos novos de contrato.

Traz pauta de **75 minutos** e a lista do que já fechou do nosso lado, para a agenda não gastar
tempo com isso. Vira ata em `_processo/atas/` depois da reunião.

🟡 **Tema 7 — governança do uso de agentes.** Circulou restrição ao uso de agentes de IA sobre
conteúdo de revisão de código; houve esclarecimento posterior de que o alvo são **times externos**,
o que resolve a aplicação imediata e **não fecha o item**. Permanece: a diretriz não foi emitida por
canal com mandato (sem PO, PMO ou Head), as duas frentes envolvidas são lideranças técnicas de mesmo
nível em disciplinas distintas, e escopo esclarecido em conversa não alcança quem não estava nela.
**Quem decide subiu para PO / PMO / Head** — não é decisão entre pares técnicos.

## Os POPs da Fábrica — 🟡 EXISTEM, NENHUM VIGENTE

**Dez documentos** em `.alt.doc/POPs/`, cobrindo ciclo de vida de ML, modelo HuggingFace, coleta de
dados, biblioteca Python, esteira DevOps, padronização de objetos, catálogos e schemas, edição da
NLP Platform, e controle de versões.

🔴 **Todos em `1.0`, com `Vigência: a definir na aprovação` e `Aprovado por: a definir`.** O
POP-IA-09 declara: *"nenhum elo da cadeia está em uso hoje… este documento é o modo de trabalho a
ser adotado, não a descrição do que já acontece."* **Aprová-los é o item de maior alavancagem** da
pauta com o Ops.

**O que eles já resolvem, e muda o nosso discurso:**

- **POP-IA-08 (Edição da NLP Platform)** define a fronteira de alçada em três quadros. *Você edita:*
  `ntb_ia_<especialidade>_config.py` e `jobs/definicoes/<job>.json`. *Dono do NLP Engine:* a lib e o
  `ORGANS_SHARED`. *Administrador Databricks:* cluster, policy, catálogo, schema, grants, Volume,
  `mlops.yml`, ambientes e `jobs/clusters`.
  ✅ **`gold_filter` é nossa alçada** — vive no config da especialidade. Sai da pauta de Ops.
  ℹ️ **A versão do motor é do Dono do NLP Engine** — sustenta a nossa posição no pin.
  ℹ️ `jobs/definicoes/<job>.json` é nosso, e é onde a versão é declarada por linha. **Trocar
  `${nlp_engine_version}` por literal pode pinar sem tocar na infraestrutura** — a confirmar.
  ⚠️ **O gate está escrito numa direção só:** diz a quem o Cientista recorre antes de tocar cada
  quadro, e não diz a quem o dono de um quadro recorre antes de alterá-lo.
- **POP-IA-08 §13 lista cinco bugs conhecidos.** Dois cruzam com o que levantamos: **bug 5** é o fuso
  UTC da janela de datas (com contorno *"evite agendar 21h–00h"*, sem correção), e **bug 2** é a
  dedup apontando fixo para hepatologia/dev, com a saída duplicando — ⚠️ **pode ser a causa real do
  card `300201`**, que precisa ser cruzado.
- **POP-IA-04 (Bibliotecas Python)** declara **layout flat, sem `src/`**, com a `rededor-ai-lib` como
  referência canônica. 🟡 **A `nlp-engine-lib` usa `src/`** — divergência a declarar por nós.

📄 **`_processo/alinhamentos/pauta-minima-ops.md`** — 9 itens, só o que está aberto e depende do Ops. A pauta
longa (`alinhamento-ops-2026-09-10.md`) vira documento de apoio com a evidência completa.

## Cards — quadro em 2026-09-08 (tarde)

✅ **Em *Pronto para QA*, os 15 da `0.12.x`:** `253573` `P2-07` · `253574` `P2-08` · `253575`
`P2-09` · `253576` `P2-10` · `253577` `P2-11` · `253578` `P2-12` · `253583` `P2-17` · `253585`
`P2-19` · `253586` `P2-20` · `253587` `P2-21` · `253588` `P2-22` · `253589` `P2-23` · `253590`
`P2-24` · `253592` `P3-26` · `253594` `P3-28`. Cada um comentado com a evidência dos seus
critérios — 67 evidências medidas contra a árvore mergeada, não contra a branch de trabalho.

**Nossos, em execução:** `285305` TI-RADS (defeito 1 medido e corrigido; falta o defeito 2 =
`0.15.0`) · `283644` juiz por contorno · `283647` contrato lib↔plataforma (fica aberto, só alinha
quando os bumps fecharem) · `283648` `[P0-29]` juiz sem evidência (= `0.14.0`) · `298598` Feature:
plano de bumps.

**Para a plataforma:** `298596` `limit_rows` não isola coorte · `298600` `embedding_model` por
ambiente · `299238` **SPEC 27 contradiz o código** (card acumulador — registrar ali os ajustes
que aparecerem, alinhar de uma vez com o `283647`).

✅ **Os três achados sem card foram abertos em 08/09:**

- **`300200`** (Defect **P1**, nosso) — âncora ausente sai do gate. É a `0.12.2`.
- **`300201`** (Defect P2, **plataforma**) — texto de entrada duplicado `2n+1` vezes: 6 laudos em
  4.507. É a montagem da entrada, antes da lib. Custa LLM proporcional e pode truncar por
  `max_input_chars`.
- **`300202`** (Defect P2, nosso) — hepatologia descarta 86% na segmentação:
  `segmentation_coverage` < 1,0 em **3.867 de 4.507**, com 3.196 cabeçalhos descartados. É o
  `mode: auto`, única linha assim. No ca-rim a mesma correção recuperou +25 laudos em 6 dias.
  ⚠️ Medir A/B antes de trocar: sem gabarito clínico, não há como arbitrar o delta.

**Encerrados:** `298597` (a `0.11.1`) · `299423` (a `0.11.2`).

✅ **`299111` reumatologia em *Pronto para QA***, comentado com a evidência.

## A refinar — anotado, não iniciado

- 🟡 **Estudo: como o LLM é ligado e desligado na lib — BACKLOG LOCAL, sem card (15/09).**
  Decisão do usuário: **não abrir card agora**; analisar quando possível e então incluir num bump
  específico ou embutir noutro que mexa em algo próximo na lib.
  **O que motiva:** o LLM é chamado de **três lugares independentes** — `llm_router_backend.py` (o
  juiz, 53 ocorrências), `quantitative.py` (extração de medida, 15) e `ordinal_extraction.py`
  (`llm_fallback`, 8) — e **nove chaves** participam da decisão, espalhadas por até 10 arquivos:
  `llm_router`, `decision_mode`, `use_embeddings`, `llm_fallback`, `uncertainty_band`,
  `ambiguity_band`, `relevance_mode`, `api_key_env`, `api_key`.
  **Sintomas já medidos:** `ambiguity_band` é inerte em `decision_mode: hybrid` · a contabilidade de
  tokens cobre só o juiz (**221 das 270 chamadas diárias sem registro**) · `api_key` literal vence
  `api_key_env` · desligar de um lado e religar de outro é possível e silencioso.
  ⚠️ **Muda contrato** — alinhar antes de implementar, como a régua exige. O resultado do estudo é
  que decide em qual bump entra.


- 🟡 **Expansão léxica por similaridade (fuzzy) na lib — DECISÃO EM ABERTO.** É o par da
  `0.12.2`: sozinha, a correção troca falso positivo por falso negativo nos laudos com texto
  corrompido. Medido com `difflib.SequenceMatcher`, `min_ratio` 0,84 (o do legado):
  `nodulo`↔`no◆dulo` **0,923 casa** · `ulcera`↔`u◆lcera` **0,923** · `lesao`↔`les◆ao` **0,909** ·
  controles negativos rejeitados (`nodulo`↔`deulcera` 0,429; `nodulo`↔`figado` 0,167).
  ✅ **Serve as 4 linhas** e não depende de infraestrutura — é `difflib`, biblioteca padrão.
  Contraste: os embeddings estão **não-funcionais em produção** (`FileNotFoundError` em 86% a
  100% dos laudos nas 3 linhas que os usam).
  ⚠️ **Não substitui embeddings:** `sacroiliaca`↔`sacroileite` dá 0,727 e não casa.
  ⚠️ Só **aumenta** recall — exige medição por linha antes de subir. O limiar é régua: 0,84
  rejeita `nodulacao` (0,800).

- 🟡 **TR4 sem evidência de tamanho quando houver PAAF** (sugestão do Natan). É **exceção**, não
  mudança da régua geral: PAAF como gatilho alternativo ao tamanho. Perguntas em aberto: a lib já
  expressa "gate dispensado quando outro achado está presente", ou é feature nova? Qual a
  volumetria — PAAF é raro e nem todo laudo com PAAF vem sem medida. Prioridade **abaixo** dos
  defeitos em curso. Candidato a delegar.
- 🟡 **Migração dos algoritmos legados**, na ordem **reumatologia** (começar o quanto antes),
  **ateromatose**, **doenças biliares**, **neuroimunologia**. A levantar: inventário de cada régua,
  existência de gabarito, volumetria, e o que reaproveitar do padrão que ca-cólon e ca-rim já
  produziram.

## Dívidas transversais

- ✅ **GRANT `USE CATALOG` em `mlops_fabrica_ia` CONCEDIDO** — verificado em 23/09: o catálogo
  responde, não dá mais `PERMISSION_DENIED`. Destrava a camada semântica em dev e hml.
  🟢 **E o efeito é medido: HML roda 100% com modelo real.** Em 23/09, **65.306 laudos em quatro
  linhas, ZERO `token_overlap`** — `tumor_osseo` 53.805, `cancer_rim` 4.991, `hepatologia` 4.858,
  `tirads` 1.652.
  🔴 **PRODUÇÃO continua em fallback, com a MESMA `config_version`:** `cancer_estomago` 100%,
  `hepatologia` 99,4%, `cancer_rim` 98,5%, `tirads` 85,4%. **Isola a causa: não é a lib nem o
  modelo, é o caminho por ambiente.** É o card `305810`, em *Pronto para QA* com o João.
  🔴 **CONFERIDO EM 25/09, e NÃO eram coortes diferentes — é o MESMO LOTE.**

  | | PRD | HML |
  |---|---|---|
  | config | `0.1.13-hep-emb-volume` | **idêntica** |
  | engine | `0.12.3` | **idêntica** |
  | laudos | 4.538 | **4.538** |
  | relevantes | **61** | **911** |
  | `token_overlap` | **4.494 (99,0%)** | **0 (0,0%)** |

  **A única variável é a camada semântica.** Produção entrega **6,7%** do que a mesma configuração
  produz com os embeddings funcionando. O `305810` deixa de ser risco de infraestrutura e passa a
  ter consequência medida.
  ⚠️ **Isso NÃO estabelece que os 911 estão certos.** A hepatologia é justamente a linha com a
  régua cega por segmentação (`300202`), e boa parte do que a semântica promove ali é **o termo
  literal da régua na parte do laudo que ela não vê**. Qual das duas pontas está clinicamente
  correta segue **sem gabarito** — o que está provado é que os dois ambientes rodam perfis
  diferentes, e só um foi homologado.

- 🟡 **Histórico do grant (19/09), mantido para rastreabilidade:**
  O PR 7321 apontou o `embedding_model` de **seis configs** para o Model do Unity Catalog
  `mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm`. **O caminho funciona** — provado no
  run de 16/09, 10.000 de 10.000 com `[sentence_transformers]`, com a identidade de quem o criou.
  **Nós não temos acesso**, e o run em dev falha no passo `config`, antes de o motor existir:
  `PERMISSION_DENIED: User does not have USE CATALOG on Catalog 'mlops_fabrica_ia'`.
  ✅ **É grant, não endereço** — provado comparando a mensagem de erro: catálogo inexistente devolve
  *"Catalog ... does not exist"*; este devolve *"does not have USE CATALOG"*. O catálogo existe no
  mesmo metastore de hml e prd (`azure:eastus2:cc473134…`).
  🔴 **Quatro jobs quebram na `hml` às 04:00 de segunda** — `hepatologia`, `cancer_rim`, `tirads` e
  `cancer_estomago` têm `embedding_enable=true`. `ateromatose` e `cancer_colon` declaram o mesmo
  Model e ficam **inertes** por `use_embeddings: False`.
  🟢 **Produção não é afetada:** a `main` está 68 commits atrás e tem **zero** referência ao Model —
  ainda usa os caminhos de Volume. Verificado: runs normais em prd em 18/09, 4 linhas, `0.12.3`.
  ⚠️ **O pedido precisa nomear DUAS identidades:** a nossa e a **de serviço que roda os jobs**. A
  validação de 16/09 foi interativa; identidade de job não herda acesso pessoal.
  🔴 **Bloqueia a validação da `0.13.0` em dev** — e não é problema da lib.

- 🔴 **UM grant bloqueia TRÊS frentes — `USE CATALOG security` + `EXECUTE` em
  `security.prd.rdsl_decrypt`.** Aparece como pendência no DII (falha da view de exportação em dev),
  no PR 7275 da ateromatose (*"as cinco colunas saíram CIFRADAS"*), e é candidato a explicar o
  terceiro caso, medido em 15/09: **a view do TI-RADS entrega `nome_paciente` e `medico_solicitante`
  em base64 em 89 de 89 linhas**, em prd e em hml, enquanto a da reumatologia entrega em claro.
  **Não são três pendências — é uma.** Tratar como item único com a plataforma.

- ✅ **`main` sincronizada com a `hml`** (21/08, ambas na `0.9.4`). Estava 167 commits atrás e
  **causou a queda do TI-RADS em produção** — a esteira publica prd a partir da `main`. Débito que
  estava catalogado como "decisão de repositório" e era, na verdade, risco de produção.
- ✅ **HISTÓRICO REESCRITO EM 17/09 — o PHI saiu, e o push está liberado.**
  Autorizado explicitamente pelo usuário; os 98 commits **nunca foram publicados**, então ninguém
  mais tinha esse histórico.
  **Verificado depois da reescrita:** zero ocorrências de CSV clínico em qualquer ref · zero objetos
  alcançáveis · as **3 fixtures sintéticas preservadas** · os 16 arquivos **intactos em disco** ·
  os 4 commits mantidos com assunto e conteúdo restante (`765ec42`, `17d86c3`, `e4b3dc9`, `99b41a5`).
  ✅ **`origin/main` inalterado em `528498b` e ainda ancestral do HEAD** — o push é **fast-forward,
  sem `--force`**. HEAD em `6ba3bc5`, **98 commits a subir**.
  ℹ️ O commit que só removia os 16 do índice foi **podado por ficar vazio** — depois da reescrita não
  havia o que apagar. A explicação vive no diário de 17/09.
  ✅ **PUSHADO em 17/09** — `528498b..10762a1`, 99 commits, fast-forward, confirmado pela REF.
  **`docs/` deixa de estar sem backup**, pela primeira vez desde que a dívida foi registrada.
  **Backups guardados fora do git**, para descarte depois da confirmação do push:
  `_backup-git-projects-2026-09-17/` (cópia integral do `.git` anterior) e
  `_dados-clinicos-backup-2026-09-17/` (os 16 CSVs).
  ⚠️ **O `.gitignore` da raiz ignora a si mesmo** (primeira linha) — a regra de LGPD vive só nesta
  máquina e não viaja com o repositório. Item em aberto.

- 🔴 **A base ouro não tem lugar oficial.** Gabarito vive em planilha, e-mail e arquivo
  temporário — sem `spec_version`, sem `dt_anotacao`, sem dono. Custou uma conclusão errada em
  20/08. A sandbox do Datahub (Diego) **não cobre** isso: nosso caso é o inverso, artefato que já
  nasce oficial.
  ✅ **Destino decidido em 02/09: o LAKE, não o repositório.** Alinhamento com o Diego em curso
  sobre schema e formato, com reuso posterior e treinamento de modelo proprietário no horizonte.
  Enquanto isso os harnesses saíram do diretório temporário do job — que é apagado junto com ele —
  para `Desktop/Rede D'Or/_ferramentas/`, fora do git.
- ✅ **A CAMADA SEMÂNTICA RODOU COM MODELO REAL PELA PRIMEIRA VEZ — dev, 16/09, `cancer_rim`.**
  📄 `_processo/medicoes/diagnostico-embeddings-run-joao-2026-09-16.md`. Branch `feature/embedding` da
  plataforma (João): o `embedding_model` passa a ser um **Model do Unity Catalog**
  (`mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm`), resolvido pelo `ConfigLoader`
  para um path local no driver antes de o config chegar ao motor.
  **10.000 laudos, `[sentence_transformers]` em 10.000, ZERO fallback.**
  🟡 **Reportado como "não retorna embedding", e não é falha:** o `cancer_rim` declara
  `similarity_threshold: 0.92` e o **máximo observado foi 0,9654**, com apenas **2 laudos ≥ 0,92**
  em 10.000 (11 ≥ 0,90, 85 ≥ 0,85). Não promover era o objetivo declarado do 0,92 — e os
  candidatos que o comentário da config previa **agora existem**, com score gravado.
  ⚠️ **Os dados NÃO provam que foi o UC que carregou:** `embedding_model` não é emitido no blob, e
  em dev o path antigo do Volume também resolve. Quem distingue é a linha de log do driver.
  🔴 **E a query que circulou estava errada:** o acessor é `$.decision_trail.steps.semantic`, não
  `$.decision_trail.semantic` — com o caminho errado o campo vem nulo e a leitura inverte.
  ℹ️ **Não fecha o card `305810`** — ali o problema é o caminho em produção; este run é dev.

- 🔴 **OS EMBEDDINGS NÃO FUNCIONAM EM PRODUÇÃO — remedido em 15/09, e são QUATRO linhas.**
  hepatologia **12.094 de 12.184 (99,3%)** · cancer_rim **7.763 de 7.892 (98,4%)** ·
  tirads **3.155 de 3.659 (86,2%)** · cancer_estomago **728 de 728 (100%)**.
  ✅ **Pré-condição atendida:** a camada semântica foi exercitada em **100% dos laudos** das quatro.
  🔴 **O `cancer_rim` prova que trocar o literal não resolve:** aponta para `gold_fabrica_ia_hml`
  desde o PR 7159 e falha igual — é o volume de homologação, e a linha roda em produção.
  📄 Card `305810` — *[Plataforma NLP] Modelo de embeddings sem caminho válido em produção*, criado
  em 15/09, com causa raiz escrita. O `298600` ficou com a nossa metade, bloqueado por ele.
  ℹ️ Medição anterior, de 10/09, com as três linhas que os declaravam:
  rodam em `token_overlap`, e a trilha registra laudo a laudo:
  `"semantic": "... [token_overlap] FALLBACK:FileNotFoundError"`.

  | linha | laudos | com `FileNotFoundError` | % |
  |---|---|---|---|
  | hepatologia | 5.172 | **5.108** | **98,8%** |
  | cancer_estomago | 201 | **201** | **100%** |
  | tirads | 1.568 | **1.348** | **86,0%** |

  **Causa:** `embedding_model` aponta para `/Volumes/diamond_ia_hml/...`, o Volume do workspace
  ANTIGO, em **caminho literal idêntico nos três ambientes**. Em produção ele não existe.
  🔴 **A consequência é de qualidade:** a config declara `decision_mode: hybrid` com embeddings, e
  o que executa é régua mais sobreposição de tokens — **produção roda um perfil que nunca foi
  homologado**. A monitoria não pega, porque a régua sustenta a taxa.
  ⚠️ **A nota anterior dizia "hoje responde, o risco é latente" — não se sustenta.** O risco está
  materializado, e provavelmente desde antes: só ficou visível porque a `0.11.0` instrumentou a
  queda.
  ℹ️ O **ca-rim já foi corrigido** (aponta para `gold_fabrica_ia_hml` desde o PR 7159), mas não está
  em produção. As três que estão são as três que falham.
  ✅ **MiniLM já copiado** para `gold_fabrica_ia_hml/nlp_engine/nlp_engine_lib/st_models/` (01/09).
  🔴 **Produção não tem destino, reconfirmado em 08/09:** `gold_fabrica_ia` tem apenas `fhir` e
  `information_schema` — não possui o schema `nlp_engine`.
  Criar schema é do time da Fábrica. Card **`298600`**, com o ponto em aberto: como resolver o
  caminho **por ambiente** — mesma classe do `base_url` do LLM, resolvido no PR 7135.
  ℹ️ O `mpnet-base-v2` fica **fora de escopo**: só existe no volume antigo e serve notebooks legados.
- `git-steward` ainda é 78 linhas sempre carregadas; candidato a virar regra curta + skill.
