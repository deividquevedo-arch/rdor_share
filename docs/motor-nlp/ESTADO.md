# Estado das frentes

> **Documento vivo.** É a única fonte de "onde cada coisa está agora". Carregado em toda sessão
> via `@` no `CLAUDE.md`, então sobrevive à compactação.
>
> **Regra:** aqui vai **estado** (o que está feito, o que falta, de quem depende). Fato durável e
> lição aprendida vão para a memória (`/memory`). Se uma linha aqui não muda há meses, ela é fato —
> mova para lá. Se uma memória tem data e "estado atual", ela é estado — mova para cá.
>
> Atualizado em **2026-09-10**.

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
- 🔴 **A `0.12.0` do FEED é inutilizável.** Um `409 Conflict` publicou no feed antes da correção do
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
⚠️ **A regra foi quebrada nesta entrega** — `waive`, `gate_waived_by` e `gate_waived_error`
entraram na lib antes do alinhamento. Exposição real é zero (nada emite sem config), mas o
procedimento é alinhar antes. Memória ampliada para cobrir **chave de config**, não só saída.

⚠️ **O `gold_filter` segue sem punção, e é a causa MAIOR:** 67 exames citando TI-RADS 4 em 16 dias
nunca chegam ao motor, contra 36 rebaixados pelo gate. Sem medição e sem card — filtro de entrada
só se mede rodando.

## `0.13.0` — 🟡 2 DE 6 CARDS, BRANCH PUSHADA SEM PR

Branch `feat/0.13.0-estrutura` (`e832f7c`, `9313809`). ✅ **SPEC salva na `hml`** (`fa9e7ea`) —
existia só em branch local havia 7 dias.

- ✅ **`253579` [P2-13] singleton do spaCy.** A consolidação já existia; faltava o que o card
  descreve: **inicialização sem lock, publicando o objeto ANTES do `add_pipe`**. Uma segunda
  thread recebia pipeline **sem sentencizer**, e o `decision_pipeline` degradava em SILÊNCIO para
  `full_doc`. Corrigido com double-checked locking + publicação atômica. **5 mutantes mortos**;
  memória: **−580,2 KB por processo**.
- ✅ **`253581` [P2-15] divisão do módulo.** 778 → **407** linhas, mais três de 207, 87 e 210.
  **CA5 provado: 300 laudos, ZERO divergentes, byte-a-byte.**
  ⚠️ Três desvios declarados: primitivos de frase e `OrdinalMention` foram para o módulo de
  categoria (evitam ciclo); **o CA2 não fecha ao pé da letra** — pede nenhum arquivo acima de 300
  linhas e **dez excedem**, `quantitative.py` tem 1.687; os nomes `test_rads_*` do CA6 são
  anteriores ao rename de 0.9.0.
- 🟡 Faltam `253580` (decompor `process()` **sem** streaming), `253582` (clamp — **pode alterar
  valor**, e a SPEC manda sair para release própria se alterar), `253591` e `253593` (ADRs).
- 🔵 **`process()` com streaming vira BUMP PRÓPRIO** — decisão do usuário: quebra a API e a
  plataforma já contorna montando lotes externamente.

ℹ️ **A fila da lib é nossa.** Nenhum dos seis cards do Ops depende deles para começar; onde o Ops
é gargalo é em **config** — `waive`, `gold_filter`, pin e índice de dev.

🟡 **Plano de bumps registrado** — card `298598` e `docs/plano-acao-backlog-lib-2026-09.md`.
✅ `0.12.0`–`0.12.3` **entregues** → 🟡 `0.13.0` estrutura (2 de 6) → `0.14.0` **juiz sem evidência
+ tokens na extração quantitativa** (card `283648`, P1 aberto há 20 dias sem medição registrada;
o item de tokens é inclusão de 10/09) → `0.15.0` vínculo lesão↔medida.

🔴 **P0-29 segue aberto** — o juiz pode promover sem evidência de regra. Card `283648`, alocado na
`0.14.0`. Exige medição prévia por linha.

⚠️ **Branches sem push:** `docs/plano-e-specs-ops` (plano dos 28 cards + SPEC da `0.13.0`) e
`docs/0.11.1-impacto-medido`. Conteúdo absorvido, **exceto a SPEC da `0.13.0`** — descartar as duas
implica reescrevê-la.

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

## Hepatologia

🔴 **Única especialidade em `segmentation.mode: auto`** — provavelmente descarta IMPRESSÃO/CONCLUSÃO
de todo laudo. **Nunca medido.** Mensurável desde a 0.8.3 (`segmentation_coverage`).
No ca-rim, a mesma correção recuperou **+25 laudos em 6 dias**.

## Câncer de estômago — ✅ EM PRODUÇÃO desde 2026-09-04

Config **`0.6.9-cancer_estomago`** (gate da úlcera isolada), engine `0.10.1`. PRs 7166 e 7187
mergeados pelo João.

**Primeiro dia em prd (04/09):** 151 laudos · 6 relevantes (3,97%) · **zero entregue sem achado**
(eram 36) · zero erro de LLM. O gate rebaixou **5 de 11** laudos com achado léxico.

🔴 **MAS os 6 relevantes eram FALSO POSITIVO** — todos pelo defeito do espaço colado, corrigido na
`0.11.2`. Com a correção seriam **11 relevantes em vez de 17** na coorte de 333 laudos.

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

## Migração dos algoritmos legados — as três seguintes

Ordem indicada: **ateromatose**, **doenças biliares**, **neuroimunologia**. A reumatologia
produziu o padrão a reaproveitar: clonar a branch `hml` do repo legado (nunca a cópia local),
gerar a config programaticamente do `CONFIG`, e medir paridade contra a saída gravada.

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

## Plataforma / MLOps

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
[`_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md`](_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md)
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
⚠️ **O Volume não é mais rota alternativa:** `gold_fabrica_ia_hml/.../nlp_engine_lib/` parou na
**0.9.4**, e o widget `nlp_engine_volume` foi **removido** no PR 7233. Isso também responde o item
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

## Alinhamento com o Ops — pauta consolidada em 2026-09-10

📄 **`_processo/alinhamento-ops-2026-09-10.md`** reúne **17 itens em 6 temas**, cada um com
evidência medida, o que se pede e quem decide. **Três bloqueiam trabalho hoje:** o índice
`fabrica-ai-hml` em dev, o pin por especialidade, e o aval dos campos novos de contrato.

Traz pauta de 60 minutos em quatro blocos e a lista do que já fechou do nosso lado, para a agenda
não gastar tempo com isso. Vira ata em `_processo/atas/` depois da reunião.

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

- ✅ **`main` sincronizada com a `hml`** (21/08, ambas na `0.9.4`). Estava 167 commits atrás e
  **causou a queda do TI-RADS em produção** — a esteira publica prd a partir da `main`. Débito que
  estava catalogado como "decisão de repositório" e era, na verdade, risco de produção.
- 🔴 **9 CSVs com texto de laudo** nos commits locais da raiz — bloqueia push de `docs/`, que
  portanto está **sem backup**.
- 🔴 **A base ouro não tem lugar oficial.** Gabarito vive em planilha, e-mail e arquivo
  temporário — sem `spec_version`, sem `dt_anotacao`, sem dono. Custou uma conclusão errada em
  20/08. A sandbox do Datahub (Diego) **não cobre** isso: nosso caso é o inverso, artefato que já
  nasce oficial.
  ✅ **Destino decidido em 02/09: o LAKE, não o repositório.** Alinhamento com o Diego em curso
  sobre schema e formato, com reuso posterior e treinamento de modelo proprietário no horizonte.
  Enquanto isso os harnesses saíram do diretório temporário do job — que é apagado junto com ele —
  para `Desktop/Rede D'Or/_ferramentas/`, fora do git.
- 🔴 **OS EMBEDDINGS NÃO FUNCIONAM EM PRODUÇÃO — medido em 10/09.** As três linhas que os declaram
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
