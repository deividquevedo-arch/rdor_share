# Alinhamento com o time de Ops — pauta

> **Pauta, não ata.** Cada item traz **tópico · evidência · proposta · card sugerido**. Depois da
> reunião vira ata em `_processo/atas/`, com o que passou a valer.
>
> Consolidado em **2026-09-10**, revisto em **2026-09-11** contra os **dez POPs da Fábrica**.
> Verificação feita nos repositórios, no board e nos catálogos de produção. Nenhum item entrou sem
> evidência conferida.

---

## Regra de organização — um card, um dono

🔴 **Card com responsabilidade dividida não fecha.** Feita a parte de um lado, ele segue aberto pela
parte do outro, e ninguém consegue dizer se está pronto. Onde o trabalho é dos dois, o item vira
**dois cards ligados**, cada um com dono único e critério de aceite próprio.

Vale para os cards existentes que hoje misturam donos — marcados com **⚠️ separar**.

---

# Bloco 0 — fundação

Dois itens que destravam quase todo o resto.

## 0.1 Equalizar a ata de 21/08

**Evidência.** `_processo/atas/` está vazio. De 21/08 existem a transcrição bruta da ferramenta e o
`_fundacao/design/mapa-gaps-lib-plataforma-2026-08-21.md`, que é documento **de um lado só**. Nenhum
registro acordado entre os dois times diz o que ficou decidido.

**Consequência observada.** "O que foi combinado" é contestável, e foi contestado na definição do
pin de versão em 10/09.

**Proposta.** Fechar a ata de 21/08 com os dois lados, e registrar ata de toda agenda de interface.

**Card sugerido:** `[Acordo] Equalizar a ata do alinhamento de 21/08` — dono: quem convocou.

---

## 0.2 🔴 Aprovar os POPs — nenhum dos dez está vigente

**Evidência.** Os dez POPs estão em **versão 1.0**, com **`Vigência: a definir na aprovação`** e
**`Aprovado por: a definir`**. O POP-IA-09 é explícito: *"Nenhum elo da cadeia está em uso hoje…
este documento é o modo de trabalho a ser adotado — não a descrição do que já acontece."*

**Consequência.** O material existe, é bom, e **não obriga ninguém**. Toda discussão de processo
recomeça do zero porque não há norma a invocar — o que aconteceu duas vezes esta semana.

**Proposta.** Aprovar e datar os dez, ou os que estiverem prontos. **É o item de maior alavancagem
da pauta:** vários pedidos abaixo deixam de existir no instante em que o POP correspondente passa a
valer.

**Card sugerido:** `[Acordo] Aprovar e datar os POPs da Fábrica` — dono: quem os escreveu, com
aprovação formal.

---

# Bloco A — Ops / Fábrica

## A1. Dev depende do feed de produção — a `hml` da lib não tem consumo

**Evidência.** O cluster de dev resolve o `pip` contra `fabrica-ai`, que é o feed de **produção**.
Erro em 09/09: `Could not find a version that satisfies nlp-engine==0.12.2 (from versions: 0.9.4,
0.10.0, 0.10.1, 0.11.2)` — a lista é o conteúdo do feed de prd. O widget `nlp_engine_volume` foi
removido e não há índice extra declarado em nenhum arquivo da plataforma.

✅ **A wheel existe.** O volume de HML tem `nlp_engine-0.12.1`, `0.12.2` e `0.12.3`, e a esteira da
lib segue publicando lá (`UploadVolume`). O que falta é o consumo.

ℹ️ O POP-IA-04 registra *"Feed: há um por ambiente"* — a estrutura está prevista; o consumo em dev é
que não aponta para o de homologação.

**Consequência.** Só se valida em dev uma versão que **já está em produção** — o que anula a função
da branch `hml` da lib e obriga a promover sem validar, ou a não validar.

**Proposta.** Religar o consumo: restaurar a leitura do volume de HML, **ou** declarar
`fabrica-ai-hml` como índice extra do `pip` no cluster de dev — e só de dev.

**Card sugerido:** `[Plataforma NLP] Restabelecer o consumo da lib em dev` — dono Ops.
⚠️ **separar de `283645`**, onde hoje está enterrado.

---

## A2. O gate do POP-IA-08 existe numa direção só

**Evidência.** O POP-IA-08 §5 estabelece que **alinhamento prévio é anterior à revisão de PR**:

> *"O alinhamento não é uma etapa da revisão de Pull Request — é anterior a ela."*
> *"Tocar nisso sem alinhar antes não é agilidade — é o que produz o incidente que alguém mais vai
> precisar diagnosticar depois."*

E a fronteira de alçada é explícita: o Cientista de Dados **não edita** `mlops.yml`,
`azure-pipelines.yml`, `jobs/clusters/` nem `jobs/ambientes/`; e **`ORGANS_SHARED` e a versão do
motor são do Dono do NLP Engine**.

A **Figura 2** desenha três quadros: *você edita* (`ntb_ia_<especialidade>_config.py` e
`jobs/definicoes/<job>.json`) · *Dono do NLP Engine — alinhar antes de tocar* (`nlp-engine-lib` e
`ORGANS_SHARED`) · *Administrador Databricks — alinhar antes de tocar* (cluster, node type, policy;
catálogo, schema, grants, Volume; `mlops.yml`, ambientes, `jobs/clusters`).

🟡 **A matriz é escrita de um ponto de vista só — o de quem calibra.** Ela diz a quem o Cientista de
Dados recorre antes de tocar em cada quadro, e **não diz a quem o dono de um quadro recorre antes de
alterá-lo**. `jobs/ambientes/` e a policy pertencem ao Administrador Databricks; alterá-los muda o
caminho de instalação de quem consome, e nenhuma cláusula pede alinhamento nesse sentido.

**O que se observou.** PR 7194 (03/09, troca wheel por `pip`, 18 arquivos, toca
`jobs/ambientes/prod.json`, **descrição vazia**) e PR 7233 (08/09, troca a policy e remove o volume
da configuração do job). **As duas foram identificadas por tentativa em 09/09**, não por comunicação
prévia. O efeito é o item A1.

**Proposta.** Estender o POP-IA-08 §5 com a coluna inversa: mudança em caminho de instalação,
policy de cluster, arquivo de ambiente ou contrato é alinhada com o Dono do NLP Engine **antes do
merge**, com o mesmo registro que o POP já exige na outra direção.

ℹ️ Não é régua nova — é a mesma régua, na direção que falta.

**Card sugerido:** manter `283645`, com escopo redefinido para isso — dono Ops.

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

## A6. `dt_execucao_modelo` em UTC — **já consta como bug conhecido**

**Evidência.** O POP-IA-08 §13 lista cinco bugs conhecidos, e o **bug 5** é este: *"Janela de datas
em hml/prd usa UTC… a janela pode deslocar um dia; evite agendar 21h–00h."*

Conferido no código: a gravação é em UTC e a view de exportação filtra pela data local (BRT). Run
entre 21:00 e 00:00 produz **view vazia, sem erro**.

ℹ️ **Não é achado nosso** — está documentado pelo próprio time, com contorno operacional ("evite
agendar"), sem correção.

**Proposta.** Definir se o contorno é a solução permanente ou se entra na fila de correção. Hoje a
única defesa é lembrar da regra.

**Card sugerido:** `[Plataforma NLP] Bug 5 do POP-IA-08 — fuso de UTC na janela de datas` — dono Ops.

---

## A7. Entrada recebe o documento RTF cru

**Evidência.** `exm_laudo_texto` vem de `laudo_original`. Em 4.321 laudos do TI-RADS, **116 (2,7%)
são o documento RTF inteiro** e ocupam **63,4 dos 68,6 MB do dia**; o maior tem 815 KB. O mesmo
struct traz `laudo_transformado`, com o texto limpo — vazio em 158 dos 4.321.

**Proposta.** Avaliar `coalesce(laudo_transformado, laudo_original)`, com custo medido antes.

**Card sugerido:** `[Plataforma NLP] Entrada recebe RTF cru havendo texto extraído` — dono Ops.

---

## A8. Provisionamento de schema

**Evidência.** O schema `nlp_engine` não existe em `gold_fabrica_ia`, que tem apenas `fhir` e
`information_schema`. É o destino do modelo de embeddings em produção.

ℹ️ `reumatologia` já foi provisionado — a linha entrou em produção em 11/09.

**Proposta.** Provisionar `nlp_engine` em `gold_fabrica_ia`.

**Card sugerido:** `300348`, nomeando o schema que falta — dono Ops.

---

## A9. ℹ️ O pin por linha pode estar na nossa alçada — a confirmar

**Evidência.** A Figura 2 coloca `jobs/definicoes/<job>.json` no quadro **"você edita"**. É nesse
arquivo que cada linha declara `"nlp_engine_version": "${nlp_engine_version}"` — uma **variável**,
resolvida por `jobs/ambientes/<amb>.json`, que é do Administrador Databricks e hoje está em
`latest`.

**A pergunta.** Substituir a variável por um literal (`"0.12.3"`) na definição do job fixa aquela
linha **sem tocar em `jobs/ambientes/`** — e a definição está no nosso quadro.

ℹ️ Se funcionar, o pin por linha deixa de depender de mudança na infraestrutura e passa a ser PR de
config. ⚠️ **Não é para fazer sem alinhar:** muda comportamento em produção, e o POP-IA-08 §5 diz
que alinhamento antecede a revisão de PR. Mas muda quem executa, e é a pergunta mais barata da
pauta.

**Card sugerido:** resolver dentro da história `301938` — dono a definir conforme a resposta.

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

**Card sugerido:** `298600` ⚠️ **separar** — a config é nossa; o schema em prd vai para A8.

---

## B2. `gold_filter` não seleciona punção

**Evidência.** O exame que originou o relato do negócio sobre o TI-RADS **não chega ao motor**:
`data.filters.gold_filter.keywords` seleciona por tireoide e pescoço, e *punção aspirativa por
agulha fina guiada por ultrassonografia* não casa nenhum termo. São **67 punções citando TI-RADS 4
em 16 dias barradas na entrada**, contra 36 rebaixadas pelo gate. Custo de incluir: **+492 exames em
16 dias**, ~31/dia, sobre linha que processa ~3.600/dia.

⚠️ Filtro de entrada é invisível para A/B local — só se mede rodando, e isso depende de A1.
✅ **Alçada confirmada na Figura 2 do POP-IA-08:** `gold_filter` vive dentro de
`ntb_ia_<especialidade>_config.py`, que está no quadro *"você edita"*. **Não exige alinhamento** —
é PR de config pelo caminho normal.

**Proposta.** Incluir os termos, com medição em dois dias distintos.

**Card sugerido:** `[NLP Engine] gold_filter não seleciona punção` — dono DS.

---

## B3. SPEC 27 contradiz o código

**Evidência.** Card `299238` em estado **Novo e sem dono**. É a dependência declarada para destravar
o PR 7228, que está com voto `-10` desde 08/09.

**Proposta.** Atribuir, levar ao refinamento e fechar com os ajustes acumulados.

**Card sugerido:** `299238`, com dono — DS.

---

## B4. A `nlp-engine-lib` diverge do layout declarado no POP-IA-04

**Evidência.** O POP-IA-04 §3 define: *"**Layout flat**: pacote na raiz do repositório, **sem
diretório `src/`**. É o padrão adotado."* A referência canônica dele é a `rededor-ai-lib`.

A `nlp-engine-lib` usa `src/nlp_engine/nlp_engine/`, com `pythonpath=["src"]`.

ℹ️ **Levantado por nós, não apontado.** A lib é anterior ao POP, e o layout `src/` é o recomendado
pela comunidade Python — evita import acidental do diretório de trabalho em vez do pacote instalado.

**Proposta.** Ou o POP acomoda os dois layouts com o critério de escolha, ou registramos a
divergência como deliberada. O que não serve é o POP dizer uma coisa e a lib principal fazer outra.

**Card sugerido:** `[NLP Engine] Declarar a divergência de layout ante o POP-IA-04` — dono DS.

---

# Bloco C — acordo entre os dois

## C1. Contrato de entrada e de saída

**Evidência.** A **saída** tem piso versionado de 21 chaves, extraído de run real. A **entrada** não
tem contrato nenhum: nem coluna de origem declarada (ver A7), nem tipos, nem obrigatoriedade. Campos
novos do blob (`waive`, `gate_waived_by`, `gate_waived_error`, tokens da extração quantitativa)
aguardam aval.

ℹ️ O POP-IA-04 §1 já enuncia o princípio — *"contratos de entrada e saída explícitos e testáveis,
sem depender de convenção tácita"*. Falta aplicá-lo à interface lib ↔ plataforma.

**Proposta.** Declarar e versionar os dois lados do contrato.

**Card sugerido:** `283647` ⚠️ **separar em dois** — contrato de entrada, dono Ops; contrato de
saída, dono DS. Ligados entre si.

---

# Fora da pauta

**Definição de "validado" ao trocar versão** — proposta escrita em
[`procedimento-promocao-de-versao.md`](procedimento-promocao-de-versao.md). ℹ️ **Passa a ter destino
natural: um POP.** Escrita no formato da wiki, encaixa como POP-IA-10 ou como seção do POP-IA-04.

**Card `300201`** (texto de entrada duplicado `2n+1`) — **sai da pauta e volta para nós**. O bug 2 do
POP-IA-08 descreve dedup apontando fixo para hepatologia/dev, com a saída duplicando. Pode ser a
mesma causa, e o card precisa ser cruzado antes de ir a qualquer reunião.

**Uso de agentes na revisão de código** — 🔒 **tema sensível, sem card.** Nota ao final da agenda,
para conhecimento do time, com cautela.

---

# O que já fechou do nosso lado

- ✅ Defeito da esteira que pulava a publicação em produção — corrigido.
- ✅ `0.12.2` e `0.12.3` publicadas nos dois feeds e validadas.
- ✅ **O LLM voltou a funcionar em produção** — 270 chamadas em 10/09, zero erro.
- ✅ Reumatologia **em produção**, legado desligado, com o exchange no formato do `cancer_rim`.
- ✅ 15 cards de higiene da `0.12.x` em *Pronto para QA*, com 67 evidências medidas.
- ✅ Lista de versões por linha entregue — história `301938`.
- ✅ Feature `298598` atualizada: 8 versões entregues, `0.13.0` com 2 de 6 cards.

---

# Resumo

**16 itens** — 2 de fundação · 9 do Ops · 4 nossos · 1 de acordo.

🔴 **Três bloqueiam trabalho hoje:** A1 (consumo em dev), B1 (embeddings) e C1 (aval dos campos
novos).

⭐ **O item 0.2 é o de maior alavancagem.** Aprovar os POPs resolve, de uma vez, a ausência de norma
que faz cada discussão de processo recomeçar do zero — e A2, A6 e C1 mudam de natureza no instante
em que ela existe.

⚠️ **Quatro cards precisam ser separados por dono:** `283645` (A1 sai), `298600` (B1 × A8), `283647`
(C1 vira dois), `300348` (nomear o schema).

✅ **A Figura 2 do POP-IA-08 foi lida** e fechou o item B2: `gold_filter` está no quadro
*"você edita"*. Segue por ler a Figura 1 (o fluxograma do gate), que não altera nenhum item desta
pauta.
