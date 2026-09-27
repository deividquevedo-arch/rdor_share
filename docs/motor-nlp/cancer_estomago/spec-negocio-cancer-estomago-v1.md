# SPEC de negócio — Linha de cuidado Câncer de Estômago (V1)

**Versão:** 2.0 · **Data:** 2026-08-25 · **Config:** `0.6.9-cancer_estomago` · **Motor:** `nlp_engine >= 0.9.4`
**Status:** decisão de escopo da úlcera **revista**; implementação em validação
**Fonte:** revisão de 37 laudos pelo Dr. Giovanni Targa · e-mail sobre lesões pré-malignas (Targa) ·
respostas da Carol de 2026-08-20 · **homologação de 120 casos pelo Targa, 2026-08-24** · retorno da
Carol sobre a classificação de Sakita, 2026-08-24.

> **O que mudou da 1.0 para a 2.0.** A decisão de 18/08 — *"toda úlcera gástrica entra"* — foi
> medida contra a homologação de 120 casos e **revista**: precisão de 0,267, com 54 dos 55 falsos
> positivos contendo úlcera, e a úlcera sendo o **único achado** em 52 deles. Entra agora só a
> úlcera com sinal de alarme ou acompanhada de outro achado suspeito. Ver §3.3 e §4.
**Segue o** [template de SPEC](../_fundacao/templates/template-spec-especialidade-v0.md).

---

## 1. Objetivo

Captar, em endoscopia digestiva alta, o laudo que deve entrar na navegação de oncologia gástrica.

**Princípio, acima da lista de palavras:** entra o laudo em que **há uma decisão em aberto que a
navegação pode acelerar e o paciente ainda não está capturado**. Achado descrito na **conclusão do
exame atual** entra; doença citada em indicação, histórico ou nota **não** entra — ali o paciente já
está em outra linha, ou não há nada a acelerar.

O princípio foi derivado das 37 marcações do Targa e explica 10 das 11 conferidas.

## 2. Universo de exames

**Entram:** endoscopia digestiva alta (EDA).
Filtro textual: `gold_filter.keywords: ['endoscop.a digestiva alta', 'eda']`.

⚠️ **Declarar o filtro é obrigatório.** A plataforma nova lê **somente** `filters.gold_filter.keywords`;
o `gold_query` do runner legado é ignorado em silêncio. Sem o filtro, a entrada foi de **4.818.237
laudos** em vez de 10.783.

## 3. Régua

### 3.1 O que entra

| achado | observação |
|---|---|
| neoplasia / câncer / carcinoma maligno | núcleo do V1 |
| tumor maligno, massa, processo expansivo | benigno declarado não conta |
| Bormann I–IV, linite plástica, lesão infiltrativa, vegetante, úlcero-vegetante, estenosante, deprimida, elevada com depressão central | morfologias de gravidade |
| linfoma gástrico | inclusive recidiva |
| recidiva / lesão residual | precedência absoluta sobre tratamento prévio |
| **úlcera gástrica com sinal de alarme, ou acompanhada de outro achado suspeito** | escopo revisto em 2026-08-24 — ver §3.3 |
| **tumor neuroendócrino (TNE)** | qualquer tipo, com ou sem tipo declarado |

### 3.2 O que NÃO entra

| achado | por quê | custo se entrasse |
|---|---|---|
| pólipo (incl. glândulas fúndicas), gastrite atrófica, metaplasia, displasia de qualquer grau | decisão do Targa | — |
| **lesão subepitelial indeterminada** | não está nas palavras-chave; maioria é lipoma e pâncreas ectópico (Carol) | **+3,9 laudos/dia** |
| **área elevada** | não está nas palavras-chave; fica para versão futura, com critério de tamanho (Carol) | +0,3 laudos/dia |
| **úlcera isolada de aspecto benigno** — péptica, erosiva, rasa, em cicatrização, com fibrina, ou estadiada por Sakita/Sakita-Miwa (A1, A2, H1, H2) — sem sinal de alarme e sem outro achado | achado corriqueiro de endoscopia, não oncológico (homologação de 120, 24/08) | **era a maior fonte de falso positivo: 52 dos 55** |
| úlcera **cicatrizada** / cicatriz de úlcera (Sakita S) | mucosa já reepitelizada | +1,5 laudos/dia |
| úlcera de **anastomose / bypass** | complicação mecânica pós-cirúrgica | −3 laudos |
| achado maligno **fora** do estômago (esôfago, duodeno, orofaringe, JGE) | outra linha de cuidado | +0,2 laudos/dia |
| retração cicatricial, gastrite isolada ou combinada com pós-cirúrgico benigno | achado benigno nomeado | — |

### 3.3 Origem de cada termo e limiar

**Úlcera gástrica** — Targa, por e-mail: *"Vamos considerar somente as úlceras, pois podem ser lesões
neoplásicas. Restante das palavras vamos continuar desconsiderando."* A pergunta enumerava *"pólipos,
gastrite atrófica, metaplasias, displasia e **úlceras (com e sem sinais de malignidade associados)**"* —
por isso a decisão cobre as duas formas, e **revoga** a regra da especialista de 2026-08-04, que só
promovia úlcera com sinal morfológico de malignidade.

**Sakita H entra, Sakita S sai** — em fase de cicatrização a lesão ainda é ativa, e um
**adenocarcinoma ulcerado mimetiza úlcera péptica em cicatrização**, exigindo reavaliação com biópsia
até a cura completa. Em fase de cicatriz a mucosa já reepitelizou e o risco de tumor oculto sob a
lesão foi descartado.

**TNE** — Targa marcou relevante o laudo de seguimento pós-ressecção; Carol confirmou (*"entra
todos"*). Revoga a exigência de tipo 2 ou 3, que a própria régua declarava **provisória**.

**Linfoma** — Carol: *"pode deixar o linfoma"*. O Targa marcou **relevante** um laudo com
`recidiva_tumoral; linfoma` e **não relevante** um MALT em seguimento — ver §5.

## 4. Decisões fechadas

| # | decisão | quem | data |
|---|---|---|---|
| 1 | dos pré-malignos, **só úlcera** entra; pólipo, gastrite atrófica, metaplasia e displasia seguem fora | Targa | 2026-08 |
| 2 | TNE entra, todos os tipos | Carol / Targa | 2026-08-20 |
| 3 | área elevada **não** entra nesta versão | Carol | 2026-08-20 |
| 4 | lesão subepitelial **não** entra | Carol | 2026-08-20 |
| 5 | úlcera de cicatriz e de anastomose **fora** | Carol | 2026-08-20 |
| 6 | Sakita H entra, Sakita S sai | negócio | 2026-08-20 |
| **7** | **úlcera ISOLADA de aspecto benigno NÃO entra.** Entra a úlcera com sinal de alarme descrito nela — lesão ulcerada, escavada, bordas irregulares ou elevadas, aspecto úlcero-infiltrativo, etiologia a esclarecer, biópsia motivada por suspeita — **ou** acompanhada de outro achado maligno/suspeito no exame atual | Targa (homologação de 120 casos) | **2026-08-24** |
| **8** | **Sakita indica benignidade, mas não prevalece sobre sinal de alarme.** Medido: dos 56 laudos do lote que citam Sakita, o negócio aprovou 8; excluir todos seria regressão (0,591 / 0,619) | Carol + medição | **2026-08-25** |

> ⚠️ **A decisão 7 revisa a leitura da decisão 1.** "Só úlcera entra, dos pré-malignos" continua
> valendo como *escopo* — nenhum outro pré-maligno entrou. O que mudou é **qual** úlcera: a de
> 18/08 era toda; a de 24/08 é a que tem sinal de alarme. Foi o próprio Targa quem revisou, na
> homologação, e não uma reinterpretação nossa.

## 5. Decisões abertas

| # | pergunta | espera | desde |
|---|---|---|---|
| 1 | **MALT em seguimento** conta como progressão? Targa marcou não relevante um laudo com aumento de número e extensão das áreas; a régua trata recidiva com precedência absoluta | Targa | 2026-08-20 |
| 2 | achado maligno **fora do estômago** (orofaringe): Targa marcou relevante, a régua recusa por órgão | Targa | 2026-08-20 |
| 3 | **área elevada** com critério de tamanho, em versão futura | Carol | 2026-08-20 |
| **4** | **Qual é o critério que separa, já que sinal de alarme não separa?** Aplicado ao pé da letra, o critério da decisão 7 captura 10 laudos de úlcera isolada e acerta 3 — precisão de 30%. Três laudos aprovados não têm sinal de alarme nenhum. A pergunta não é o que a régua deixa de ler: é se o critério escrito está certo, ou se a decisão usa contexto do paciente que o laudo não carrega. **3 casos enviados em 25/08.** | Carol | **2026-08-25** |

## 6. Quem decide

| papel | quem | alcance |
|---|---|---|
| **dono de negócio** | Dr. Giovanni Targa | escopo clínico — **a palavra dele vence** |
| PO | Monique | escopo de entrega e priorização do produto |
| PMO | Natan | prazo, agenda e coordenação das frentes |
| cientista de dados, médica de formação | Carol | régua clínica, interpretação do escopo acordado, homologação |
| tech lead de ciência de dados | Deivid | arquitetura, métrica e plataforma; apoia a Carol e o time |

## 7. Mapeamento para o motor

| critério | camada |
|---|---|
| todos os achados da §3.1 | `findings` (regra determinística) |
| separação anatômica de `ulcera` e `tne` | `exclude` / `unless` + `skip_organ_gate` |
| indicação, histórico e notas fora da régua | `findings_policy.ignore_sections` |
| filtro de precisão | `llm_router` (juiz) |

**Consequência prática para esta SPEC:** toda inclusão de escopo tem de virar **achado de regra**.
Instrução no prompt do juiz não adiciona escopo — a decisão de TNE ficou inerte na `0.6.0` por ter
sido feita só ali.

**Banda de incerteza `[0.60, 0.97]`.** Define **quando** o juiz é chamado, e é o único parâmetro de
juiz que esta SPEC fixa. Sem span positivo, o score de regra é no máximo `0.35`; com o composto
`0.62·regra + 0.38·semântico`, o teto **analítico** de um laudo sem achado é **0,597** — e o medido
foi **0,5876, idêntico em dois runs**, confirmando que o teto é estrutural e não amostral. O piso de
laudo com achado é apenas empírico (**0,774**, n=21.566), então a folga vai toda para esse lado, e
não para o meio do vão. Revalidar sempre que mudarem pesos, política de score ou régua.

> ⚠️ **Débito de arquitetura, não decisão desta linha.** Hoje esse piso de `0,60` também está
> cumprindo o papel de impedir que o juiz promova um laudo **sem nenhuma evidência de regra**. Isso é
> **invariante da lib** — juiz decidir sem evidência quebra a arquitetura — e deveria ser garantido
> em código, não por parâmetro de config, senão cada especialidade nova pode violá-lo baixando a
> banda. Registrado em `DIVIDA-TECNICA.md` (documento removido da lib).

## 8. Gabarito

- **Origem:** revisão de 37 laudos, retorno do negócio
- **Quem anotou:** Dr. Giovanni Targa
- **Quando:** anterior ao e-mail da §4.1
- **Sob qual critério:** régua V1, **antes** da decisão sobre úlceras
- **Tamanho:** 37 laudos, 15 marcados relevantes

⚠️ **A anotação é anterior ao critério explícito.** Cinco dos laudos que ele marcou relevantes ficam
fora por decisão posterior (§3.2), então o recall máximo alcançável contra este gabarito é **0,667**,
não 1,000. Um sexto era promoção do juiz sem evidência de regra, bloqueada por princípio na §4.7 — o
que põe o teto prático em **0,600**.

## 9. Baseline e volumetria

Janela **2026-05-01 a 2026-06-30** (61 dias), **10.783 laudos**, catálogo `diamond_fabrica_ia_dev`.

| config | relevantes | /dia | recall | precisão | juiz chamado | run |
|---|---|---|---|---|---|---|
| `0.4.0` | 20 | 0,3 | 0,467 | 0,700 | — | — |
| `0.5.0` | 103 | 1,7 | 0,600 | 0,750 | 3.199 | 169 min |
| `0.5.1` | 103 | 1,7 | 0,600 | 0,750 | 3.199 | 181 min |
| `0.6.0` | 74 | 1,2 | 0,533 | **1,000** | **341** | **45 min** |
| **`0.6.1`** | **75** | **1,2** | **0,600** | **1,000** | 345 | 45 min |

**`0.6.1` é a versão candidata.** Atinge o teto declarado na §8 (0,600) sem nenhum falso positivo no
lote revisado, e **nenhum laudo entregue sai sem achado**. Da `0.6.0` para a `0.6.1` houve **uma
única transição**, `0 → 1`, no laudo de TNE que o negócio havia marcado relevante — nenhuma perda.

Os 6 falso-negativos remanescentes são **todos** decisão registrada, não falha de régua: dois estão
fora por §3.2 (área elevada, subepitelial), dois por regra de órgão e cicatriz, e dois eram promoção
do juiz sem evidência de regra, hoje bloqueada por arquitetura.

⚠️ **41% do corpus são laudos sem texto na origem** — teto de recall que não depende da régua.

### 9.0 Volumetria com a régua vigente — 2026-08-26

Janela de **61 dias** (01/05 a 30/06): **10.782 laudos** de **9.862 pacientes**. Um laudo por
paciente entre os relevantes.

| | entregas em 61 dias | **corretas** | **erradas** | por dia |
|---|---|---|---|---|
| `0.6.2` — o que roda em hml | 75 | 20 | **55** | 1,23 |
| **`0.6.9`** | **14** | **13** | **1** | **0,23** |

Comparação **exata, não extrapolada**: as 75 entregas da janela são as mesmas 75 do lote homologado,
e as 14 da `0.6.9` também estão todas dentro dele.

**Projetado para um mês de 30 dias:** de **37 entregas com 10 corretas** para **7 entregas com 6,4
corretas**.

> **A troca, em linguagem de operação:** perde-se **~3,5 pacientes corretos por mês** para deixar de
> enviar **~26,5 errados**. É essa a decisão do negócio — não precisão e recall.

⚠️ **Volume baixo em termos absolutos.** 7 casos por mês é pouco para dimensionar uma fila de
navegação, e a pergunta que decorre disso — se a linha se sustenta nesse volume — é do negócio, não
da régua. O número está aqui para que ela seja feita com dado.

---

### 9.1 Os 8 não entregues — classificados pelo critério ESCRITO aqui

⚠️ **Chamar os 8 de "falso negativo" atribui à régua uma divergência que nem sempre é dela.**
Classificados pelo que a decisão 7 desta SPEC sustenta:

| | quantos | o que são |
|---|---|---|
| **A. A SPEC sustenta a relevância** | **5** | o laudo descreve sinal de alarme que o critério cobre — `a esclarecer` (2), `lesão ulcerada`, `retração de pregas`, `bordas elevadas`. **Falha de implementação:** 4 foram rebaixados pelo gate com o sinal escrito no laudo. |
| **B. A SPEC não sustenta** | **3** | sem sinal de alarme e sem outro achado. Não é defeito da régua: ela fez o que está especificado. É **divergência entre a anotação e o critério acordado**. |

**Duas leituras da mesma medição:**

| | valor | o que mede |
|---|---|---|
| recall contra a **anotação** | 13/21 = **0,619** | quanto capturamos do que o especialista marcou |
| recall contra o **critério da SPEC** | 13/18 = **0,722** | quanto capturamos do que está acordado por escrito |

A segunda é a que mede a implementação. A primeira mede implementação **e** aderência da anotação
ao critério, somadas — e reportá-la sozinha esconde qual das duas falhou.

### 9.1.1 🔴 O critério da decisão 7 também diverge da anotação

Medido nos 57 laudos de úlcera isolada do lote: aplicar **"sinal de alarme entra"** ao pé da letra
captura 10 laudos e acerta **3** — precisão de **30%**, contra taxa de base de 8,8%.

Ou seja: **não é só a régua que não reproduz a anotação. O critério escrito nesta SPEC também não.**
Corrigir a implementação dos 5 do grupo A subiria o recall e **derrubaria a precisão**, porque
traria junto os 7 que o especialista recusa.

Isso reposiciona a pergunta aberta nº 4: não é *"o que a régua não está lendo"*, é **"qual é o
critério que separa, já que sinal de alarme não separa"**. É a pergunta que está com a Carol.

### 9.2 Teto de recall medido — 2026-08-26

Contra a homologação de 120 casos do Targa, a régua captura **13 de 21** relevantes.
**Os 8 restantes não são separáveis pelos atributos visíveis no laudo**, verificado por três
medições independentes sobre os 57 laudos de úlcera isolada do lote (5 aprovados — taxa de base
**8,8%**):

| hipótese testada | n | taxa de aprovação | ganho sobre a base |
|---|---|---|---|
| qualquer sinal de alarme | 10 | 30,0% | 3,4× |
| menção a **biópsia** | 51 | 9,8% | **1,0× — sem sinal** |
| lesão ulcerada · retração de pregas | 5 cada | 20,0% | 2,3× |
| grande curvatura · hematina | 15 · 31 | 13,3% · 12,9% | 1,5× |
| incisura / pequena curvatura | 46 | 6,5% | 0,7× |
| múltiplas úlceras · controle/seguimento | 14 · 5 | 7,1% · 0% | ≤ 0,8× |

Nada passa de 1,5× com amostra que sustente. Descrições praticamente idênticas recebem veredito
oposto — *"convergência de pregas … biopsiada"* aparece em **5 recusados e 2 aprovados**.

**Consequência:** com 5 positivos em 57, qualquer combinação de sinais fracos é sobreajuste ao lote,
não régua. ⚠️ **Ler junto com a §9.1:** desses 8, apenas **3** são desta natureza. Os outros 5 têm evidência
que a SPEC sustenta e são falha de implementação. O teto **por limitação do dado** é 18/21 = 0,857,
não 0,619 — enquanto a decisão aberta nº 4 não for respondida.

### 9.3 Critério objetivo encontrado e NÃO implementado

**Nenhuma úlcera menor que 10 mm foi aprovada** — 0 de 13. Entre 10 e 19 mm, 11,1%; acima de 20 mm,
12,5%.

É sinal de **exclusão**, não de captura: os 8 falsos negativos já são todos ≥ 10 mm, então o corte
não recupera ninguém. Fica registrado porque é o único critério objetivo que o lote sustenta — se o
negócio pedir recall maior, ele é a trava que permite afrouxar em outro ponto sem devolver os 52
falsos positivos.

⚠️ Medido sobre **um** lote de 120. Antes de virar régua, precisa de confirmação clínica.

---

## 10. Output e homologação

`findings` entregue ao negócio com o rótulo legível (`Úlcera`, `Tumor neuroendócrino`).

**Nenhum laudo entregue pode sair com a coluna de achado vazia** — é o teste de aceite da cascata, e
a `0.6.0` fechou em **zero** (eram 19 na `0.5.1`).

Homologação: lote gerado pela versão candidata, revisado pelo negócio.

## 11. Armadilhas verificadas

- [x] **cola de acento** — 27% dos laudos com úlcera só tinham a forma colada (`deúlcera`); `\b` inicial removido na `0.5.1`, +101 laudos casando
- [x] **laudo de uma linha** — **70% do corpus**; nenhum começa com seção ignorada, então não houve supressão total
- [x] **`ignore_sections`** — `notas` não casava `Nota:`; corrigido, 14,3% do corpus tem essa seção
- [x] **gate de órgão** — derrubava 7 laudos de úlcera gástrica real; `skip_organ_gate` no `ulcera` e no `tne`, validado com controle positivo **e** negativo
- [x] **dedup da entrada** — `reprocess_enable` usado em todos os runs; volume conferido antes de cada métrica
- [x] **`segmentation.mode`** — `full_doc`, sem risco de perder a conclusão
- [x] **config engolida** — todos os testes atravessaram `load()` → `get_nlp_config()`
- [x] **vocabulário estrangeiro** — não se aplica: config nativa, não migrada de notebook
- [x] **negação fora de alcance** — `negation.window` era 10 e a frase de cromoscopia do NBI tem
  **exatamente 10 palavras** entre a negação e o termo: falhava por uma. Criava achado `Neoplasia`
  falso em **187 laudos** da janela de 10.782. Corrigido na `0.6.9` (janela → 12): 148 eliminados
  (79%) e **57% menos chamadas ao LLM**. ⚠️ Declarar a frase inteira como negação **não** resolve —
  o motor ancora a janela no *início* da frase; verificado em bancada.
- [x] **simulação não prevê LLM** — regex sobre o laudo acertou exatamente a parte determinística
  (TP e FN) e errou por 15 na parte de julgamento. Onde há juízo, só medindo.

## 12. Histórico SPEC ↔ config

| SPEC | config | data | decisão que mudou |
|---|---|---|---|
| v1.0 | `0.5.0` | 2026-08-19 | úlcera gástrica entra (Targa); neoplasia precoce deixa de ser encoberta por Paris |
| v1.0 | `0.5.1` | 2026-08-20 | *(sem mudança de decisão — correção do regex de úlcera)* |
| v1.0 | `0.6.0` | 2026-08-20 | cascata regra → juiz; TNE; escopo final da úlcera; `ignore_sections` |
| v1.0 | `0.6.1` | 2026-08-20 | *(sem mudança de decisão — achado `tne`, que a `0.6.0` não implementou)* |
| v1.0 | `0.6.2` | 2026-08-21 | *(sem mudança de decisão — `label` em 9 achados, `enabled` explícito)* |
| **v2.0** | `0.6.3` | 2026-08-24 | **úlcera isolada deixa de entrar** — gate por critério qualitativo ancorado no achado |
| v2.0 | `0.6.4` | 2026-08-25 | *(tentativa revertida — Sakita deixou de ser veto e a pergunta alargou o TRUE: precisão caiu para 0,500)* |
| v2.0 | `0.6.5` | 2026-08-25 | *(tentativa revertida — 2º critério em `ulcera_suspeita` custou 2 verdadeiros para tirar 1 falso)* |
| **v2.0** | `0.6.6` | 2026-08-25 | **prompt do juiz alinhado à decisão 7** — mandava promover toda úlcera "com ou sem sinais de malignidade" |
| v2.0 | `0.6.7` | 2026-08-25 | *(tentativa revertida — precedência de alarme sobre benignidade: +2 FP, 0 TP recuperado)* |
| v2.0 | `0.6.8` | 2026-08-25 | *(sem mudança de decisão — reverte a `0.6.7`, régua idêntica à `0.6.6`)* |
| v2.0 | `0.6.9` | 2026-08-26 | *(sem mudança de decisão — `negation.window` 10 → 12; mesma métrica, 148 achados falsos a menos e 57% menos LLM)* |
