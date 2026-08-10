# SPEC de negócio — Linha de cuidado Tireoide **V3**

**Versão:** 3.0 · **Data:** 2026-08-10 · **Config:** `0.3.0-tirads` · **Motor:** `nlp_engine >= 0.8.1`
**Status:** implementado, aguardando homologação clínica

> **V3 = V2 (imagem) + cintilografia de tireoide + exame de sangue.**
> As regras de imagem são **idênticas** às homologadas na V2 — nada foi alterado ali.

---

## 1. Objetivo

Rastrear pacientes com doença tireoidiana a partir de laudos, para captação na linha de cuidado.
A decisão é binária: **encaminhar** ou **não encaminhar**.

---

## 2. Régua de IMAGEM (V1/V2 — ratificada em 2026-08-10)

| achado | conta? | condição |
|---|---|---|
| Nódulo | ✅ | **dimensão ≥ 1 cm discriminada no laudo** |
| Cisto | ✅ | **dimensão ≥ 1 cm discriminada no laudo** |
| TI-RADS | ✅ | TR5 e TR6. ⚠️ **TR4 sozinho NÃO aprova** — exige nódulo ≥ 1 cm |
| Massa · tumor · neoplasia | ✅ | pela **presença** (independe de medida) |
| Linfonodo | ✅ | suspeito, patológico ou indeterminado |
| Bócio | ✅ | **nodular** (multinodular, mergulhante) |

**Não contam:**

- nódulo ou cisto **< 1 cm**, ou **sem a dimensão discriminada** (sem medida não se confirma ≥ 1 cm)
- **aumento difuso** da glândula sem nódulo — tireoidopatia difusa, bócio difuso homogêneo
  (tratamento clínico, sem foco cirúrgico)
- textura difusa ou heterogênea sem nódulo ou cisto
- linfonodo **reacional**, habitual ou pós-operatório
- pós-operatório / pós-tireoidectomia sem achado
- achado **negado** ou ausente

**Na dúvida entre benigno/inespecífico e relevante, prefira NÃO relevante.**

### Decisões fechadas em 2026-08-10

| # | questão | decisão |
|---|---|---|
| 1 | TR4 sozinho promove? | **Não.** Exige nódulo ≥ 1 cm |
| 2 | Faixa 1,0–1,5 cm tem zona cinzenta? | **Não.** ≥ 1 cm aprova |
| 3 | Linha segue órgão ou especialidade? | **Órgão: TIREOIDE.** Não é cabeça e pescoço |
| 4 | Paratireoide | **Fora do escopo** |

⚠️ A decisão 3 encerra a divergência com a avaliação de 2026-07-30, em que um cirurgião de cabeça e
pescoço marcou achados de parótida, laringe e mandíbula. **Esses ficam fora.**

---

## 3. Régua de SANGUE (V3 — nova)

### 3.1 Regra de uso

> **Redefinida em 2026-08-03:** sangue e imagem valem **isoladamente**. Não há exigência de janela
> temporal entre eles, nem de exame de imagem pareado. **Cada analito captura o paciente sozinho.**

### 3.2 Critérios

| analito | limiar | unidade | interpretação |
|---|---|---|---|
| **TSH** | **< 0,4** | mUI/L | hipertireoidismo |
| **T4 livre** | **> 1,8** | ng/dL | hipertireoidismo |
| **T3 livre** | **> 6,3** | pg/mL | hipertireoidismo (T3-toxicose) |
| **T3 total** | **> 1,81** | ng/mL | hipertireoidismo (T3-toxicose) |
| **TRAb** | **> 1,5** | UI/L | **doença de Graves** |
| **Anti-TPO** | **> 34** | IU/mL | **autoimunidade tireoidiana** |

### 3.3 Origem de cada limiar

**TSH e T4 livre** — spec clínica original, confirmados pela Carol.

⚠️ O T4 livre foi passado em **pmol/L** (> 23) e o lake registra em **ng/dL**. Convertido
(`ng/dL × 12,87 = pmol/L`), 23 pmol/L = **1,79 ng/dL** — as duas fontes concordam. Copiar `23`
direto nunca seria atingido (p95 do lake = 1,6) e **falharia em silêncio**.

**TRAb** — corroborado pelo dado: o limite superior da faixa de referência do próprio laboratório
tem mediana **1,76**.

**T3 livre e T3 total** — o limiar **é** o limite superior da faixa de referência que o laboratório
grava em campo estruturado, com concentração quase total: **6,3** em 98% dos T3 livre e **1,81** em
99,9% dos T3 total.

🔴 **RESSALVA ABERTA, para a homologação decidir:** faixa de referência é *"acima do normal"*, o que
**não é necessariamente** *"relevante para captação"*. A spec original dizia apenas "elevado". Este é
o único critério do V3 cujo corte ainda não tem aval clínico explícito.

**Anti-TPO** — a spec original o classificou como **qualitativo** (reagente / não reagente). O dado
mostra o contrário: é **numérico com censura à esquerda** (2.263 de 3.962 vêm como `"Inferior a 0,2"`).
O limiar de 34 IU/mL captura 418 de 1.462 com valor (28,6%).

### 3.4 Valores censurados

Resultado laboratorial nem sempre é um número. `"Inferior a 0,01"` significa que o valor está
**abaixo** de 0,01 — é o **TSH indetectável**, o sinal mais forte de hipertireoidismo manifesto.

O motor trata o valor como **intervalo** e só conclui quando ele **inteiro** satisfaz o limiar.
Intervalo que cruza o corte fica indeterminado, sem decidir.

São 133 casos/mês, **12,1% da população relevante de TSH** — um tratamento ingênuo os perderia.

### 3.5 Valores implausíveis

**187 TSH/mês chegam com valor `0`** — ausência gravada como zero. Como 0 satisfaz `< 0,4`, virariam
falso-positivo (~15% das promoções). O motor rejeita valores fora da faixa fisiológica, sem decidir.

---

## 4. Régua de CINTILOGRAFIA (V3 — nova)

**Somente cintilografia de tireoide** — 14 exames/mês.

**Fora:**

| exame | volume/mês | motivo |
|---|---|---|
| cintilografia de paratireoide | 11 | órgão fora do escopo |
| corpo inteiro com iodo-131 | 17 | pesquisa de metástase **pós-tireoidectomia** — paciente já tratado |
| miocárdio, fígado, refluxo, gálio-67, óssea, cerebral | ~1.480 | outros órgãos |

⚠️ Não existe seleção por "cintilografia" genérica: das 1.523 do mês, apenas 25 são tireoidianas.

---

## 5. Universo de exames

**Entram na seleção**, por nome do exame:

`tireoide` · `tireóide` · `pescoço` · `pescoco` · `tsh` · `tireoestimulante` · `t4 livre` ·
`t3 livre` · `t3 total` · `iodotironina` · `trab` · `anti receptor do tsh` · `anti-tpo` ·
`antitireoperox`

**Ficam fora, por decisão:**

- **região cervical/supraclavicular** e **PAAF/biópsia de linfonodo** — não constam da lista de
  exames da spec. São 6 positivos conhecidos; incluí-los custaria 2,7× o lote
- **T3 reverso** — outro analito, faixa própria
- **TSH neonatal** e teste do pezinho — faixa de referência diferente da do adulto
- **testes genéticos** que contêm "t3" no nome (mutação FLT3, bcr/abl)
- painéis de fenilalanina

---

## 6. Como a decisão é tomada

| camada | papel |
|---|---|
| regra léxica | encontra o achado no texto (nódulo, cisto, linfonodo, massa, tumor, bócio) |
| gate quantitativo | exige a **dimensão ≥ 1 cm**; sem medida, rebaixa |
| extração TI-RADS | lê a categoria e aplica a política |
| **medida laboratorial** | lê o valor **sem LLM** e aplica o limiar em código |
| juiz LLM | decide apenas na **zona de incerteza** |

**Os critérios de sangue não usam LLM.** O laudo laboratorial **é** o valor, e o código o lê
diretamente — determinístico, reprodutível e sem custo de inferência.

### Saída

Cada laudo devolve as colunas de achados com **nome clínico**:

| coluna | exemplo |
|---|---|
| `findings` | `Nódulo; Cisto` · `Hipertireoidismo` |
| `findings_spans` | `Hipertireoidismo (0.15mUI/L < 0.4)` |
| `findings_match` | trecho do laudo, para auditoria |

Nomes usados: `Nódulo` · `Cisto` · `Massa` · `Linfonodomegalia` · `Tumor` · `Bócio` ·
`Hipertireoidismo` · `Doença de Graves` · `Autoimunidade tireoidiana`

---

## 7. Volume esperado

Medido em junho/2026.

| fonte | exames/mês | taxa de relevância | **encaminhamentos** |
|---|---|---|---|
| imagem | 22.059 | 19,1% | ~4.200 |
| sangue | ~90.000 | 1,7% | ~1.500 |
| cintilografia de tireoide | 14 | a medir | — |

⚠️ **Paciente ≠ laudo.** Cerca de 25% dos relevantes são exames repetidos do mesmo paciente. Se o
envio for por paciente, o volume cai proporcionalmente.

---

## 8. Fora do escopo do V3

**V3.2 — sangue positivo → buscar USG com doppler.** É lógica **entre** exames e pacientes; o motor
decide laudo a laudo. Exige desenho próprio.

**Bethesda** (punção e biópsia) — deferido na V2 por ausência de ganho medido.

**Ecocardiograma e cateterismo** — pertencem a outra linha de cuidado.

---

## 9. O que ainda depende de decisão clínica

| # | pendência | com quem |
|---|---|---|
| 1 | **Limiar do T3** — faixa de referência é o corte certo para rastreio? | homologação |
| 2 | Limiar do Anti-TPO (34 IU/mL) — confirmação | homologação |
| 3 | Cintilografia de tireoide — nenhum caso avaliado ainda (14/mês) | homologação |

---

## 10. Método de validação

A base ouro da V2 **não serve** para a V3: a definição de relevância mudou, e comparar régua nova
contra gabarito antigo não significa nada.

**A base ouro V3 nasce do lote processado pela V3** — roda-se a janela, estratifica-se a amostra e o
gabarito é construído a partir dela. Isso está registrado como decisão de método.

A amostra de homologação deve conter, deliberadamente:

- relevantes por **imagem** e relevantes por **sangue**, separados
- casos na faixa **1,0–1,5 cm** (ratifica a decisão 2)
- **Anti-TPO entre 34 e 100** (pendência 2)
- **T3 acima do limiar** (pendência 1 — a mais importante)
- **cintilografia de tireoide** (pendência 3)
- amostra **aleatória de negativos**, para medir o que escapa
