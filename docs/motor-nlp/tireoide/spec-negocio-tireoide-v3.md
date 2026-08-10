# SPEC de negócio — Linha de cuidado Tireoide **V3**

**Versão:** 3.2 · **Data:** 2026-08-10 · **Config:** `0.4.0-tirads` · **Motor:** `nlp_engine >= 0.8.1`
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
| **T3 livre** | **≥ 4,4** | pg/mL | 🚩 **FLAG** — não captura sozinho |
| **T3 total** | **≥ 2,0** | ng/mL | 🚩 **FLAG** — não captura sozinho |
| **TRAb** | **> 1,5** | UI/L | **doença de Graves** |
| **Anti-TPO** | **> 34** | IU/mL | 🚩 **FLAG** — não captura sozinho |

### 3.3 Origem de cada limiar

**TSH e T4 livre** — spec clínica original, confirmados pela Carol.

⚠️ O T4 livre foi passado em **pmol/L** (> 23) e o lake registra em **ng/dL**. Convertido
(`ng/dL × 12,87 = pmol/L`), 23 pmol/L = **1,79 ng/dL** — as duas fontes concordam. Copiar `23`
direto nunca seria atingido (p95 do lake = 1,6) e **falharia em silêncio**.

**TRAb** — corroborado pelo dado: o limite superior da faixa de referência do próprio laboratório
tem mediana **1,76**.

**T3 livre e T3 total** — limiares definidos pela Carol em 2026-08-10: **≥ 4,4 pg/mL** e
**≥ 2,0 ng/mL**. Substituem a proposta anterior baseada na faixa de referência do laboratório
(6,3 e 1,81), que produzia assimetria inexplicável — 11 casos de T3 livre contra 164 de T3 total.
Com os cortes clínicos o resultado equilibra: **87 e 91 casos/mês**, coerente com serem o mesmo
fenômeno medido de duas formas.

### 🚩 Por que o T3 é FLAG e não critério de captação

> *"T3 elevado sozinho, **sem TSH suprimido**, não estabelece o diagnóstico de hipertireoidismo."*
> — Carol, 2026-08-10

Há elevação de T3 e T4 **totais** sem hipertireoidismo real: excesso de medicação, disalbuminemia
familiar, gravidez, uso de estrogênio (anticoncepcional, menopausa, pessoas trans) — condições que
aumentam as **proteínas ligadoras** sem doença tireoidiana.

⚠️ Por isso o T4 usado aqui é o **LIVRE**, que não sofre esse efeito. Já o **T3 total sofre** — mais
um motivo para ser flag.

No pipeline diagnóstico (referência americana), **o TSH é a porta de entrada**: T3 e T4 só entram
*depois* de TSH suprimido, para separar tireotoxicose franca de hipertireoidismo subclínico. O T3
é **diagnóstico complementar** — confirma T3-toxicose quando TSH baixo e T4 livre normal.

**Implementação:** `annotate_only` — o critério é avaliado e registrado no audit, mas **não promove
sozinho**. Volume: ~178 casos/mês marcados.

**Anti-TPO** — a spec original o classificou como **qualitativo** (reagente / não reagente). O dado
mostra o contrário: é **numérico com censura à esquerda** (2.263 de 3.962 vêm como `"Inferior a 0,2"`).
O limiar de 34 IU/mL marca **442 exames/mês**.

### 🚩 Por que o Anti-TPO é FLAG

> *"Anti-TPO isolado, sem TSH suprimido e sem TRAb, indica paciente para a linha de cuidado?"*
> **"Só acompanhada."** — Carol, 2026-08-10

O anticorpo marca autoimunidade **em geral**: é positivo tanto em **Graves** (hipertireoidismo,
alvo) quanto em **Hashimoto** (hipotireoidismo, **fora do alvo**) — e Hashimoto é muito mais
prevalente. Isolado, inflava o escopo em **442 encaminhamentos/mês** sem indicar a doença-alvo.

A própria spec de negócio já o descrevia como *"flag autoimune"*; promovê-lo foi erro de leitura
na implementação.

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

| fonte | exames/mês | **encaminhamentos** |
|---|---|---|
| imagem | 22.059 | ~4.200 (19–22%) |
| sangue — **promotores** (TSH · T4 livre · TRAb) | ~80.000 | **~1.676** |
| sangue — **flags** (Anti-TPO · T3) | ~8.400 | 0 (só marcam) |

A régua **promove o que é específico** e **marca o que é complementar**. Passar Anti-TPO e T3 a
flag removeu ~620 encaminhamentos/mês de baixa especificidade.
| cintilografia de tireoide | 14 | a medir | — |

⚠️ **Paciente ≠ laudo.** Cerca de 25% dos relevantes são exames repetidos do mesmo paciente. Se o
envio for por paciente, o volume cai proporcionalmente.

---

## 8. Fora do escopo do V3

**V3.2 — sangue positivo → buscar USG com doppler.** É lógica **entre** exames e pacientes; o motor
decide laudo a laudo. Exige desenho próprio.

### V3.3 — relação T3/T4 total (proposta clínica, viabilidade medida)

> *"Em doença de Graves e nódulos tóxicos — que são as doenças que estamos avaliando — a relação é
> **> 20:1**, porque há muito mais T3 que T4. Já nas **tireoidites destrutivas** (De Quervain,
> silenciosa, pós-parto, induzida por drogas ou por iodo) a relação é **< 20:1**. Essas não são
> doenças-alvo, mas são **diferenciais** de hipertireoidismo verdadeiro."*
> — Carol, 2026-08-10

**É o critério de maior potencial para reduzir falso-positivo**, porque separa doença-alvo de
diferencial. Medido em junho/2026:

| | |
|---|---|
| pares paciente-dia com T3 e T4 total | **1.324** (19% dos pacientes com algum dos dois) |
| relação mediana | **14,6** |
| **acima de 20:1** — perfil Graves / nódulo tóxico | **101** |
| abaixo de 20:1 — perfil tireoidite destrutiva | 1.223 |

Os números confirmam a descrição clínica: a maioria fica abaixo, e uma minoria consistente acima.

**Três obstáculos, e o terceiro é estrutural:**

1. **Unidade.** O lake traz T3 em **ng/mL** e a relação clássica exige **ng/dL** (fator 100). Sem a
   conversão a razão dá 0,15 em vez de 14,6, e **nenhum caso apareceria** — mesma armadilha do T4
   livre em pmol/L, e igualmente silenciosa.
2. **Um analito por exame.** T3 total e T4 total **nunca vêm no mesmo laudo**; são linhas distintas.
3. **O motor decide laudo a laudo** e não cruza exames do mesmo paciente. Mesmo que cruzasse, a
   camada quantitativa compara **medida contra limiar fixo** — não sabe comparar **uma medida com
   outra**.

**Conclusão:** exige capacidade nova — agregação por paciente, janela temporal entre exames,
normalização de unidade e comparação medida-a-medida. Mesma classe da V3.2. **Não cabe no V3.**

---

**Bethesda** (punção e biópsia) — deferido na V2 por ausência de ganho medido.

**Ecocardiograma e cateterismo** — pertencem a outra linha de cuidado.

---

## 9. O que ainda depende de decisão clínica

| # | pendência | com quem |
|---|---|---|
| 1 | ~~Limiar do T3~~ | ✅ **fechado** — ≥ 4,4 e ≥ 2,0, como FLAG |
| 2 | ~~Anti-TPO isolado indica?~~ | ✅ **fechado** — "só acompanhada"; vira FLAG |
| 2b | 🔴 **T4 livre elevado sem TSH suprimido** também não é hipertireoidismo pelo pipeline. Mantido promotor por ora — virá-lo flag agora perderia recall sem substituto (exige V3.3) | decisão futura |
| 3 | Cintilografia de tireoide — nenhum caso avaliado ainda (14/mês) | homologação |
| 4 | 🚩 **Visibilidade da flag T3 na saída** — hoje só existe no blob de audit, não nas colunas | decisão técnica |

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
