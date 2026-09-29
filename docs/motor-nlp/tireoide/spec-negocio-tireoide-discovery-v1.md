# Spec de negócio — linha de cuidado de Tireoide (origem: discovery)

**Status:** referência · **Registrado em:** 2026-07-30 · **Órgão declarado:** Tireoide
**Relacionados:** [`relatorio-final-tirads-v1-2026-07-12.md`](relatorio-final-tirads-v1-2026-07-12.md) (decisões fechadas do V1 implementado) · [`mapa-gaps-tirads-v0.md`](mapa-gaps-tirads-v0.md)

Este documento registra a **spec como saiu do discovery**, que até aqui só existia fora do repo. Ele é a
fonte de origem; o relatório final documenta o que de fato foi implementado, e os dois **divergem** —
a tabela de status abaixo torna essa divergência explícita.

---

## 1. Exames de imagem no escopo

- Ultrassonografia (doppler / doppler colorido) de tireoide
- Ultrassonografia (US) de pescoço e da tireoide (com doppler)
- Doppler / doppler colorido de tireoide
- Cintilografia de tireoide
- Punção (PAAF) de tireoide · PAAF tireoide · punção aspirativa com agulha fina de tireoide
- Biópsia de tireoide · biópsia por/com agulha fina de tireoide
- **Tomografia computadorizada de pescoço** (partes moles, laringe, tireoide e faringe) — *exame não específico*

## 2. Critérios por versão

| Versão | Critério | Aplica a |
|---|---|---|
| **V1** | TI-RADS 4 e 5 | todos os exames de imagem |
| **V2** | Nódulo / cisto acima de 1 cm | todos os exames de imagem |
| **V2** | Massa / linfonodo / tumor — **exclusão: linfonodo reacional** | todos os exames de imagem |
| **V2** | Bócio / bócio mergulhante | todos os exames de imagem |
| **V2** | Bethesda | punção e biópsia |
| **V3** | Hipertireoidismo / hipertiroidismo | cintilografia |
| **V3** | Hipertireoidismo e doença de Graves | exame de sangue |
| **V3.2** | Após sangue positivo, buscar USG com doppler com achado de hipertireoidismo | — |

### Exame de sangue (V3)

| Marcador | Limiar | Interpretação |
|---|---|---|
| T4 livre | > 1,8 ng/dL | confirma hipertireoidismo (pode estar normal em casos subclínicos) |
| TSH | < 0,4 mUI/L (frequentemente < 0,1 ou "indetectável") | confirma hipertireoidismo |
| Trab | > 1,5 UI/L (ou "positivo"/"reagente") | confirma doença de Graves |
| Tireoglobulina | trazer o valor | marcador secundário (elevada na doença nativa) |
| Antitireoglobulina | trazer o valor | marcador secundário de autoimunidade geral |

**Regras de uso:** hipertireoidismo pode vir de exame de sangue **ou** cintilografia.

> ⚠️ **REDEFINIDO em 2026-08-03 — substitui a regra de origem.** A spec original exigia janela de
> **até 12 meses** entre sangue e imagem e tratava o sangue como **complemento**, nunca enviado
> isoladamente. Isso caiu. Agora **um ou outro já serve**: um exame de sangue que bata o limiar
> captura o paciente por si só, sem precisar de exame de imagem pareado e sem verificação de
> janela temporal. Some com isso o join paciente×data, que era a parte mais cara da V3.

### Parâmetros da Carol (2026-08-03) — preliminares, a validar com o especialista

| Parâmetro | Subclínico | Manifesto |
|---|---|---|
| TSH | 0,1–0,4 mUI/L (leve) · < 0,1 (grave) | tipicamente < 0,1 mUI/L |
| T4 livre | normal, faixa média-alta (18–23 pmol/L) | acima do limite superior (> 23 pmol/L) |
| T3 total/livre | normal, tendendo à faixa alta | elevado; pode ser a única alteração (T3-toxicose) |
| TRAb | positivo / reagente / > 1,5 UI/L | → flag **doença de Graves** |
| Anti-TPO | positivo / reagente / > 34 IU/mL | → flag **autoimune** (Graves/Hashimoto) |

Observação clínica dela: relação **T3:T4 total > 20:1** sugere Graves ou nódulo tóxico; **< 20:1**
sugere tireoidite.

🔴 **Conversão obrigatória — não usar os números de T4 livre como vieram.** A Carol passou T4 livre
em **pmol/L**; o lake registra em **ng/dL**. Fator: `ng/dL × 12,87 = pmol/L`.

| limiar dela | equivale a |
|---|---|
| 18 pmol/L | **1,40 ng/dL** |
| 23 pmol/L | **1,79 ng/dL** |

Ou seja: o `> 23 pmol/L` dela é o **mesmo limiar** do `> 1,8 ng/dL` que já estava nesta spec — as
duas fontes concordam. O risco é copiar `18`/`23` direto para a config: como o T4 livre medido no
lake tem p95 = 1,6, o critério **nunca** seria atingido e a falha seria **silenciosa** (o motor não
acusa unidade incompatível quando a origem não declara unidade; ver nota de método).

### Realidade do dado no lake (medido em jun/2026, `tb_gold_mov_exame`)

| analito | exames/mês | com texto | mediana | p05 – p95 | unidade inferida |
|---|---|---|---|---|---|
| T4 livre | 37.334 | 33.340 | 1,24 | 0,90 – 1,60 | ng/dL |
| TSH | 17.946 | 12.203 | 1,84 | 0,64 – 5,08 | mUI/L |
| T3 livre | 4.130 | 3.814 | 3,21 | 2,44 – 4,06 | pg/mL |
| Anti-TPO | 4.419 | 4.007 | — | — | ⚠️ ver abaixo |
| TRAb | 1.765 | 1.658 | 0,26 | 0,25 – 1,40 | UI/L |

⚠️ **O resultado é um número nu.** 99,1% dos TSH e 80,8% dos T4 livre têm como "laudo" apenas o
valor — **sem unidade e sem faixa de referência** no texto. A unidade é convenção, não dado.

⚠️ **Anti-TPO não é numérico**: dos 4.419 do mês, apenas **95** parseiam como número puro. O
restante vem como texto (reagente/não reagente e variações). O limiar `> 34 IU/mL` é inaplicável na
maioria dos casos — esse marcador precisa de tratamento **qualitativo**, não de limiar.

---

## 3. Status de cada critério na implementação

| Critério | Versão na spec | Status |
|---|---|---|
| TI-RADS 4 e 5 | V1 | implementado |
| Nódulo / cisto ≥ 1 cm | V2 | implementado — **antecipado para o V1 efetivo** por decisão médica |
| Massa / linfonodo / tumor | V2 | implementado |
| Exclusão de linfonodo reacional | V2 | implementado via critério `linfonodo_suspeito` |
| Bócio / bócio mergulhante | V2 | implementado — **bócio difuso excluído** por decisão médica (tratamento clínico) |
| Bethesda | V2 | **deferido** — 0 evidência de ganho na base, adiciona risco |
| Hipertireoidismo (cintilografia) | V3 | não implementado |
| Hipertireoidismo / Graves (sangue) | V3 | não implementado |
| USG doppler após sangue positivo | V3.2 | não implementado |
| Paratireoide | — | fora de escopo (órgão = tireoide) |

⚠️ **O "V1" entregue não é o V1 da spec.** A spec define V1 como *apenas* TI-RADS 4 e 5; o que foi para
produção junta V1 + a maior parte do V2. O relatório final registra isso como decisão médica deliberada.

---

## 4. Decisões fechadas depois do discovery

Vieram da revisão médica durante a construção do V1 e **não constam da spec de origem**:

| Decisão | Definição |
|---|---|
| Nódulo / cisto | conta só se **≥ 1 cm** (critério do V2 adotado como V1 efetivo) |
| Linfonodo | **suspeito / real / indeterminado** conta; **reacional / habitual / pós-op / negado não** |
| Bócio | **nodular / específico** conta; **aumento difuso** não (dimensões/volume aumentado, tireoidopatia difusa, bócio difuso homogêneo) |
| Paratireoide | fora de escopo |
| Exame não-tireoide | fora de escopo — higiene de entrada do runner |

---

## 5. Decisões ABERTAS

Levantadas na homologação de 2026-07-30 (221 laudos, avaliados pelo médico cirurgião de cabeça e pescoço).
Detalhamento e casos: planilha `pauta-revisao-spec-tireoide-V2` (fora do repo — contém texto de laudo).

| # | Questão | Volume | Natureza |
|---|---|---|---|
| 1 | TI-RADS 4 sozinho deixa de promover, passando a exigir nódulo ≥ 1 cm? | 29 laudos, **100% consistentes** | ratificação — os dados já decidem |
| 2 | Na faixa **1,0–1,5 cm** o corte de 1 cm decide sozinho, ou há zona cinzenta? | 15 laudos, 11 Sim / 4 Não no mesmo estado | **ambiguidade real** — nenhum sinal textual separa |
| 3 | A linha de cuidado segue o **órgão** (tireoide) ou a **especialidade** (cabeça e pescoço)? | 17 laudos | decisão de produto |

### Sobre a questão 3

A tensão é **da própria spec**, não do avaliador: a lista de exames inclui *TC de pescoço (partes moles,
laringe, tireoide e faringe)*, marcada como "exame não específico", enquanto o órgão declarado é
**Tireoide**. O avaliador é cirurgião de cabeça e pescoço e classificou como relevantes achados de
parótida, tonsila, laringe, adenoide e mandíbula — coerente com a especialidade dele e com a lista de
exames, incoerente com o órgão declarado.

Se a resposta for *cabeça e pescoço*, isso **não é ajuste de régua**: exige achados novos (parótida,
laringe, tonsila, glândula salivar, mandíbula) e caracteriza outra linha de cuidado.

---

## 6. Nota de método

A avaliação de negócio **não é gabarito** — o gabarito é a base ouro (`dados/base-ouro-tirads-v2-*.csv`,
rótulos do time + revisão médica + spec). Contra ela o motor está em precisão 0,969 · recall 1,000 ·
MCC 0,980 (n=586, FN=0). A divergência com o avaliador desta rodada é sinal de **evolução de régua**,
não de defeito do motor.

Antes de aplicar qualquer decisão desta lista na config, a base ouro precisa ser **re-rotulada** conforme
a régua nova (`verdade_v3`). Validar a config nova contra o gabarito antigo não significa nada.
