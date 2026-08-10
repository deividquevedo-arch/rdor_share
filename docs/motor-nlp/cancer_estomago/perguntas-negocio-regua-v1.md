# Câncer de Estômago — 5 perguntas objetivas

**Base:** retorno de negócio da versão `0.1.8` — 36 laudos com veredito, 15 marcados relevantes.
**Resultado:** 8 acertos · 3 marcados pelo motor e recusados · **7 marcados por vocês e não pelo motor**.

> A amostra foi montada com **negativos difíceis** selecionados de propósito. As divergências estão
> concentradas ali por construção — não representam a taxa da base inteira.

---

## 1. Lesão pré-maligna entra no escopo?

**Observado:** os 7 casos divergentes não têm neoplasia, tumor, massa nem linfoma. Todos são
lesão pré-maligna ou vigilância:

| # | conclusão do laudo |
|---|---|
| 1 | pólipo gástrico · retração cicatricial na incisura |
| 2 | área elevada e enantemática em antro (biópsias) |
| 3 | lesão ulcerada em antro — Sakita H1 |
| 4 | pólipos gástricos — Paris 0-Is |
| 5 | gastrite atrófica · lesão polipoide séssil → mucosectomia |
| 6 | gastrite atrófica · cicatrizes — Sakita S2 |
| 7 | úlcera em cicatrização · pólipos gástricos |

A régua V1 excluiu esse grupo por decisão registrada.

**Impacto medido:** dos 3.189 laudos hoje classificados como não relevantes, **1.575 (49%)
mencionam esses termos**.

**Resposta:** ( ) Entra ( ) Não entra

---

## 2. Se entra, quais destes?

Não são clinicamente equivalentes.

( ) Displasia
( ) Metaplasia intestinal
( ) Gastrite atrófica
( ) Pólipo gástrico — Paris 0-Is
( ) Úlcera Sakita em cicatrização

---

## 3. O texto da INDICAÇÃO deve ser ignorado?

**Observado:** 1 dos 3 falso-positivos foi marcado porque a **indicação** diz *"seguimento de
linfoma MALT"*. O exame não descreve linfoma.

**Se sim:** ajuste de configuração, elimina uma classe inteira de erro.

**Resposta:** ( ) Ignorar indicação ( ) Considerar indicação

---

## 4. Tumor gástrico tratado, sem recidiva — continua fora?

**Observado:** o caso da pergunta 3 é exatamente esse — paciente já em seguimento.

**Resposta:** ( ) Continua fora ( ) Passa a entrar

---

## 5. Achado exclusivamente no esôfago conta?

**Observado:** 1 dos 3 falso-positivos foi marcado por *"3 lesões erosivas"* no esôfago, sem
achado gástrico. Cárdia e JGE já foram definidos como fora.

**Resposta:** ( ) Não conta ( ) Conta

---

## Observação sobre os 3 falso-positivos

Os três foram decididos **sem nenhum achado de regra** — vieram do modelo de linguagem na zona de
incerteza. As perguntas 3, 4 e 5 cobrem os três casos. Respondidas, o ajuste é de configuração.

---

## O lote de 348 laudos responde 1 e 2 com base estatística

| grupo | laudos | função |
|---|---|---|
| positivos do motor | 18 | mede acerto sobre o que ele marca |
| **negativos sorteados ao acaso** | **250** | mede o que escapa — primeira vez sem amostra dirigida |
| casos com termo pré-maligno | 80 | decide as perguntas 1 e 2 |

⚠️ Nos 250 sorteados, **não marcar "Sim" por precaução**. Havendo dúvida, registrar em
*Observações* — o valor desse grupo depende de refletir o critério real.
