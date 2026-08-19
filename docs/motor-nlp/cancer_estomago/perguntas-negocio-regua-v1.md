# Câncer de Estômago — 5 perguntas objetivas

> ## ✅ RESPONDIDO PELO NEGÓCIO — Targa, 2026-08-18
>
> > *"A palavras-chaves da V1 passarão a contemplar também lesões pré-malignas (ex.: pólipos,
> > gastrite atrófica, metaplasias, displasia e úlceras)?"*
> > **"Vamos considerar somente as úlceras, pois podem ser lesões neoplásicas. Restante das
> > palavras vamos continuar desconsiderando."**
>
> **As 5 perguntas ficam fechadas por esta resposta:**
>
> | # | fecho |
> |---|---|
> | 1 · pré-maligna entra? | **sim, e só úlcera** |
> | 2 · quais delas? | **só úlcera**; pólipo, gastrite atrófica, metaplasia e displasia seguem fora |
> | 3 · ignorar INDICAÇÃO? | **sim** — é a pergunta do exame, não o resultado; já implementado |
> | 4 · tumor tratado sem recidiva? | **segue fora** — "restante vamos continuar desconsiderando" |
> | 5 · achado só no esôfago? | **não conta** — a linha de cuidado é estômago; consequência do escopo |
>
> ### O que isso mudou na régua (`0.3.0-cancer_estomago`)
>
> Achado **`ulcera`** novo, separado do `ulcera_suspeita` — este último exige sinal morfológico de
> malignidade e excluía "úlcera em cicatrização", que era um dos falso-negativos confirmados.
> Separados, a fila distingue *"Úlcera suspeita"* de *"Úlcera"* na priorização.
>
> Exclusão explícita de **duodeno, esôfago, palato, boca e língua** — decorre do fecho da 5. Não é
> redundante com o gate de órgão: com `organ.scope: block` e laudo numa linha só, o documento
> inteiro é um bloco, e uma úlcera duodenal passaria.
>
> ### Reclassificação dos 7 falso-negativos
>
> Só **2 continuam FN** (úlcera gástrica de verdade). Dos outros 5: três são pólipo/enantemática
> (fora do escopo), um tem a úlcera **negada** no texto e outro é úlcera em **palato duro**.
> **Recall 0,533 → 1,000** no lote, contra o gabarito revisado.
>
> ⚠️ **Precisão não medida** — a avaliação foi offline, sem o juiz, que é a camada que filtra.
> Úlcera gástrica é achado comum: exigir run antes de comprometer data.

---

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
