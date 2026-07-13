# Relatório Final de Homologação — Motor NLP vs Legado — Hepatologia

**Versão do motor:** `0.1.12-hep-llm-prompt-clin`
**Data:** 2026-06-08
**Status:** Homologação concluída · pendências de validação de FN mapeadas

---

## 1. Resumo executivo (1 minuto de leitura)

> O **motor NLP** foi comparado ao sistema **legado** em dois cenários independentes de homologação. Em ambos, o motor demonstrou **capacidade real de discriminar** casos relevantes; o legado comporta-se como um classificador que **marca quase tudo como positivo**, gerando alto volume de encaminhamentos desnecessários.
>
> **Conclusão:** o motor está apto a substituir o legado, condicionado a duas validações de fechamento (FN do ponto cego e FN real do legado) e a um ciclo de redução de falsos positivos.

| Pergunta de negócio | Resposta |
|---|---|
| O motor acerta mais que o legado? | **Sim** — vence em 12 das 14 métricas comparáveis |
| O motor descarta casos irrelevantes? | **Sim** — legado tem especificidade ≈0; motor discrimina negativos |
| O motor perde casos relevantes? | **Risco baixo, a confirmar** — 52 FNs no board, parte fora de escopo |
| Pode ir para produção já? | **Sim, como filtro de priorização**; fonte principal após validações |

---

## 2. Glossário rápido (entendimento facilitado)

| Termo | O que significa em linguagem simples |
|---|---|
| **TP** (verdadeiro positivo) | Disse SIM e era SIM — acertou o relevante |
| **FP** (falso positivo) | Disse SIM mas era NÃO — alarme falso |
| **TN** (verdadeiro negativo) | Disse NÃO e era NÃO — acertou o irrelevante |
| **FN** (falso negativo) | Disse NÃO mas era SIM — perdeu um caso relevante |
| **Precision** | Dos que disse SIM, quantos eram realmente SIM (qualidade do alarme) |
| **Recall** | Dos SIM reais, quantos o sistema pegou (não deixar passar) |
| **Specificity** | Dos NÃO reais, quantos o sistema descartou corretamente |
| **Accuracy** | Do total, quantas decisões foram corretas |
| **F1** | Equilíbrio entre precision e recall (peso igual) |
| **F2** | Equilíbrio favorecendo recall (prioriza não perder casos) |
| **MCC** | Nota geral de discriminação (−1 a +1); a mais honesta em dados desbalanceados. 0 = chute |

---

## 3. Os dois cenários — o que cada um enxerga

A validade de cada métrica depende de **o que o cenário consegue observar**. Esta é a chave para ler o relatório corretamente.

| | Cenário 1 — Carol | Cenário 2 — Board |
|---|---|---|
| **Ground truth** | Especialista clínico (Carol) | Board médico (`cod_achado_relevante`) |
| **Regra de positivo** | Veredicto da Carol | cod 1 ou 2 = positivo · cod 3 = negativo |
| **Amostra** | ~60 revisados / 496 | 2000 (317 pos / 1683 neg) |
| **O que foi revisado** | União: motor **OU** legado sinalizaram | Apenas o que o legado marcou (legado=1) |
| **Ponto cego** | Casos onde ambos disseram NÃO (436) | Casos onde legado disse NÃO (nunca chegam ao board) |
| **FN do legado é visível?** | ✅ Sim | ❌ Não (estrutural) |
| **Força do cenário** | Menos enviesado (2 filtros) | Volume grande, ground truth clínico formal |
| **Fraqueza do cenário** | Amostra pequena | Enviesado: só vê positivos do legado |

---

## 4. Cenário 1 — Homologação Carol (dia4)

**Base:** 48/12/424/0 (motor TP/FP/TN/FN) · 13/1/435/47 (legado)

| Métrica | Motor | Legado | Quem vence | Leitura simples |
|---|---|---|---|---|
| **Precision** | 83.3% | **92.9%** | Legado | Ambos alta qualidade de alarme; legado um pouco melhor na amostra pequena |
| **Recall** ⚠️ | **100%** | 21.7% | Motor | Motor pegou tudo que foi revisado; legado perdeu ~78% |
| **Specificity** | 97.2% | **99.8%** | Legado | Empate prático — ambos descartam bem negativos |
| **Accuracy** | **97.6%** | 90.3% | Motor | Motor acerta mais no total |
| **F1** | **90.9%** | 35.1% | Motor | Motor muito mais equilibrado |
| **F2** | **96.2%** | 25.6% | Motor | Motor muito superior em não perder casos |
| **MCC** | **0.90** | 0.42 | Motor | Motor 2× melhor em discriminação |

> ⚠️ **Recall 100% do motor não é absoluto:** Carol só revisou casos que ao menos um sistema sinalizou. Os 436 casos onde ambos disseram NÃO não foram olhados — possíveis FNs invisíveis (ver Track 1).

---

## 5. Cenário 2 — Homologação Board Médico

**Base:** 265/968/715/52 (motor) · 317/1683/0/0 (legado) · total 2000

| Métrica | Motor | Legado | Quem vence | Leitura simples |
|---|---|---|---|---|
| **Precision** | **21.5%** | 15.9% | Motor | Ambos baixos; motor gera menos alarme falso |
| **Recall** ⚠️ | **83.6%** | ~~100%~~ | Motor | 100% do legado é artefato (ver nota) |
| **Specificity** | **42.5%** | 0% | Motor | Legado não descarta nenhum negativo |
| **Accuracy** | **49.0%** | 15.9% | Motor | Motor acerta 3× mais no total |
| **F1** | **34.2%** | 27.4% | Motor | Motor mais equilibrado |
| **F2** | **53.0%** | 48.5% | Motor | Motor superior mesmo priorizando recall |
| **MCC** | **0.196** | ≈0* | Motor | Legado ≈ chute; motor discrimina |

> ⚠️ **Recall legado = 100% é inválido.** O cohort só contém casos que o legado marcou como positivo (legado=1). Logo FN_legado = 0 por definição — ele não pode "perder" o que ele mesmo enviou.
>
> *️ **Specificity e MCC do legado ≈ 0 / indefinidos:** com TN=0 e FN=0, o MCC é matematicamente indefinido (denominador zero), tratado como ≈0 = ausência de poder discriminativo.
>
> ⚠️ **52 FNs do motor:** parte tem `cod=2` ("Sim, mas não tem doença de fígado") — relevância por achado biliar/outro, **fora do escopo** do motor (hepatopatia). A segregar antes de tratar como erro real.

---

## 6. Placar consolidado

| Métrica | Carol (vence) | Board (vence) |
|---|---|---|
| Precision | Legado (margem pequena) | **Motor** |
| Recall | **Motor** ⚠️ | **Motor** ⚠️ |
| Specificity | Legado (empate prático) | **Motor** |
| Accuracy | **Motor** | **Motor** |
| F1 | **Motor** | **Motor** |
| F2 | **Motor** | **Motor** |
| MCC | **Motor** (0.90 vs 0.42) | **Motor** (0.196 vs ≈0) |

**Resultado:** Motor vence em 12 de 14 métricas comparáveis. As 2 exceções (Precision e Specificity no Carol) são margens pequenas em amostra de 60 casos.

---

## 7. Interpretação de negócio

```
LEGADO  → grita SIM para quase tudo.
          Não perde positivos, mas afoga a equipe em alarmes falsos.
          No board: 5 de cada 6 encaminhamentos eram desnecessários.

MOTOR   → é seletivo e discrimina.
          Reduz drasticamente encaminhamentos inúteis.
          Custo: alguns FNs a confirmar (parte fora de escopo).
```

**Saldo do motor sobre o legado (cohort de 2000):**
- ✅ Evita 715 encaminhamentos desnecessários (TN que o legado erraria)
- ❌ Perde 52 casos (a investigar — parte é `cod=2`, fora de escopo)
- **Balanço líquido: +663 decisões melhores**

---

## 7.1 Composição dos 968 FPs do motor (análise por confiança × achado hepático)

Segmentação dos 968 FPs usando proxy heurístico: presença de termo de hepatopatia no laudo (`RLIKE`) × faixa de `confidence_score`.

| Faixa confiança | Com achado hepático | Sem achado | Total | % |
|---|---|---|---|---|
| ≥ 0.90 | 23 | 19 | 42 | 4.3% |
| 0.70–0.90 | 224 | 237 | 461 | 47.6% |
| 0.50–0.70 | 12 | 15 | 27 | 2.8% |
| < 0.50 | 375 | 63 | 438 | 45.2% |
| **Total** | **634 (65.5%)** | **334 (34.5%)** | **968** | |

### Leitura

**1. 65.5% dos FPs (634) têm achado hepático real no laudo → motor provavelmente certo.**
Exemplos de alta confiança: "sugerindo hepatopatia crônica" (0.99), "hipertensão portal" (0.91), "esteatose hepática" (0.92). Nesses casos o board marcou `cod=3` mesmo com hepatopatia descrita → **critério de programa do board**, não erro do motor.
> ⚠️ O `RLIKE` não trata negação ("Não há esteatose" conta como achado). Logo **634 é um teto** — o número real de "motor certo" é um pouco menor. Confirmar com spot-check da Carol (~30 casos da Query 3).

**2. 45% dos FPs (438) estão abaixo de 0.50 de confiança → alvo de threshold.**
Distribuição bimodal: 461 FPs em 0.70–0.90 (alta confiança) e 438 FPs em <0.50 (baixa confiança). Um piso de confiança para aceitar positivo eliminaria boa parte dos 438 — **a confirmar via threshold sweep** (medir quantos TPs cairiam junto).

### Implicação para o plano

| Tipo de FP | Volume aprox. | Tratamento |
|---|---|---|
| Tipo B — motor certo, critério de programa do board | ~634 (teto) | **Não corrigível só com NLP** — depende de regra de negócio externa ao laudo |
| Tipo A — erro real sem achado hepático | ~334 (piso) | **Refino de prompt/YAML** (ganho limitado — ver 7.2) |

---

## 7.2 Threshold sweep — o piso de confiança NÃO é alavanca (refutado)

Testamos aplicar um piso de `confidence_score` sobre os positivos do motor. Resultado:

| thr | TP | FP | TN | FN | precision | recall | accuracy | f1 |
|---|---|---|---|---|---|---|---|---|
| 0.00 | 265 | 968 | 715 | 52 | 0.215 | 0.836 | 0.490 | 0.342 |
| 0.45 | 187 | 658 | 1025 | 130 | 0.221 | 0.590 | 0.606 | 0.322 |
| 0.50 | 150 | 530 | 1153 | 167 | 0.221 | 0.473 | 0.652 | 0.301 |
| 0.80 | 113 | 381 | 1302 | 204 | 0.229 | 0.356 | 0.708 | 0.279 |
| 0.90 | 33 | 42 | 1641 | 284 | 0.440 | 0.104 | 0.837 | 0.168 |

### Conclusão: confiança NÃO prediz concordância com o board

- **Precisão travada em ~0.21** em quase toda a faixa. Subir o piso corta FPs **e** TPs na mesma proporção.
- Faixa removida de 0.00→0.50: −438 FPs mas −115 TPs → precisão da faixa = **0.208 = idêntica à global**. Prova que a baixa confiança **não está enriquecida em FPs**.
- Mesmo em confiança ≥0.90, o board rejeita **42 de 75** (56%) — laudos com hepatopatia/cirrose explícita.
- F1 **piora** em qualquer piso (máximo está em thr=0.00). O threshold só melhora `accuracy` porque converte FP→TN num dataset dominado por negativos do board — métrica enganosa aqui.

> **Achado central da homologação:** o board não decide pelo conteúdo do laudo, mas por **critério de programa** (caso conhecido, sem nova conduta, elegibilidade). Nenhum ajuste de NLP — threshold, prompt ou YAML — alinha o motor a um critério que não está no texto. A qualidade de leitura clínica do motor deve ser medida pelo **cenário Carol**, não pela concordância com o board.

---

## 7.3 Composição dos 52 FNs do motor — apenas ~5 são misses reais

Segregação dos 52 FNs (`fl_motor=0` ∧ `fl_board=1`) por `cod_achado_relevante`:

| `cod` | Significado | FNs | % | Veredicto |
|---|---|---|---|---|
| 1 - Tem Doença Fígado | Doença hepática real | 9 | 17.3% | A investigar |
| 2 - Sim, Mas Não Tem Doença Fígado | Relevante por outro motivo (biliar/pâncreas) | 43 | 82.7% | ✅ Motor correto (fora de escopo) |

### Os 43 `cod=2` não são erros (82.7% dos FNs)

Padrão unânime nos laudos: **fígado descrito como normal**, relevância vinda da vesícula (colelitíase, colecistite). O motor disse "sem doença hepática" e estava certo — o achado é biliar, **fora do escopo** (hepatopatia). Penalização indevida do motor.

### Os 9 `cod=1`: só ~5 são misses clínicos genuínos

**Grupo A — 5 misses reais** (motor deveria ter capturado; todos confiança 0.32–0.34, `hybrid_calibrated` que não acionou LLM):

| Achado no laudo | Tipo de lesão |
|---|---|
| "acentuada infiltração gordurosa (esteatose)" | Esteatose |
| "volumosa lesão... hemangioma... outros nódulos" | Hemangiomas / lesões focais |
| "adenoma esteatótico... lesão hepatocelular benigna" | Adenoma |
| "áreas de edema periportal" | Edema periportal |
| "múltiplos cistos hepáticos... distúrbios perfusionais" | Cistos hepáticos |

**Grupo B — 4 casos defensáveis:** elastografias com rigidez normal (F0-F1, "descarta DHCAc") e fígado normal no exame, ou provável erro de `cod` do board (fígado normal, doença biliar marcada como cod=1).

### Implicação

- **Recall real para doença hepática é muito superior a 83.6%.** Dos 52 FNs, só **~5 são misses clínicos** — o restante é fora de escopo (43) ou defensável (4). São **~5 misses em 2000 casos**.
- **Padrão acionável:** os 5 misses são lesões focais benignas (hemangioma, adenoma, cisto), esteatose e edema periportal, todos com baixa confiança e **sem acionamento do LLM**. A camada rule/embedding não os roteou para revisão.

> **Ação de refino (alto valor, baixo risco):** expandir keywords/padrões no YAML para `esteatose`/`infiltração gordurosa`, `hemangioma`, `adenoma`, `hiperplasia nodular focal`, `cistos hepáticos`, `edema periportal` → acionar o LLM nesses borderline. Corrige os misses reais sem mexer no comportamento geral.

---

## 8. Pendências antes do go-live como fonte principal

| # | Ação | Objetivo | Esforço |
|---|---|---|---|
| 1 | **Alinhar critério com o board** ⭐ | Esclarecer o que significa `cod=3` com hepatopatia explícita no laudo — definir se é elegibilidade de programa | Reunião board (alto valor) |
| 2 | ✅ **Segregar 52 FNs por cod** (feito — ver 7.3) | 43 fora de escopo, 4 defensáveis, ~5 misses reais | Concluído |
| 3 | **Expandir keywords dos 5 misses** | Rotear esteatose/hemangioma/adenoma/cisto/edema periportal ao LLM | YAML (baixo) |
| 4 | **Spot-check Tipo B (Carol)** | Confirmar que os ~634 FPs com achado hepático são critério de programa, não erro | Carol: ~30 casos |
| 5 | **Track 1 — ponto cego (436)** | Estimar FN onde ambos disseram NÃO | Carol: ~30 casos |
| 6 | **Track 2 — negativos do legado** | Medir recall REAL do legado (FNs invisíveis) | Carol: ~50 casos |

> ❌ **Threshold sweep descartado como alavanca** (ver 7.2): precisão flat prova que confiança não prediz concordância com o board.

### Detalhe dos tracks de validação de FN

**Track 1 — ponto cego dos 436 (cenário Carol)**
```
Fonte: 436 casos onde motor=0 E legado=0 (não revisados)
Seleção: 20 com uncertainty_band_hit=true (borderline) + 10 aleatórios
Carol avalia ~30 → estima taxa de FN no ponto cego
```

**Track 2 — negativos do legado (o mais valioso)**
```
Fonte: entrada (universo) filtrando exames SEM par na saída (legado=0)
Motor classifica → seleciona motor=1 com confidence > 0.6 → top 50
Carol avalia 50 → mede FN real do legado (única forma sem viés de seleção)
```

**Total para Carol: ~80 casos.**

---

## 9. Recomendação final

> **Adotar o motor.** A evidência é consistente nos dois cenários e na métrica mais robusta (MCC), o motor supera o legado de forma inequívoca. O legado não possui poder discriminativo real (MCC ≈ 0 em volume).

**Transição sugerida:**

```
Agora:        Motor como filtro de priorização sobre a saída do legado
Após tracks:  Recall real do legado medido → decisão de substituição embasada
Após tuning:  Motor como fonte principal · legado como safety-net opcional
```

---

*Documento de fechamento. Notas técnicas detalhadas e racional metodológico em `s10-validacao-comparativa-motor-legado-v0.md`.*
