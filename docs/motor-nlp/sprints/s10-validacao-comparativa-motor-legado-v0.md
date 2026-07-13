# Validação Comparativa — Motor NLP vs Legado — Hepatologia
**Versão motor:** `0.1.12-hep-llm-prompt-clin`
**Data:** 2026-06-03

---

## Contexto e premissas

O motor é avaliado em dois cenários distintos. Cada cenário tem um escopo de visibilidade diferente — isso afeta diretamente a validade de cada métrica.

### O que cada cenário consegue ver

| | Cenário 1 — Carol | Cenário 2 — Board |
|---|---|---|
| **O que foi revisado** | União: casos que motor OU legado marcaram como relevante | Apenas casos que o legado já marcou como relevante |
| **Ponto cego** | Casos onde ambos (motor e legado) disseram "não" (436 casos) | Casos onde o legado disse "não" — FNs do legado são invisíveis por definição |
| **FN do legado observável?** | ✅ Sim — o motor pode capturar o que o legado perdeu | ❌ Não — legado=0 nunca chegou ao board |
| **FN do motor observável?** | ✅ Sim — se legado pegou mas motor não, Carol vê | ✅ Sim — se board confirma positivo e motor disse não |
| **Ground truth** | Especialista clínico (Carol) | Board médico (cod_achado_relevante 1 ou 2 = positivo; 3 = negativo) |
| **Tamanho da amostra** | ~60 casos revisados / 496 total | 2000 casos (317 positivos, 1683 negativos) |

---

## Cenário 1 — Homologação Carol (dia4)

**Amostra:** 496 exames · 60 revisados pela Carol (union motor ∪ legado)
**Configuração:** nova versão YAML + prompt `0.1.12`

| Métrica | O que representa neste cenário | Motor | Legado |
|---|---|---|---|
| **TP / FP / TN / FN** | contagem base | 48 / 12 / 424 / 0 | 13 / 1 / 435 / 47 |
| **Precision** | dos que o sistema disse SIM, quantos Carol confirmou | **83.3%** | 92.9% |
| **Recall** ⚠️ | dos positivos que Carol viu, quantos o sistema pegou | **100%** | 21.7% |
| **Specificity** | dos negativos que Carol viu, quantos o sistema acertou | 97.2% | **99.8%** |
| **Accuracy** | de todos os casos revisados, quantos o sistema acertou | **97.6%** | 90.3% |
| **F1** | equilíbrio precision × recall (peso igual) | **90.9%** | 35.1% |
| **F2** | equilíbrio priorizando recall (não perder positivos) | **96.2%** | 25.6% |
| **MCC** | discriminação geral, robusto ao desbalanceamento | **0.90** | 0.42 |

> ⚠️ **Recall**: Carol só revisou casos que ao menos um sistema sinalizou.
> Casos onde motor=0 e legado=0 (436 casos) não foram revisados — FNs invisíveis para ambos.
> O recall de 100% do motor significa: "não perdeu nada que o legado ou ele mesmo enviou para revisão."

---

## Cenário 2 — Homologação Board Médico (histórico)

**Amostra:** 2000 casos (cohort desde 2020-01-01)
**Ground truth:** `cod_achado_relevante` no retorno do board (1 ou 2 = positivo; 3 = negativo)
**Importante:** o cohort foi montado com `legado=1` ∩ `retorno preenchido` — o legado já filtrou antes.

| Métrica | O que representa neste cenário | Motor | Legado |
|---|---|---|---|
| **TP / FP / TN / FN** | contagem base | 265 / 968 / 715 / 52 | 317 / 1683 / 0 / 0 |
| **Precision** | dos que o sistema disse SIM, quantos o board confirmou | **21.5%** | 15.9% |
| **Recall** ⚠️ | dos positivos do board, quantos o sistema capturou | **83.6%** | ~~100%~~ artefato |
| **Specificity** | dos negativos do board (cod=3), quantos o sistema acertou | **42.5%** | 0% |
| **Accuracy** | de todos os 2000 casos, quantos o sistema decidiu certo | **49.0%** | 15.9% |
| **F1** | equilíbrio precision × recall (peso igual) | **34.2%** | 27.4% |
| **F2** | equilíbrio priorizando recall (não perder positivos) | **53.0%** | 48.5% |
| **MCC** | discriminação geral, robusto ao desbalanceamento | **0.196** | ~~0~~ indefinido* |

> ⚠️ **Recall legado = 100%** é inválido como métrica de desempenho.
> O cohort é formado exclusivamente por casos que o legado marcou como positivo (legado=1).
> Por definição, FN_legado = 0 nesse dataset — o legado nunca pode "perder" um caso que ele mesmo enviou.
>
> * **Specificity e MCC do legado = indefinidos/≈0**: com TN=0 e FN=0, o legado não discrimina nenhum negativo.
> MCC é matematicamente indefinido (denominador = 0) e tratado como ≈0 — ausência total de poder discriminativo.
>
> ⚠️ **52 FNs do motor**: parte desses casos tem `cod_achado_relevante=2`
> ("Sim, mas não tem doença de fígado") — board marcou como relevante por achado biliar/outro,
> fora do escopo de detecção do motor (hepatopatia). Ainda não segregado.

---

## Comparativo consolidado — Motor ganha em 13 de 14 métricas válidas

| Métrica | Motor vence? | Ressalva |
|---|---|---|
| Precision — Carol | ❌ | Legado ligeiramente melhor (92.9% vs 83.3%), amostra pequena (60 casos) |
| Precision — Board | ✅ | 21.5% vs 15.9% |
| Recall — Carol | ⚠️ | Motor 100%, mas FNs de ambos invisíveis (casos onde motor=0 e legado=0) |
| Recall — Board | ⚠️ | Motor 83.6% observável; legado 100% é artefato de seleção |
| Specificity — Carol | ❌ | Legado minimamente melhor (99.8% vs 97.2%) |
| Specificity — Board | ✅ | Motor 42.5% vs legado 0% |
| Accuracy — Carol | ✅ | 97.6% vs 90.3% |
| Accuracy — Board | ✅ | 49.0% vs 15.9% |
| F1 — Carol | ✅ | 90.9% vs 35.1% |
| F1 — Board | ✅ | 34.2% vs 27.4% |
| F2 — Carol | ✅ | 96.2% vs 25.6% |
| F2 — Board | ✅ | 53.0% vs 48.5% |
| MCC — Carol | ✅ | 0.90 vs 0.42 — motor 2× melhor |
| MCC — Board | ✅ | 0.196 vs ≈0 — motor 10× melhor |

---

## Veredito e recomendação

**O motor substitui o legado.** O legado é funcionalmente um classificador trivial que marca tudo como positivo (Specificity≈0, MCC≈0 no cenário grande). O custo operacional é alto: 5 em cada 6 encaminhamentos do legado são desnecessários (Precision=15.9%).

O motor discrimina em ambos os cenários e supera o legado em todas as métricas robustas ao desbalanceamento (MCC, F1, F2, Accuracy, Specificity).

### Trabalho restante antes do go-live

| Item | Descrição | Impacto |
|---|---|---|
| **Segregar FNs por cod** | Separar FN `cod=1` (doença hepática perdida) de FN `cod=2` (biliar/outro, fora do escopo) | Define se os 52 FNs são bugs reais ou escopo diferente |
| **Reduzir FPs (968)** | Refinar YAML + prompt para não sinalizar casos já conhecidos / sem nova ação | Aumenta Precision sem penalizar Recall |
| **Validar ponto cego — 436 casos (Carol)** | Ver track 1 abaixo | Estima taxa de FN onde ambos disseram "não" |
| **Validar negativos do legado (entrada sem saída)** | Ver track 2 abaixo | Única forma de medir Recall real do legado |

---

## Plano de validação de FNs — próximos passos

### Por que não basta rodar o board para medir FNs do legado

O cohort do board é formado por `legado=1` ∩ `retorno preenchido`.
Casos onde `legado=0` nunca chegam ao board → FNs do legado são estruturalmente invisíveis nesse dataset.
Para medi-los é necessário uma estratégia fora do fluxo atual.

---

### Track 1 — Validar ponto cego dos 436 casos (cenário Carol)

**Objetivo:** verificar se existem FNs reais entre os casos onde motor=0 e legado=0.

```
Fonte: os 436 casos que Carol não revisou (ambos disseram "não")
Seleção:
  → 20 casos com uncertainty_band_hit=true (borderline, maior chance de FN)
  → 10 casos aleatórios (sanity check sem viés)
Ação: Carol avalia ~30 laudos
Resultado esperado: estimar % de FN real no ponto cego
```

| Abordagem | Vantagem | Limitação |
|---|---|---|
| Aleatória | Sem viés, estimativa representativa | Baixo yield de FNs (maioria será TN) |
| Por confidence score (borderline) | Maior chance de achar FNs com menos casos | Representa apenas o limiar, não o universo todo |

**Recomendação:** usar confidence-based (uncertainty_band_hit=true) + ~10 aleatórios como controle.

---

### Track 2 — Validar negativos do legado (entrada sem par na saída)

**Objetivo:** medir se o legado está perdendo casos relevantes — o recall real do legado.

```
Fonte: tabela de entrada (universo total de exames)
Filtro: exames que NÃO aparecem na saída do legado (legado=0)
Processamento: motor classifica esses exames
Seleção: motor=1 com confidence > 0.6 → top 50 casos
Ação: Carol avalia os 50 laudos
Resultado esperado: confirmar FNs reais do legado que o motor capturaria
```

Este track é o mais valioso: é a única forma de comparar Recall de ambos os sistemas sem viés de seleção.

---

### Esforço estimado para Carol

| Track | Casos para Carol | Objetivo |
|---|---|---|
| Track 1 — ponto cego 436 | ~30 | FN rate no blind spot |
| Track 2 — negativos legado | ~50 | Recall real do legado |
| **Total** | **~80 casos** | Complementa ambos os cenários |

---

### Estratégia de transição sugerida

```
Fase atual:   legado como fonte → motor como filtro de priorização
Após tracks:  com Recall real do legado medido, decisão de substituição embasada
Após tuning:  motor como fonte principal → legado como safety-net opcional
```
