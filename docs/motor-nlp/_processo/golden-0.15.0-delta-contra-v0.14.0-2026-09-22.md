# Golden da `0.15.0` contra a `v0.14.0` — o delta, enumerado

> **7 laudos mudam de decisão. Todos multilesão. Todos `1 → 0`. ZERO acrescidos.**
> E os quatro perfis que não tocam a camada ordinal ficam **byte a byte idênticos**.

- **Data:** 2026-09-22 · **Card:** `306034` — *[NLP Engine] TI-RADS entrega a medida do nódulo errado*
- **Método:** duas árvores de trabalho, venv por árvore, **o mesmo script** dos dois lados.
  Baseline: tag `v0.14.0` (`6c373b5`).

---

## 1. 🔴 A pré-condição, e ela quase reprovou o próprio golden

**Antes desta versão, o golden NÃO enxergava a camada quantitativa.** O corpus era de uma lesão por
laudo, e nenhum dos oito perfis declarava `quantitative_criteria`. Resultado: nenhuma chave de
`quantitative`, nenhum `measure_lesion_*`.

**Comparar as duas árvores daria IDÊNTICO — e seria medição vazia:** o caminho não rodaria em
nenhuma das duas. Foi conferido **antes** de comparar; sem isso, a não-regressão teria sido
declarada em cima de um ponto cego.

| chave | antes | depois |
|---|---|---|
| `measure_lesion_linked` | **0** | **18** |
| `measure_lesion_window` | 0 | 13 |
| `require_measure_sem_vinculo` | 0 | 5 |

---

## 2. O delta

| perfil | laudos | **decisão muda** | blob muda |
|---|---|---|---|
| `1_regra_pura` · `2_regra_sem_negacao` · `3_hibrido_embeddings` · `6_com_juiz_llm` | 375 | **0** | **0** |
| `4_ordinal` · `5_ordinal_hibrido` · `7_completo` · `8_juiz_banda_total` | 375 | **0** | 310 |
| **`9_quantitativo`** | 375 | **4** | 310 |
| **`10_quantitativo_janela_linha`** | 375 | **3** | 310 |

🟢 **Os quatro perfis sem camada ordinal ficam byte a byte idênticos** — é a não-regressão onde a
mudança não devia chegar.

### Os 310 blobs são puramente aditivos — conferido, não suposto

Nos perfis com ordinal e **sem** quantitativo, a **única** chave que difere é `ordinal_mentions`:

```
antes : category, confidence, matched_text, negated, source, system
depois: category, confidence, end, matched_text, negated, source, start, system
```

**Nenhum valor preexistente mudou.** É o campo novo da `0.15.0`, e nada além.

---

## 3. Os 7 que mudam são exatamente os certos

| laudo | por que mudou |
|---|---|
| `multi-menor-e-a-categorizada` | o TR4 era aprovado pela medida de **outra lesão** (2,1 cm); a dele mede 0,4 cm |
| `multi-categoria-antes-da-medida` | mesmo caso, com a categoria escrita antes da medida |
| `multi-cinco-lesoes` | mesmo caso, num laudo com cinco lesões |
| `multi-sentenca-vizinha` | 🔴 **muda só no perfil 9** — ver abaixo |

**Todos `fl_relevante: 1 → 0`. Nenhum `0 → 1`.** É o `CA6`: a mudança só remove.

### 🟢 Os dois perfis DISCRIMINAM a janela — e é isso que prova o piso

O laudo `multi-sentenca-vizinha` tem a categoria numa linha e a medida (`0,9 cm`) na seguinte:

| perfil | janela | o que acontece |
|---|---|---|
| **9** | `linha+sentenca` | o fallback **acha** a medida na sentença vizinha → **liga** → 0,9 < 1,0 → **rebaixa** |
| **10** | `linha` | **não liga** → `vinculo = ausente` → **não afirma e NÃO rebaixa** → segue entregue |

**As duas pernas do desenho estão provadas no mesmo run:** a correção quando há vínculo, e o piso
quando não há. E o par de perfis existe justamente para que uma mudança no default da janela
reprove a comparação.

---

## 4. O controle

**Laudo unilesão: zero mudanças.** Os 365 laudos do corpus original passam idênticos também nos
perfis quantitativos. É o `CA5` — *a mudança não toca o caso que já funcionava*.

---

## 5. ⚠️ O que este golden NÃO é

- **Corpus sintético.** O limiar do perfil é `>= 1,0 cm` e as medidas foram escolhidas para cercar
  a fronteira. **Não é estimativa de produção.**
- **O cliente HTTP real fica fora**, por construção — o LLM é estubado de forma determinística.
- **A medição em coorte real segue pendente:** `CA4` e `CA5` pedem laudos multilesão de verdade.
  A população existe e está dimensionada: **2.729 multilesão** e **2.042 unilesão** de controle,
  em 15 dias de TI-RADS.

---

## 6. Três coisas corrigiram o corpus, e as três eram a lib estando certa

⚠️ Vale registrar, porque a suspeita natural era do código:

1. **O `\n` foi comido** na cadeia heredoc → Python → arquivo, e virou quebra de linha real.
2. **A âncora exige o termo que a régua declara** (`nodulo solido`); com `Nodulo` apenas, o
   critério nem era avaliado.
3. 🔴 **O gate de órgão.** Com `Tireoide.` só na primeira linha, os nódulos das linhas seguintes
   caíam em `skipped_no_anchor` — o órgão precisa estar **perto** do achado.

**Nas três, o corpus é que precisava respeitar a lib.**
