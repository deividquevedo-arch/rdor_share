# Relatório de Homologação — Motor NLP vs Legado — BI-RADS (mama)

**Motor:** `nlp-engine 0.1.1` (lib no baseline) · **config** `0.1.0-birads-rads-v2` (fixes só no pattern)
**Data:** 2026-06-23 · **Estágio:** Bancada A/B local (laudos reais do lake) · revisão clínica formal pendente

---

## 1. Resumo executivo

> Comparação restrita à **extração da categoria BI-RADS** (a flag de relevância não entra — o legado não modela regra clínica, então comparar relevância mede critério de negócio, não leitura). Contra o mesmo gabarito (conclusão do laudo), **o motor supera o legado em todas as métricas, nas duas amostras**, e vence 12 de 14 desacordos. O erro dominante do legado é ler "5" da citação "BI-RADS 5ª ed". Os 2 erros restantes do motor são de super-agregação (investigados; correção adiada — §5).

| Pergunta | Resposta |
|---|---|
| Quem lê a categoria com mais acurácia? | **Motor** (multiclasse 99,9% vs 99,6% e 99,6% vs 96,1%) |
| Onde discordam, quem acerta? | **Motor em 12 de 14**; legado em 0; 2 ambos errados |
| O motor erra? | **2 casos**, ambos super-agregação (sem correção segura ainda) |
| Pode ir para shadow em HML? | **Sim** na dimensão técnica (fixes aplicados e validados); resta revisão clínica |

**Método/limitações:** categoria como inteiro (4A/4B/4C → 4, pois o legado não distingue). Gabarito = categoria da **conclusão** do laudo; nos 14 desacordos o motor foi conferido correto em 12 e ambos erraram em 2. Métricas no universo de **pares comparáveis** (ambos extraíram categoria): 893 (representativa) e 254 (estratificada). Sem revisão clínica formal ainda.

---

## 2. Métricas completas — tarefa binária "BI-RADS ≥ 4"

> Corte clínico acionável (RADS-only): positivo = categoria ≥ 4. Permite precision/recall/specificity/F1/F2/MCC para cada sistema contra o gabarito.

### Representativa (893 pares · 79 positivos)
| Métrica | **Motor** | **Legado** |
|---|---|---|
| TP / FP / TN / FN | 79 / 1 / 813 / 0 | 79 / 2 / 812 / 0 |
| Precision | **0,988** | 0,975 |
| Recall (sensibilidade) | 1,000 | 1,000 |
| Specificity | **0,999** | 0,998 |
| Accuracy | **0,999** | 0,998 |
| F1 | **0,994** | 0,988 |
| F2 | **0,997** | 0,995 |
| MCC | **0,993** | 0,986 |

### Estratificada (254 pares · 114 positivos)
| Métrica | **Motor** | **Legado** |
|---|---|---|
| TP / FP / TN / FN | 114 / 0 / 140 / 0 | 113 / 7 / 133 / 1 |
| Precision | **1,000** | 0,942 |
| Recall | **1,000** | 0,991 |
| Specificity | **1,000** | 0,950 |
| Accuracy | **1,000** | 0,969 |
| F1 | **1,000** | 0,966 |
| F2 | **1,000** | 0,981 |
| MCC | **1,000** | 0,938 |

---

## 3. Métricas completas — categoria exata (multiclasse 0–6)

| Métrica | Motor (repr.) | Legado (repr.) | Motor (estrat.) | Legado (estrat.) |
|---|---|---|---|---|
| Accuracy | **0,9989** | 0,9955 | **0,9961** | 0,9606 |
| Macro-F1 | **0,998** | 0,912 | **0,997** | 0,960 |
| Erros de categoria | **1** | 4 | **1** | 10 |
| Desacordos vencidos (14 total) | **12** | 0 (+2 ambos errados) | | |

> **Macro-F1** (média por categoria, sensível às classes raras 4/5/6) é onde o motor mais se destaca: legado 0,912 / 0,960 vs motor 0,998 / 0,997 — porque o artefato "5ª ed" do legado contamina justamente as categorias altas.

---

## 4. Onde cada sistema erra

**Legado (14 erros):**
| Padrão | Repr. | Estrat. | Causa |
|---|---|---|---|
| Lê "5" da citação **"BI-RADS 5ª ed / 5th ed"** | 3 | 8 | Não remove a seção de referência ACR |
| **Romano VI não convertido** → 0 | — | 1 | Conversão de romanos só cobre I–V |
| Número de **histórico/boilerplate** | 1 | 1 | Janela ±3 palavras pega fora da conclusão |

**Motor (2 erros — super-agregação):**
| Caso | Verdade | Motor leu | Causa |
|---|---|---|---|
| `…030002656681` (estrat) | 2 | 3 | Pegou "BI-RADS 3" de **comparação com exame anterior** |
| `…640013186643` (repr) | ~3 | 4 | Pegou categoria de **exame anterior citado** (máximo do documento) |

> A política `max_category` agrega o maior BI-RADS de qualquer parte do laudo, incluindo exames anteriores citados. **Não corrigido nesta versão** (ver §5).

---

## 5. Fixes desta versão

**✅ Aplicados — só no pattern da config (sem mudança na lib, sem rebuild de wheel):**
1. **Tolerância a erro de grafia** — `(?:BI[- _]?RADS|BIRADS|…)` → `(?:B[IR]?[- _]?R{1,2}ADS|…)`. Cobre `BR-RADS`, `BRADS`, R duplicado; mantém o núcleo `RADS` (com `S`) para não casar PT ("brado/bradar"). Recuperou `…0008210394` ("ACR BR-RADS - 3").
2. **Separador com parênteses** — `[:.=°º®ª-]` → `[:.=°º®ª()-]`. Casa `BI-RADS®(USG) 6` → recuperou `…010007963856`, um **BI-RADS 6 (carcinoma invasor)** que o motor perdia.

Validados: exact-match estável/melhor, suíte da lib **141 passed**, ruff e mypy verdes.

**❌ Revertido — super-agregação (§4) — 2 abordagens tentadas, ambas regrediram:**
1. **Exclusão por contexto comparativo** (marcadores "comparação/laudo prévio" + data): cortava conclusões legítimas → exact-match repr. 99,55% → 97,4%.
2. **Scoping por seção de conclusão** (agregar só após "Impressão/Conclusão"): corrige os 2 casos, mas o BI-RADS operativo nem sempre está na conclusão (às vezes nos Achados) → exact-match estrat. **96,06% → 88,98%** (≈18 regressões nas categorias altas).

Ambas revertidas (lib no baseline, 141 passed). **Conclusão:** distinguir a categoria operativa de citações de exame anterior exige compreensão de documento de nível clínico, não heurística de marcador/seção. Não vale trocar 2 acertos por ~18 erros. **Os 2 casos ficam como limitação conhecida e aceita** (motor em 99,8%+ de acurácia); reabrir só com um parser de seção validado clinicamente.

**🛠 Bancada — CSV Excel-safe:** `id_exame` agora gravado como `="<id>"` no CSV de divergências, evitando que o Excel o converta em notação científica / perca zeros à esquerda. Os 4 ids antes corrompidos foram recuperados por conteúdo do laudo (todos `igual`, sem erro escondido).

---

## 6. Pendências antes do shadow em HML

| # | Ação | Tipo | Responsável |
|---|---|---|---|
| 1 | **Super-agregação** (2 casos) — limitação aceita; só reabrir com parser de seção validado clinicamente (2 heurísticas já regrediram — §5) | Técnico (motor, grande) | Motor + validação clínica |
| 2 | Revisão clínica dos 2 ambos-errados + lote dos "iguais" | Clínico | Time clínico |
| 3 | ✅ Reconferidos os 4 ids corrompidos pelo Excel (todos `igual`) | Técnico | **Concluído** |
| 4 | Commit dos fixes (config v2 + CSV Excel-safe) + publicação da wheel | Técnico | Aguarda autorização |

---

## 7. Recomendação final

> Na leitura de categoria BI-RADS — a única comparação justa — **o motor supera o legado em todas as métricas** (binário ≥4: MCC 0,993 vs 0,986 e 1,000 vs 0,938; multiclasse macro-F1 0,998 vs 0,912 e 0,997 vs 0,960), vencendo 12 dos 14 desacordos. Os fixes de pattern fecharam os misses pontuais, incluindo um **BI-RADS 6 de carcinoma**, sem regressão. Restam 2 erros de super-agregação, cuja correção segura exige extração por seção (item dedicado).
>
> **Próximo passo:** levar à **rodada shadow em HML** (entrada do legado → process → homolog) após commit dos fixes; super-agregação e revisão clínica correm em paralelo. A relevância (escopo achado × ≥4) permanece como decisão de negócio, separada desta avaliação.

---

*Fonte: bancada `nlp-engine-lib/bancada/` (`run_ab_birads.py`, amostras `sample_birads_repr2.jsonl` e `sample_birads.jsonl`). Gabarito = conclusão do laudo; desacordos adjudicados manualmente. Antecedentes: `checkpoint-birads-bancada-ab-2026-06-19.md`, `s11-birads-paridade-rads-v0.md`.*
