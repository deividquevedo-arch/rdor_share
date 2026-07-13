# Hepatologia — validação clínica Carol + homolog motor (v0)

**Fecho do parêntese** (homolog 496 + recorte adjudicado). Referência para produto e DS clínica. Sem PHI.

**Homolog técnica:** `fabrica-ia-plataforma/apps/databricks/hepatologia_motor` — `dt_execucao` 2026-05-29, n=496, match motor×legado ≈80,6%.

---

## 1. Recorte adjudicado (Carol = gold)

| | Dia 1 | Dia 2 | **Total** |
|---|-------|-------|-----------|
| **n** | 22 | 96 | **118** |
| Motor acertos | 14 | 53 | **67** |
| Legado acertos | 8 | 43 | **51** |
| **Acurácia motor** | 63,6% | 55,2% | **56,8%** |
| **Acurácia legado** | 36,4% | 44,8% | **43,2%** |
| Motor recall | 100% | 100% | **100%** |
| Motor especificidade | 0% | 6,5% | **5,6%** |
| Legado recall | 0% | 0% | **0%** |
| Legado especificidade | 100% | 93,5% | **94,4%** |

**Leitura:** na amostra revista, o motor **acerta mais** que o legado (+13,6 p.p. de acurácia), **não perde relevantes** (FN=0), mas **marca em excesso** (FP alto, especificidade baixa).

---

## 2. Hipótese “concordantes = clinicamente certos” (496)

Premissa: nos **400** concordantes motor=legado ambos certos; nos **96** discordantes aplicam-se as métricas do dia 2 (Carol).

| Métrica | Motor (estimado) | Legado (estimado) |
|---------|------------------|-------------------|
| **Acurácia** | **91,3%** (453/496) | **89,3%** (443/496) |
| Recall | 100% | 80,6% |
| Especificidade | 81,9% | 98,7% |
| Precisão (PPV) | 85,7% | 98,6% |

**Limitação:** os 400 concordantes não foram adjudicados um a um; prevalência nos concordantes estimada (~52% relevante). Não substitui validação completa dos 496.

---

## 3. Conclusão executiva (motor vs legado)

| Pergunta | Resposta |
|----------|----------|
| Motor melhor que legado? | **Sim** na amostra Carol; **ligeiramente sim** na hipótese dos 496. |
| Motor pronto para produção clínica? | **Não** — reduzir FP (léxico/config + perfil `rule_only` vs `llm_http`). |
| Legado como referência? | **Fraco em recall** na amostra revista; útil só como linha histórica. |

---

## 4. Seguinte foco (pós-parêntese)

Alinhar `configs/hepatologia/config.yaml` ao **hepatologia v2** + **documento clínico do time** (lista activa / removidos / em estudo). Ver `hepatologia-config-alinhamento-v2-clin-v0.md`.

---

*Atualizado: 2026-05-29. Task relacionada: S06 / homolog hepatologia motor.*
