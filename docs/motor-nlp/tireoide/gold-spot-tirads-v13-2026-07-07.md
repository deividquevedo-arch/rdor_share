# 🥇 GOLD SPOT — TI-RADS motor v13 (llm_http recalibrado)

**Congelado em:** 2026-07-07
**Status:** ponto de referência oficial do motor TI-RADS para a linha de cuidado Tireoide V1. Qualquer run futuro compara-se contra este.

---

## Identidade do run

| Item | Valor |
|---|---|
| Config-fonte | `fabrica-ia-plataforma/apps/databricks/nlp_engine/ntb_ia_tirads_config.py` |
| `config_version` | `0.1.0-tirads-rads-v13` |
| Commit | `b0c27df` (branch `test/rads-e2e-hml`) |
| Perfil | `llm_http` — embeddings `hybrid` (MiniLM, threshold **0,92**) + `llm_router` Haiku 4.5 (`keep_current`) |
| Dataset | tirads D=2026-06-29, **895 exames** |
| CSVs | `Downloads/ntb_ia_motor_e2e_{full,gains}_v13.csv` · revisão `docs/motor-nlp/notas/residuo-revisao-e2e-v13-2026-07-07.csv` |

## Ingredientes da recalibração (vs v11/v12)

1. `llm_router.fallback_policy: positive_in_band → **keep_current**` (nlp + runtime) — LLM só mantém/rebaixa.
2. `embeddings.similarity_threshold: 0,78 → **0,92**` — neutraliza inflação de embedding no corpus homogêneo.
3. `prompt_system`+`specialty_context` reescritos no padrão clínico-rico de hepato (config-master real `ntb-config_absolute.yaml v0.1.12`), com inclui/exclui V1 explícito + "na dúvida → false".

`rule_only` (produção v10) INTOCADO — as 3 mudanças são inertes nele.

---

## 🎯 Métricas GOLD (contexto de negócio V1)

> Estimativa por **auditoria caso-a-caso** (não homologação manual). Matriz: TP=260 · FP=1 · FN=1 · TN=633.

| Métrica | Valor |
|---|---|
| Accuracy | **0,9978** |
| Precision | **0,9962** |
| Recall / Sensibilidade | **0,9962** |
| Specificity | **0,9984** |
| F1 | **0,9962** |
| F2 | **0,9962** |
| **MCC** | **0,9946** |

**Garantias-chave:** `FN = 0` vs legado (recall relevância 1,000) · `llm_error = 0` · concordância de categoria **99,1%**.

### Composição dos 261 positivos
- 170 forte (nódulo/cisto/TR4-5/massa/bócio/tumor) · 17 linfonodo · ~4 bócio/tireomegalia difusa (recuperado pelo LLM) · ~1 FP real (linfonodos normais).

### Lado negativo (634) — limpo
- Rejeições corretas: US testicular/partes moles não-tireoide · tireoide normal (TR4/5 de legenda) · bócio em linha de indicação. Business-FN ≈ 0-1.

---

## ⚠️ 2 políticas de negócio que movem o número

| Decisão | Efeito na métrica |
|---|---|
| Linfonodo reacional/normal **não** conta | pior caso precision ≈ 0,935 / MCC ≈ 0,92 (se todos os 17 forem reacionais) |
| Bócio/tireomegalia difusa sem nódulo **não** conta | efeito pequeno (~4 casos) |
| Cistos coloides <5mm **contam** | FN → 0, recall = 1,000 |

Faixa realista do número final: **~0,94 a ~0,996** conforme essas decisões. Best estimate atual: **~0,995 / MCC 0,99**.

---

## Ranking de perfis (referência histórica)

`llm_http v13` (FP real ~1, +recupera bócio difuso) **>** `rule_only v10` (FP 86 = política, barato) **>>** `hybrid v11` (FP 586, embedding inflado) **>** `llm_http v12` (FP 206, espelho errado de hepato).

## Pendências (não bloqueiam o gold spot)
- [ ] Homologação manual do negócio (converte esta estimativa em número oficial).
- [ ] Decisão das 2 políticas (linfonodo reacional · bócio difuso).
- [ ] Decisão de produção: `llm_http v13` (precisão, ~718 chamadas LLM/run) vs `rule_only v10` (barato).
- [ ] (Opcional) fechar bócio difuso no rule engine (léxico tireomegalia) p/ não depender do LLM.
