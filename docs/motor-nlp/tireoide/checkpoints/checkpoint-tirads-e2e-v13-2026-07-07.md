# Checkpoint TI-RADS — Run E2E v13 (llm_http recalibrado) — motor × legado

**Data:** 2026-07-07
**Config-fonte:** `fabrica-ia-plataforma/apps/databricks/nlp_engine/ntb_ia_tirads_config.py` (`config_version 0.1.0-tirads-rads-v13`, commit `b0c27df` em `test/rads-e2e-hml`)
**Perfil do run:** `llm_http` (widget `perfil_motor`) → embeddings `hybrid` (MiniLM, threshold **0,92**) + `llm_router` Haiku 4.5 (`keep_current`).
**Arquivos:** `Downloads/ntb_ia_motor_e2e_full_v13.csv` (895 exames) · `..._gains_v13.csv` (194 ganhos) · revisão: `residuo-revisao-e2e-v13-2026-07-07.csv`.
**Sucede:** `checkpoint-tirads-e2e-v12-2026-07-06.md`.

> **Contexto:** o run v12 concluíra "LLM não ajuda". Descobriu-se depois que a config v11/v12 espelhou a config de hepato **da branch** (errada); a **master real** é o backup `Projects/ntb-config_absolute.yaml` (`v0.1.12-hep-llm-prompt-clin`). A v13 recalibra as escalas contra ela.

---

## 1. As 3 mudanças da v13 (inertes em `rule_only` — produção v10 intocada)

1. **`llm_router.fallback_policy: positive_in_band → keep_current`** (nos blocos `nlp` e `runtime`). O LLM passa a só **manter/rebaixar**, nunca promover em abstain/erro. Era a fonte dos ~118 FP do v12 (e dos 771 do v11 com token quebrado).
2. **`embeddings.similarity_threshold: 0,78 → 0,92`**. Corpus de tireoide é homogêneo (semantic ~0,82 até em laudo normal) → 0,78 promovia ~500 FP só-embedding no perfil hybrid. 0,92 neutraliza.
3. **`prompt_system` + `specialty_context` reescritos** no padrão clínico-rico de hepato: inclui/exclui V1 explícito + "na dúvida → false" + nota de que o legado (só TI-RADS>3) NÃO é a regra.

**Não mexi em `segmentation`** (`full_doc`) — é load-bearing do baseline rule/rads de produção; trocar afetaria o `rule_only`.

## 2. Resultado — v13 vs v12

| Métrica | v12 | **v13** | Leitura |
|---|---|---|---|
| **FN** (achado real derrubado) | 0 | **0** | ✅ sem regressão de recall |
| Ganhos (só-motor) | 206 | **194** | −12 normais/reacionais bloqueados |
| — forte (nódulo/cisto/TR4-5/massa/bócio/tumor) | 170 | **170** | regra intacta |
| — linfonodo | 31 | **17** | LLM rebaixou ~14 reacionais/normais |
| — REVISAR | 5 | **~5** | composição melhor (ver §3) |
| `llm_positive` | 151 | **141** | qualidade muito maior |
| `llm_negative` | 565 | **577** | +12 bloqueios corretos |
| `llm_fallback` | 2 | **0** | `keep_current` |
| `llm_error` | 0 | **0** | LLM operante |

**4 dos 5 FP-candidatos do v12 agora corretamente bloqueados.** Crosstab v13: (0,0)=632 · (1,0)=194 · (1,1)=67 · (0,null)=2.

## 3. Os ~5 REVISAR — a maioria NÃO é FP

- **4** são tireoide de **"dimensões aumentadas / tireoidopatia difusa" sem nódulo focal** = **bócio/tireomegalia difusa** → **relevante por V1** ("Bócio"). O LLM reconheceu tireomegalia como bócio; a **regra não pega** por não haver o token literal "bócio" → **recall recuperado, não FP** (ganho de gap da regra).
- **~1** FP real: laudo com **"linfonodos de aspecto habitual"** (normais) — o único carryover do v12 ainda positivo.

**Superfície de FP real: ~5/206 (v12) → ~1/194 (~0,5%) no v13.**

## 4. Conclusão

- **Revertida a "conclusão definitiva" do v12.** O LLM não era o problema — a config espelhada (`positive_in_band` + prompt fraco) era. Com a calibração da master real de hepato, **`llm_http` v13 é o melhor perfil**: maior precisão, FN=0 e recupera bócio difuso.
- **Ranking de perfis (tirads D=2026-06-29):** `llm_http` v13 (FP real ~1, +bócio difuso) > `rule_only` v10 (FP 86, todos achado real/política) >> `hybrid` v11 (FP 586, embedding inflado) > `llm_http` v12 (FP 206, espelho errado).

## 5. Trade-offs e pendências

- **Custo/latência:** `llm_http` fez ~718 chamadas de LLM/run — tem custo de endpoint e latência que `rule_only` v10 não tem. **Decisão de produção do time:** `llm_http` v13 (precisão) vs `rule_only` v10 (barato; FP = política, não bug).
- **2 pontos de política de negócio (quantificados):**
  - [ ] **Linfonodo reacional/normal** conta como captação? (move os 17 "linfonodo").
  - [ ] **Bócio/tireomegalia difusa sem nódulo** conta? (move os ~4 REVISAR; se sim, viram TP e a regra deveria ganhar léxico "dimensões aumentadas/tireomegalia").
- [ ] (Opcional) rodar `hybrid` v13 puro para isolar/confirmar que o threshold 0,92 matou a inflação de embedding.
- [ ] Se negócio confirmar bócio difuso: adicionar ao rule engine o gatilho de tireomegalia (fecha o gap sem depender do LLM).

---

**Régua de interpretação (sempre):** o legado marca relevante só **TI-RADS > 3** e **não é gabarito** — o motor segue a **spec de negócio V1** (todos nódulos/cistos + massa/linfonodo/tumor + bócio + TI-RADS 4/5). A "queda de precisão vs legado" é divergência de política, não erro.
