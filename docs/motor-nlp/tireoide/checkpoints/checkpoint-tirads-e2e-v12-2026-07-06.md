# Checkpoint TI-RADS — Homologação do run e2e v12 (motor × legado)

**Data:** 2026-07-06
**Config-fonte:** `fabrica-ia-plataforma/apps/databricks/nlp_engine/ntb_ia_tirads_config.py` (`config_version 0.1.0-tirads-rads-v12`)
**Perfil do run:** `llm_http` (widget `perfil_motor`) → embeddings `hybrid` (MiniLM multilingual) + `llm_router` Haiku 4.5. O default do config é `rule_only`; o perfil liga embeddings + LLM.
**Arquivos:** `ntb_ia_motor_e2e_engine_vs_legacy_v12.csv` (895 exames) · `ntb_ia_motor_e2e_gain_audit_v12.csv` (206 ganhos).

> **Escopo vigente (direcionamento do head, 2026-07-06):** seguimos em **V1 GLOBAL**. Condicionamento de finding por tipo de exame e a **camada dimensional** (nódulo > 1cm, VEF1 < 30%, mmHg, PMAP, RVP…) **só entram DEPOIS** de fechar a decisão sobre embeddings + `llm_http` e refinar. Nada disto é gap a corrigir agora.

---

## 1. Panorama (895 exames)

| Segmento | N |
|---|---|
| Ambos irrelevantes (0,0) | 620 |
| **Só-motor / ganho (1,0)** | **206** |
| Ambos relevantes (1,1) | 67 |
| **motor relevante (total)** | **273** |
| Concordância de categoria | 887 true / 6 false / 2 null (~99,3%) |

`decision_source` (todos): `llm_router_llm_negative` 565 · `hybrid_calibrated` 177 · `llm_router_llm_positive` 151 · `llm_router_llm_fallback` 2.

## 2. Ganho de 206 validado contra a spec V1 (não é ruído)

Critérios V1 (relevância = OU): TI-RADS 4/5 · Nódulo/Cisto (todos, sem filtro de tamanho) · Massa/Linfonodo/Tumor · Bócio · Hipertireoidismo · Bethesda. Tipos de exame válidos incluem **TC de pescoço** (não específico).

Reclassificação dos 206 ganhos (com tratamento de negação):

| Sustentação | N | Veredito |
|---|---|---|
| **Critério forte** (nódulo/cisto/TI-RADS 4-5/massa/bócio/tumor) | **170** | ✅ captação sólida |
| **Linfonodo positivo** (proeminente/aumentado/megalia) | **31** | ✅ válido V1 — *confirmar com negócio se linfonodo reacional conta* |
| **REVISAR** (sem chave V1 clara) | **5** | ⚠️ candidatos a FP do LLM |

- **Confiança por origem:** `hybrid_calibrated` (86 ganhos) média **0,865**, zero abaixo de 0,50. `llm_router_llm_positive` (118 ganhos) média **0,451**. A baixa confiança do LLM **não** implica erro — a spec V1 é larga e a maioria casa com chave válida.
- **Tipo de exame dos 206:** US tireoide/cervical 154 · TC pescoço 21 · PAAF/biópsia 17 · cintilografia 1 · indefinido 13. **Todos dentro do escopo de exame.**

## 3. Superfície de FP do LLM router — 5 casos (~2,4%)

Todos `llm_router_llm_positive` + `cat_motor=null` (arquivo `residuo-revisao-e2e-v12-2026-07-06.csv`):

1. US tireoide — "dimensões aumentadas + ecotextura heterogênea, **sem lesão focal**" (possível bócio/tireomegalia difusa; regra não disparou por não citar "bócio").
2. TC pescoço — artefatos dentários; sem achado tireoidiano claro.
3. US partes moles — "linfonodos de aspecto **habitual**" (0,5 cm, normais) → LLM sobre-marcou.
4. TC pescoço — "sem particularidades / sem alterações focais" (pescoço normal) → provável FP.
5. US partes moles cervical — lesão cutânea + "linfonodo aspecto não habitual, **dimensões normais**" → borderline.

**Leitura:** a superfície de FP do LLM router está confinada a laudos **normais ou de textura difusa sem lesão focal**. É o dado objetivo para a decisão go/no-go do perfil `llm_http`.

## 4. Divergências de categoria (6) — motor continua correto

Destaque: `OBSWPDHMEMO...2778375` — **motor TR4 × legado 5**. O laudo diz `TI-RADS: 4` com `Total de pontos: 5`. Na escala ACR, 5 pontos = **TR4** (TR5 só ≥7). O legado leu os "5 **pontos**" como categoria. **+1 caso motor-correto / legado-errado.** As demais são `cat_motor=null vs legado=0/-1` (relevância sem categoria) e 1 TR1 benigno.

## 5. Confirmações de config (runner v12) vs escopo V1 global

- `relevance_mode: 'rule_plus_rads'` → relevância por OU (achado clínico OU RADS TR4/5/6). ✅
- **Sem filtro de tamanho** (>1cm) em lugar nenhum → coerente com V1. ✅
- **Sem condicionamento por exame**: findings promovem globalmente; `hipertireoidismo` é finding global (não restrito a cintilografia) e **`bethesda` não existe como finding**. ✅ coerente com "global por enquanto".
- `rads_extraction.negation.tokens: []` (grau RADS é asserção, nunca negado) e `aggregation_legend_filter.enabled: true` (anti super-agregação). ✅

## 6. Pendências de negócio / próximos passos

- [ ] **Negócio:** linfonodo **reacional/normal** conta como captação? (move 31 ganhos "linfonodo" + parte dos 5 REVISAR).
- [ ] **Homologar os 5 REVISAR** (arquivo dedicado) — mede FP real do `llm_http`.
- [ ] **Decisão embeddings + `llm_http`** (go/no-go do perfil) — usar o FP ~2,4% como insumo. Só depois: camada dimensional (>1cm etc.) e condicionamento por exame.
- [ ] Registrar divergência TR4×5 no consolidado "motor-correto".

---

**Nota de arquitetura:** a fonte-de-verdade da camada de achados/relevância é o **runner `.py`** (`ntb_ia_tirads_config.py`), NÃO o `configs/nlp/tireoide/config.yaml` — este último está `relevance_mode: rads_only` + `llm_router.enabled: false` e alimenta APENAS o A/B de *categoria* (`run_ab_rads.py`, bloco `rads_extraction`). Não confundir os dois ao investigar o comportamento de relevância.
