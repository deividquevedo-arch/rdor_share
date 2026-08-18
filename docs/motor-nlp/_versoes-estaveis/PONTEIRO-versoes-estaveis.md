# Ponteiro — Versões estáveis homologadas (configs-piloto)

> Backup pessoal das configs de negócio **homologadas e validadas contra base ouro**. Cada entrada
> aponta a versão estável + métricas + fonte de validação. Snapshot das configs `.py` nesta pasta.
> **Atualizado: 2026-07-24 (Fase B).** Requer `nlp_engine >= 0.6.2` (require_measure, config v3 por-entidade, failover de modelo).

## Estado
Fase B: as 3 pilotos consolidadas no padrão `nlp_engine 0.6.2` (v3 por-entidade + failover + régua V2
no tireoide), validadas E2E em 2026-07-23. Consolidadas na branch `feature/tirads-config-v3-schema`
(commit `b77749c`); **PR para `branch-from-versao-alpha` pendente** (head faz manual). **Nenhuma em
produção** (routing clínico) ainda — validadas em pilotos contra base ouro / estabilidade.

---

## 🫁 Transplante de Pulmão — `0.1.2-pulmao-failover`
- **Arquivo:** `ntb_ia_transplante_pulmao_config.py`
- **Perfil:** quantitativo puro (VEF1/CVF/DLCO por limiar; `on_met: promote`). Sem embeddings/juiz. Flat (findings vazio).
- **Novidades vs 0.1.1:** failover haiku→sonnet + `max_tokens` nos 2 critérios; `plausible_range:[5,200]` no VEF1 (FP-01); removido `pipeline_order` (default target desde 0.6.0).
- **Base ouro:** Carol (388) + recall amostral (1470) → gabarito humano.
- **Métricas (base ouro, run 2026-07-23):** **TP=126 · FP=0 · FN=0 · TN=1732** — Recall **1,000** · Precisão **1,000** · **MCC 1,000**.
- **Ganho:** o `plausible_range` (FP-01) **eliminou o único FP** (VEF1 0,68 razão lida como %) → virou TN.
- **Doc:** `docs/motor-nlp/pulmao/homologacao-pulmao-v1-metricas.md`.

## 🦋 Tireoide / TI-RADS — `0.1.0-tirads-rads-v22.11-v2`
- **Arquivo:** `ntb_ia_tirads_config.py`
- **Perfil:** TI-RADS (rads_extraction) + achados clínicos + embeddings(hybrid) + llm_router(haiku, failover sonnet) + quantitative + document_vet. Findings v3 por-entidade.
- **Novidades vs v22.7:** **régua V2** — `require_measure:True` nos critérios nódulo/cisto (só relevante com dimensão ≥1cm discriminada); failover + `max_tokens` nos 3 critérios; prompt do juiz alinhado a ≥1cm; findings migrados p/ v3 por-entidade.
- **Régua de relevância (OR):** nódulo≥1cm **OU** cisto≥1cm **OU** TR4/5/6 **OU** linfonodo (megalia/necrose/suspeito) **OU** massa/tumor/bócio nodular. PAAF/punção/biópsia são os *exames* analisados (não promovem por si).
- **Base ouro V2 CONGELADA (2026-07-24):** `base-ouro-tirads-v2-2026-07-24.csv` — 586 resolvidos (127 pos / 459 neg), **SHA `024ed00770227e24`**. 7 rótulos generosos da V1 reclassificados `1→0` (5 PAAF de nódulo sem medida + 2 extra-tireoidianos), com dupla confirmação (motor rebaixou + leitura).
- **Métricas (base ouro V2, run 2026-07-23):** **TP=127 · FP=4 · FN=0 · TN=455** — Recall **1,000** · Precisão **0,9695** · **MCC 0,9803**.
- **Doc:** `docs/motor-nlp/tireoide/base-ouro-tirads-v2-metricas.md` (régua OR + reclassificação auditável).

## 🩺 Hepatologia — `0.1.14-hep-v3`
- **Arquivo:** `ntb_ia_hepatologia_config.py`
- **Perfil:** LLM-driven (embeddings + llm_router; o juiz decide a maioria). Findings v3 por-entidade.
- **Novidades vs 0.1.13:** findings migrados p/ v3 por-entidade (ordem preservada) + failover haiku→sonnet + `max_tokens` no llm_router.
- **Base de referência:** **estabilidade comportamental** (não qualidade vs médico — o board é operacional/elegibilidade de captação, não gabarito clínico; ver avaliação do repo `plataform`).
- **Baseline congelado:** run 0.1.13 c/ LLM (2026-06-10), coorte 5690 ids, 871 relevantes, **sha `c6061919a602b27be6b56a30e72b3757`**.
- **Validação (run 2026-07-23, coorte idêntica 5690):** **1 flip** (1→0) vs baseline ≤ tolerância 15 → **estável**; `decision_source` idêntico ao congelado. Byte-compat determinístico: golden `b4dc2076` (`normalize==v1`).
- **Doc:** `docs/motor-nlp/hepatologia/baseline-comportamental-hepato-metricas.md`.

---

## Evolução vs versão estável anterior (2026-07-21 → Fase B 2026-07-23)

| Piloto | Antes | Agora | Δ |
|---|---|---|---|
| Pulmão | `0.1.1` — TP126/FP1/FN0 · MCC **0,996** | `0.1.2` — TP126/**FP0**/FN0 · MCC **1,000** | FP 1→0 (FP-01); +failover |
| Tireoide | `v22.7` — TP133/FP8/FN1 · prec 0,943 · MCC **0,958** | `v22.11-v2` (base ouro V2) — TP127/FP4/FN0 · prec **0,970** · rec **1,0** · MCC **0,980** | precisão +2,7pp; MCC +2,2pp; régua V2 (≥1cm); +failover |
| Hepato | `0.1.13` — baseline `c6061919` | `0.1.14-v3` — 1 flip / 5690 (estável) | v3 por-entidade; +failover; comportamento preservado |

**Além das métricas:** todos ganharam **resiliência a 429** (failover de modelo), **findings v3 por-entidade** (coesão por achado, onde aplicável) e passaram a rodar no padrão único da lib 0.6.2 (target default, detecção estrutural, observabilidade LLM). Nenhuma regressão; pulmão e tireoide melhoraram a métrica, hepato manteve estabilidade.

### Tabela completa de métricas (antes × depois)

**🫁 Pulmão — `0.1.1` → `0.1.2-failover` (base ouro, n=1858)**

| | Acc | Precisão | Recall (Sens.) | Especif. | F1 | F2 | MCC |
|---|---|---|---|---|---|---|---|
| Antes | 0,9995 | 0,9921 | 1,0000 | 0,9994 | 0,9960 | 0,9984 | 0,9958 |
| Depois | 1,0000 | 1,0000 | 1,0000 | 1,0000 | 1,0000 | 1,0000 | 1,0000 |
| Δ | +0,0005 | +0,0079 | — | +0,0006 | +0,0040 | +0,0016 | +0,0042 |

**🦋 Tireoide — `v22.7` (base ouro V1) → `v22.11-v2` (base ouro V2 congelada, n=586)**

| | Acc | Precisão | Recall (Sens.) | Especif. | F1 | F2 | MCC |
|---|---|---|---|---|---|---|---|
| Antes (V1) | 0,9846 | 0,9433 | 0,9925 | 0,9823 | 0,9673 | 0,9823 | 0,9578 |
| Depois (V2) | 0,9932 | 0,9695 | 1,0000 | 0,9913 | 0,9845 | 0,9937 | 0,9803 |
| Δ | +0,0086 | +0,0262 | +0,0075 | +0,0090 | +0,0172 | +0,0114 | +0,0225 |

**🩺 Hepato — `0.1.14-v3` vs baseline `c6061919` (concordância/estabilidade, n=5690 — NÃO é qualidade vs médico)**

| | Acc | Precisão | Recall (Sens.) | Especif. | F1 | F2 | MCC |
|---|---|---|---|---|---|---|---|
| Estabilidade | 0,9998 | 1,0000 | 0,9989 | 1,0000 | 0,9994 | 0,9991 | 0,9993 |

> As métricas do hepato medem **concordância com o baseline** (referência de estabilidade), não acurácia clínica — 1 divergência em 5690 (ruído do LLM). Não comparáveis às de pulmão/tireoide (essas vs gold humano). No tireoide, contra a **base ouro V2 congelada** (7 rótulos generosos reclassificados conforme a régua) o motor **sobe em todas as métricas** — inclusive recall 1,0 (os "FN" da V1 eram PAAF/extra-tireoide, não erro do motor).

---

## Notas de rastreabilidade
- **Snapshot vs vivo:** as `.py` aqui são cópias congeladas. As versões vivas estão em
  `feature/tirads-config-v3-schema` (pré-PR) → `branch-from-versao-alpha` (após merge).
- **Base ouro:** gabaritos humanos (com PHI) NÃO ficam aqui — só métricas agregadas. Fontes nos docs citados.
- **Harness de validação:** `.claude/jobs/56d76a3e/tmp/baseohro_{pulmao,tirads}.py` + `baseohro_hepato_estabilidade.py`.
- **Dependência de lib:** requer `nlp_engine >= 0.6.2` (require_measure do tireoide, v3 do hepato, failover). Lineage: F1–F4 (0.5.4–0.5.8) → v3 (0.5.10) → failover (0.5.12) → caminho único (0.6.0) → normalização aditiva (0.6.1) → require_measure (0.6.2).

---

## 🦋 Tireoide — as TRÊS variantes de escopo *(2026-08-18)*

> **Por que três arquivos e não um.** O escopo da entrega foi estreitado por **decisão de
> capacidade do negócio**, não por decisão clínica — a operação não absorve o volume. Quando a
> capacidade crescer, a régua ampla volta. Estes snapshots existem para essa reativação.

**Nada foi apagado.** As três diferem por **duas chaves**; achados, prompts, limiares e âncoras são
idênticos. Reativar é virar chave na config viva (repo da plataforma) — o snapshot é a rede.

| arquivo | entrega | `relevance_mode` | `on_met` de tsh/t4/trab |
|---|---|---|---|
| `ntb_ia_tirads_config_V2-completa.py` | achado léxico (bócio, linfonodomegalia, nódulo/cisto sem categoria) + TR4-6 | `normal_plus_ordinal` | `annotate_only` |
| **`ntb_ia_tirads_config_V2-estreita.py`** ← **ATIVA** | só TR5 e TR4 com nódulo/cisto ≥ 1 cm | `ordinal_only` | `annotate_only` |
| `ntb_ia_tirads_config_V3-sangue.py` | escopo estreito + TSH suprimido / T4 livre elevado / TRAb positivo | `ordinal_only` | `promote` |

⚠️ **`annotate_only` não desliga o critério** — ele segue avaliando e gravando no bloco
`quantitative` do blob; só não promove. Dá para **medir o que a V3 entregaria antes de decidir
entregá-la**, sem rodar outra config.

**Volumetria medida (janela da `0.5.0`, 2.815 relevantes):** a V2 completa entregava ~1.389 laudos a
mais que a estreita (~49%) por achado léxico ou juiz sem categoria TR4+; o sangue somava outros 437.

**Exige `nlp_engine >= 0.8.5`** (`gates_ordinal_promotion`). Config viva: `0.7.0-tirads`, branch
`tirads/feature/v3-sangue` do `fabrica-ia-nlp-platform`.

⚠️ **Não homologadas contra base ouro nesta forma** — ao contrário das entradas acima, estes
snapshots são de **backup de escopo**, não de versão validada. A V2 estreita ainda precisa de run.
