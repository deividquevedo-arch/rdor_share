# Base ouro — TI-RADS / tireoide (Fase 0)

> Métricas do piloto TI-RADS/tireoide (TI-RADS + achados clínicos). Só agregados (sem id_exame /
> sem laudo — LGPD). Gold permanente = coluna `verdade`; o `fl` do motor é re-executado (harness live).

## Fonte (gold permanente)
`docs/motor-nlp/tireoide/dados/base-ouro-tirads-2026-07-11.csv` — 895 laudos; coluna `verdade`
(gabarito): 134 relevantes, 452 não, **300 PENDENTE + 9 "?" excluídos** → **586 resolvidos**.
`fonte`: concordancia (398) + medico/medico_validado/medico_manual (178) + concordancia_FN (10).

## Duas medições (o gold é o mesmo; muda a fonte do `fl`)
| Fonte do `fl` | TP | FP | FN | TN | Precisão | Recall | MCC |
|---|---|---|---|---|---|---|---|
| **Motor rule_only** (núcleo determinístico, sem LLM) — 2026-07-17 | 131 | 88 | 3 | 364 | 0,598 | **0,978** | 0,680 |
| Snapshot `fl_motor` do arquivo (run com LLM) | 114 | 12 | 20 | 440 | **0,905** | 0,851 | 0,842 |
| **Produção E2E (lib 0.5.5, LLM, config v22.6)** — 2026-07-17 | 133 | 8 | **1** | 444 | **0,9433** | **0,9925** | **0,9578** |

## Insight (arquitetura)
- **Recall vem da REGRA:** o núcleo rule_only quase não perde relevante (recall 0,978; 3 FN).
- **Precisão vem do JUIZ LLM:** o `llm_router` rebaixa ~76 falsos positivos (FP 88 → 12), levando a
  precisão de 0,60 → 0,90. Coerente com o princípio "LLM-juiz por último".

## Harness Fase 0
`.claude/jobs/56d76a3e/tmp/baseohro_tirads.py` — re-executa o motor e compara com `verdade`:
- **default (local rule_only):** gate do núcleo determinístico. Baseline congelado:
  `EXPECTED_RULEONLY = {TP:131, FP:88, FN:3, TN:364}`. Não precisa de LLM.
- **`--fl-csv <path>` (E2E):** recebe `id_exame,fl` de um run real com LLM (tireoide_config) e
  computa a matriz de produção vs `verdade`. Congelar `EXPECTED_PROD` quando o run existir.

## Produção validada (0.5.5)
Run E2E `ntb_ia_motor_e2e_ofc.csv` (lib 0.5.5, config v22.6, claude-haiku, embeddings on):
**recall 0,9925 · precisão 0,9433 · MCC 0,9578 · 1 FN.** O LLM router derrubou FP 88→8 e FN 3→1
vs rule_only (MCC 0,68 → 0,96). Confirma o insight (recall da regra, precisão do juiz) e valida a
0.5.5 (F1/F2 byte-compat) contra a base ouro. `EXPECTED_PROD = {TP:133, FP:8, FN:1, TN:444}`.

## Pendências
- Reconciliar com o "gold spot" da memória (~0,995/MCC 0,99/FN=0): provável subconjunto/config
  diferente; a base-ouro `verdade` (586 resolvidos) é a referência permanente. O número atual
  (0,958) é sobre os 586 resolvidos — os 300 PENDENTE não entram.
