# Hepatologia — tracker bancada motor vs legado (evolução)

**Objetivo:** acompanhar MR / FP / FN e decisões em corridas repetíveis.  
**Bancada:** `query_hepato_validate.csv` (local), `max_rows=2000`, mesmo YAML base por corrida salvo nota.

**Seguir — protocolo FP classe 3 (reduzir FP gold `3`, um eixo por iter):** ver [`fp-class3-baseline-freeze-v0.md`](../tireoide/checkpoints/fp-class3-baseline-freeze-v0.md) e matriz [`../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix_fp_class3_tuning.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix_fp_class3_tuning.yaml).

Ordem sugerida: **F1 semântica** (`fp3_f1_semantic_thr080`) → smoke 500 → full 2000; depois F2, F3, F4 conforme gates.

```text
# Smoke (500, cod123, Fase 1 primeiro cenário)
.\.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix --max-rows 500 --promotion-profile fn_priority --print-table --only-cod-123 --scenarios-yaml configs\hepatologia\scenarios\strategy_matrix_fp_class3_tuning.yaml --only-scenarios baseline,S5_hybrid_calibrated,fp3_f1_semantic_thr080 --out-json _local_samples\exports\hepatologia_diamond_bench\fp3_f1_thr080_smoke500.json

# Full (2000) após gate G1
.\.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 --promotion-profile fn_priority --print-table --only-cod-123 --scenarios-yaml configs\hepatologia\scenarios\strategy_matrix_fp_class3_tuning.yaml --only-scenarios baseline,S5_hybrid_calibrated,fp3_f1_semantic_thr080 --out-json _local_samples\exports\hepatologia_diamond_bench\fp3_f1_thr080_full2000.json
```

Antes: `$env:OPENAI_API_KEY` só necessário para cenários com `llm_router`; F1 é só embeddings (pode omitir).

---

## Encerramento — protocolo FP classe 3 (2026-05-08)

**Escopo:** `--max-rows 2000`, `--only-cod-123`, `strategy_matrix_fp_class3_tuning.yaml`, `fn_priority`.  
**Referência:** `S5_hybrid_calibrated` = `0,8659 | 132 | 86`.

### Decisão final (data-based)

| Item | Conteúdo |
|------|-----------|
| **Candidato oficial** | `fp3_f4_llm_neg_patterns` |
| **Motivo** | Melhor equilíbrio MR/FP/FN no full 2000 vs `S5` e vs `fp3_f3_llm_band_narrow`; combo **F5** não superou F4 no mesmo recorte. |
| **Telemetria** | `llm_error_rate = 0` nas corridas LLM reportadas |
| **Estabilidade F4** | Duas corridas full 2000 com variação pequena (ver tabela abaixo). |

### Smoke 500 (histórico consolidado, `--only-cod-123`)

| Cenário | MR | FP | FN | Decisão |
|---------|----:|---:|---:|---------|
| `S5_hybrid_calibrated` | 0,8726 | 33 | 20 | referência smoke |
| `fp3_f1_semantic_thr080` | 0,8654 | 33 | 23 | rejeitado |
| `fp3_f1_semantic_thr082` | 0,7212 | 22 | 94 | rejeitado |
| `fp3_f2_proximity_200` | 0,8726 | 33 | 20 | neutro |
| `fp3_f2_negwindow_7` | 0,8678 | 33 | 22 | rejeitado |
| `fp3_f3_llm_band_narrow` | 0,8918 | 28 | 17 | aprovado smoke |
| `fp3_f4_llm_neg_patterns` | 0,8870 | 27 | 20 | aprovado smoke |
| `fp3_f5_llm_band_narrow_neg_patterns` | 0,8870 | 28 | 19 | aprovado smoke; full não superou F4 |

### Full 2000 — finalistas e referência

| Cenário | MR | FP | FN | `llm_called_rate` | Notas |
|---------|----:|---:|---:|------------------:|-------|
| `S5_hybrid_calibrated` | 0,8659 | 132 | 86 | 0,000 | âncora |
| `fp3_f3_llm_band_narrow` | 0,8831 | 103 | 87 | 0,349 | +1 FN vs S5 |
| `fp3_f4_llm_neg_patterns` (run 1) | 0,8887 | 98 | 83 | 0,391 | melhor trade-off |
| `fp3_f4_llm_neg_patterns` (rerun) | 0,8881 | 99 | 83 | 0,391 | `fp3_f4_llm_neg_patterns_full2000_rerun.json` |

**Delta estabilidade F4 (run 1 → rerun):** MR −0,0006; FP +1; FN igual.

### Full 2000 — combo F5 (referência; não promovido sobre F4)

| Run | MR | FP | FN | Artefacto |
|-----|----:|---:|---:|-----------|
| 1 | 0,8862 | 99 | 86 | `fp3_f5_combo_full2000.json` |
| rerun | 0,8868 | 98 | 86 | `fp3_f5_combo_full2000_rerun.json` |

**Leitura:** MR e FN inferiores ao melhor **F4** no mesmo protocolo; manter **F4** como candidato.

### Checklist final (protocolo FP3)

- [x] F1–F2 encerradas sem candidato.
- [x] F3/F4 full 2000 + confronto.
- [x] F5 smoke + full + rerun + confronto vs F4.
- [x] Rerun estabilidade **F4** (full 2000).
- [x] Decisão documentada.

### Próximo passo (fora deste bench)

Migrar o patch do cenário vencedor para o fluxo acordado pelo time (YAML de produção / matriz / versionamento); seguir governança de mudança do motor.

---

## 1. Definições (fixas)

| Símbolo | Significado |
|--------|-------------|
| **MR** | `match_rate` = acordo motor vs gold (`legacy_s_n_from_row`) nos pares juntados |
| **FP** | Gold **N**, motor **S** (`N→S`) |
| **FN** | Gold **S**, motor **N** (`S→N`) |
| **Gold S/N** | Deriva de `cod_achado_relevante` (1/2→S, 3→N), ou fallback enc/`flgRelevante` — ver `audit_legacy_compare.legacy_s_n_from_row` |

---

## 2. Distribuição do gold (amostra 2000)

Medição local com `read_rows_semico_first` + `legacy_s_n_from_row` (sem PHI).

| Métrica | Valor |
|---------|------:|
| Linhas | 2000 |
| Gold **S** | 1480 |
| Gold **N** | 520 |
| Razão S:N | ≈ 2,85 : 1 |

*Nota:* ficheiro completo no lake pode ter mais linhas; o bench usa **2000** por `--max-rows`.

---

## 3. Abordagens (decisões já adoptadas)

| # | Tema | Escolha |
|---|------|---------|
| A | Orçamento FP vs prioridade FN | Perfil **`fn_priority`**: minimizar **FN** com teto **FP ≤ baseline_FP × fp_ratio_max** (default **2,0** → cap **456** com baseline FP 228) |
| B | Exploração sem treino | Matriz **S1–S7** + patches YAML por cenário (`strategy_matrix*.yaml`) |
| C | Calibração por camadas | **L1** grelha `similarity_threshold`; **L2** fallback + `ambiguity_band`; **L3** combo S3/S4; **L4** regras; **L5** gate motor opcional |
| D | Artefactos | Relatório JSON: `_local_samples/exports/hepatologia_diamond_bench/hepatologia_strategy_matrix.json` |

**Ficheiros-chave**

| Ficheiro | Papel |
|----------|--------|
| [`config.yaml`](../../../plataform/nlp_engine/configs/hepatologia/config.yaml) | Baseline motor |
| [`strategy_matrix.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix.yaml) | Matriz principal + `promotion` |
| [`strategy_matrix_calibration_layers.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix_calibration_layers.yaml) | Submatriz L1–L3 |
| [`run_hepatologia_diamond_bench.py`](../../../plataform/nlp_engine/scripts/run_hepatologia_diamond_bench.py) | Orquestra audit + compare + matriz |
| [`recompute_strategy_matrix_promotion.py`](../../../plataform/nlp_engine/scripts/recompute_strategy_matrix_promotion.py) | Recalcular vencedor sem reauditar |

---

## 4. Resultados registados (corrida de referência)

**Data / ambiente:** _preencher (ex.: 2026-04-30, máquina local, `fn_priority`)._

### 4.1 Tabela principal (2000 linhas, Camada 1 — grelha limiar)

| scenario_id | MR | FP | FN | ΔMR vs baseline | McNemar p&lt;0,05 |
|-------------|-----:|---:|---:|:----------------|:------------------|
| baseline | 0,515 | 228 | 742 | — | — |
| S5_hybrid_calibrated | **0,741** | 424 | **94** | +0,226 | sim |
| S5_hybrid_sim082 | 0,647 | 320 | 386 | +0,132 | sim |
| S5_hybrid_sim085 | 0,630 | 298 | 442 | +0,115 | sim |
| S5_hybrid_sim088 | 0,615 | 278 | 492 | +0,100 | sim |
| S5_hybrid_sim090 | 0,601 | 276 | 522 | +0,086 | sim |

### 4.2 Promoção (`fn_priority`)

| Campo | Valor |
|--------|------:|
| baseline_FP | 228 |
| baseline_FN | 742 |
| fp_cap_aplicado (2×) | 456 |
| **Vencedor** | **S5_hybrid_calibrated** |
| MR / FP / FN vencedor | 0,741 / 424 / 94 |

### 4.3 Leitura rápida

- Entre **sim082–sim090**, **sim082** tem melhor MR e menos FP que os outros `sim*`, mas **pior FN** que **S5_hybrid_calibrated**.
- **Nenhum** `S5_hybrid_sim*` supera **S5_hybrid_calibrated** em MR+FN no mesmo run.
- **âncora L2/L3:** preferir **S5_hybrid_calibrated** em vez de assumir **sim085** até nova evidência.

### 4.4 Camada 2 (fallback + `ambiguity_band`)

| scenario_id | MR | FP | FN | ΔMR vs baseline | McNemar p&lt;0,05 |
|-------------|-----:|---:|---:|:----------------|:------------------|
| baseline | 0,515 | 228 | 742 | — | — |
| S5_hybrid_calibrated | **0,741** | 424 | **94** | +0,226 | sim |
| S5_fallback_calibrated | 0,613 | 352 | 422 | +0,098 | sim |
| S5_fallback_band_narrow | 0,573 | 288 | 566 | +0,058 | sim |
| S5_fallback_sim085 | 0,573 | 288 | 566 | +0,058 | sim |

**Leitura L2**

- **Fallback piorou materialmente** vs `S5_hybrid_calibrated` em MR e FN.
- `S5_fallback_band_narrow` e `S5_fallback_sim085` ficaram **idênticos** nesta amostra.
- **Vencedor mantém-se:** `S5_hybrid_calibrated` (`fn_priority`, FP cap 456).

### 4.5 Camada 3 (combo estrutural S3/S4 sobre `sim085`)

| scenario_id | MR | FP | FN | ΔMR vs baseline | McNemar p&lt;0,05 |
|-------------|-----:|---:|---:|:----------------|:------------------|
| baseline | 0,515 | 228 | 742 | — | — |
| S5_hybrid_calibrated | **0,741** | 424 | **94** | +0,226 | sim |
| S5_hybrid_sim085 | 0,630 | 298 | 442 | +0,115 | sim |
| S5_hybrid_sim085_S3_260 | 0,630 | 304 | 436 | +0,115 | sim |
| S5_hybrid_sim085_S4_7 | 0,630 | 298 | 442 | +0,115 | sim |

**Leitura L3**

- Combos sobre `sim085` **não aumentaram MR**.
- `S3_260` reduziu FN (442 → 436), mas com aumento de FP (298 → 304) e sem ganho de MR.
- `S4_7` ficou igual a `sim085` nesta amostra.
- **Vencedor global mantém-se:** `S5_hybrid_calibrated` (`fn_priority`, FP cap 456).

### 4.6 Camada 4 (RPI) — deep-dive de FN do campeão

Fonte: `S5_hybrid_calibrated_audit.csv` vs `query_hepato_validate.csv` (`top-n=100`).

| Métrica | Valor |
|---------|------:|
| FN analisados | 94 |
| Exam bucket | TC 48, RM 20, US 20, OUTROS 6 |
| Cluster líder | `biliar_litiase` 50/94 |
| 2º cluster | `vias_biliares_colangio` 24/94 |

**Triage `biliar_litiase` (50 FN)**

| Classe | Qtde |
|--------|-----:|
| `noise_negated_stones` | 24 |
| `ambiguous_vesicula_only` | 20 |
| `likely_true_miss` | 6 |

**Leitura operacional**

- Prioridade imediata: reduzir FN de **negação de cálculo** (`noise_negated_stones`).
- Segunda frente: melhorar cobertura de **vesícula ambígua sem achado explícito**.
- `likely_true_miss` é menor; tratar depois das duas classes acima.

---

## 5. Metas (produto — fechar com clínica)

| Meta | Alvo | Estado |
|------|------|--------|
| MR | ≥ **0,80** | Não atingido (melhor bancada histórica 0,741; linha operacional após PR1: 0,737) |
| FP / FN | Limites por filtro humano + `fp_ratio_max` | Em definição |

---

## 6. Registo de corridas (copiar bloco por sprint)

| Data | Scope | Cenários | baseline MR/FP/FN | Melhor MR | Vencedor promoção | Commit / notas |
|------|-------|----------|-------------------|-----------|-------------------|----------------|
| 2026-04-30 | L1 grelha | baseline + S5_calibrated + sim082–090 | 0,515 / 228 / 742 | 0,741 | S5_hybrid_calibrated | run local |
| 2026-05-05 | L2 fallback/banda | baseline + S5_hybrid_calibrated + fallback_* | 0,515 / 228 / 742 | 0,741 | S5_hybrid_calibrated | fallback abaixo do campeão |
| 2026-05-05 | L3 combo S3/S4 | baseline + S5_hybrid_calibrated + sim085 + sim085_S3/S4 | 0,515 / 228 / 742 | 0,741 | S5_hybrid_calibrated | sem ganho de MR no combo |
| 2026-05-05 | L4 deep-dive FN | campeão `S5_hybrid_calibrated` (top-n 100 FN) | 0,515 / 228 / 742 | 0,741 | S5_hybrid_calibrated | biliar_litiase 50 FN (24 negated, 20 ambiguous, 6 true miss) |
| 2026-05-05 | PR1 validação (negação) | baseline + S5_hybrid_calibrated | 0,554 / 284 / 608 | 0,737 | S5_hybrid_calibrated | PR1: FN -8 (94→86), FP +16 (424→440), MR -0,004 (0,741→0,737) |
| 2026-05-05 | PR2 validação (vesícula ambígua) | baseline + S5_hybrid_calibrated | 0,579 / 304 / 538 | 0,728 | S5_hybrid_calibrated | PR2 revertido: MR/FP piores vs PR1; FN 78 vs 86 não compensa |
| 2026-05-06 | LLM piloto (4o-mini) | baseline + S5_hybrid_calibrated + S5_hybrid_calibrated_llm_pilot | 0,554 / 284 / 608 | 0,7525 | S5_hybrid_calibrated_llm_pilot | piloto LLM vencedor em `fn_priority`; FN 86→26 (+FP 440→469); sec 2109,59 |
| 2026-05-06 | LLM piloto + gold estrito (`only_cod_123`) | baseline + S5_hybrid_calibrated + S5_hybrid_calibrated_llm_pilot | 0,5904 / 58 / 608 | 0,9157 | S5_hybrid_calibrated_llm_pilot | LLM estável em 200/500/2000; `llm_called_rate` ~0,39; `llm_error_rate` 0,0; FP cap 116 (fp=111) |
| 2026-05-06 | LLM piloto + métricas `cod123` (validação curta) | baseline + S5_hybrid_calibrated + S5_hybrid_calibrated_llm_pilot (`max_rows=200`) | 0,554 / 5 / 57 | 0,9137 | S5_hybrid_calibrated_llm_pilot | `cod123`: n=139, acc=0,9137, rec_pos=0,9688, rec_neg=0,2727; `llm_called_rate=0,365`, `llm_error_rate=0,0` |
| 2026-05-08 | FP classe 3 (smoke 500) | baseline + S5 + `fp3_f2_negwindow_7` | 0,5769 / 16 / 160 | 0,8726 | S5_hybrid_calibrated | `fp3_f2_negwindow_7` rejeitado vs S5 (`0,8678 / 33 / 22`) |
| 2026-05-08 | FP classe 3 (smoke 500) | baseline + S5 + `fp3_f3_llm_band_narrow` | 0,5769 / 16 / 160 | 0,8918 | fp3_f3_llm_band_narrow | ganho vs S5 (`0,8918 / 28 / 17`), `llm_called_rate=0,354`, `llm_error_rate=0` |
| 2026-05-08 | FP classe 3 (smoke 500) | baseline + S5 + `fp3_f4_llm_neg_patterns` | 0,5769 / 16 / 160 | 0,8870 | fp3_f4_llm_neg_patterns | ganho vs S5 (`0,8870 / 27 / 20`), `llm_called_rate=0,39`, `llm_error_rate=0` |
| 2026-05-08 | FP classe 3 (full 2000, cod123) | baseline + S5 + `fp3_f4_llm_neg_patterns` | 0,5904 / 58 / 608 | 0,8887 | fp3_f4_llm_neg_patterns | run 1: `0,8887 / 98 / 83`; rerun: `0,8881 / 99 / 83`; decisão final candidato |
| 2026-05-08 | FP classe 3 (full 2000, cod123) | baseline + S5 + `fp3_f5_llm_band_narrow_neg_patterns` | 0,5904 / 58 / 608 | 0,8868 | F4 mantido vs combo | combo full + rerun; não superou F4 em MR/FN |
| | | | | | | |

---

## 7. Próximo passo operativo (uma linha)

**Protocolo FP classe 3:** encerrado em 2026-05-08 — candidato **`fp3_f4_llm_neg_patterns`** (detalhe na secção **Encerramento** no topo deste ficheiro).

**Próximo passo fora do bench:** aplicar o patch do cenário vencedor no fluxo de configuração/governança acordado pelo time.

**Nota de leitura do documento:** os blocos abaixo desta secção (8.x, PR1/PR2 etc.) são histórico de iterações anteriores e permanecem como referência.

**Piloto LLM no motor (executado):** cenário `S5_hybrid_calibrated_llm_pilot` em [`strategy_matrix_calibration_layers.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix_calibration_layers.yaml) com `base_url=https://api.openai.com/v1`, `model=gpt-4o-mini`, `api_key_env=OPENAI_API_KEY`.

**Resultado da corrida (2000 linhas, `fn_priority`):**
- baseline: `MR 0,5540 | FP 284 | FN 608 | 486,2s`
- `S5_hybrid_calibrated`: `MR 0,7370 | FP 440 | FN 86 | 1305,53s`
- `S5_hybrid_calibrated_llm_pilot`: `MR 0,7525 | FP 469 | FN 26 | 2109,59s` (**vencedor**)
- leitura: ganho material em FN (`86 -> 26`) e MR (`+0,0155` vs calibrado), com aumento de FP (`+29`) dentro do cap aplicado (`568`).

**Validação final — gold estrito (`--only-cod-123`, 2000 linhas):**
- baseline: `MR 0,5904 | FP 58 | FN 608 | 529,63s`
- `S5_hybrid_calibrated`: `MR 0,8659 | FP 132 | FN 86 | 1186,36s`
- `S5_hybrid_calibrated_llm_pilot`: `MR 0,9157 | FP 111 | FN 26 | 2359,61s` (**vencedor**)
- observabilidade do piloto: `llm_called_rate=0,391` e `llm_error_rate=0,0`.
- promoção (`fn_priority`): `baseline_fp=58`, `fp_cap_applied=116`, piloto dentro do cap (`fp=111`).

**Validação incremental (gold estrito):**
- `max_rows=200`: piloto `0,9209 / 9 / 2`, `llm_called_rate=0,365`, `llm_error_rate=0,0`.
- `max_rows=500`: piloto `0,9159 / 29 / 6`, `llm_called_rate=0,39`, `llm_error_rate=0,0`.
- leitura: tendência consistente de ganho vs `S5_hybrid_calibrated` antes do run de 2000.

**Observabilidade adicionada ao relatório de matriz (por cenário):**
- `uncertainty.global`: `mean/median/min/max` + `count_with_confidence`.
- `uncertainty.high_uncertainty_cases_top` (até 50).
- `uncertainty.low_uncertainty_cases_top` (até 10).
- `llm_observability`: `llm_called(_rate)`, `llm_error(_rate)`, distribuição de `decision_source`, `llm_model`, `llm_router_mode`.

## 8. Relatório executivo — base rotulada (1/2/3) e qualidade clínica

Escopo: corrida final com `--only-cod-123` (usa apenas gold rotulado por `cod_achado_relevante` iniciando em `1`, `2` ou `3`).

### 8.1 Composição da base rotulada (recorte 2000)

| Classe gold | Interpretação no comparador | Qtde |
|-------------|------------------------------|-----:|
| `1` | positivo (S) | 1026 |
| `2` | positivo (S) | 454 |
| `3` | negativo (N) | 146 |
| **Total rotulado (1/2/3)** | — | **1626** |

Derivados:
- Positivos no gold (`1+2`): **1480**
- Negativos no gold (`3`): **146**

### 8.2 Desempenho do motor na mesma base (1626, sem conhecer rótulo)

Para `S5_hybrid_calibrated_llm_pilot` (corrida final 2000, `fn_priority`, `only-cod-123`):

| Métrica | Valor |
|---------|------:|
| TP (`S` corretos) | 1454 |
| FN (`S->N`) | 26 |
| FP (`N->S`) | 111 |
| TN (`N` corretos) | 35 |
| Acurácia total | **91,57%** |
| Recall positivo (`TP/(TP+FN)`) | **98,24%** |
| Recall negativo / especificidade (`TN/(TN+FP)`) | **23,97%** |

Notas de cálculo:
- `TP = 1480 - 26 = 1454`
- `TN = 146 - 111 = 35`
- `Acurácia = (TP + TN) / 1626 = (1454 + 35) / 1626`

### 8.3 Leitura de qualidade (onde acerta e onde erra)

- O motor está **forte em positivos** (classes `1`/`2`): baixa perda de casos relevantes (FN baixo).
- O principal gap continua em **negativos puros (classe `3`)**: muitos são marcados como `S` (FP).
- Para o objetivo clínico atual (`fn_priority`), o piloto LLM é superior ao calibrado e ao baseline.
- Para cenário operacional com maior custo de overcall, é recomendável gate adicional por classe `3`.

### 8.4 Incerteza e observabilidade (corrida final)

`S5_hybrid_calibrated_llm_pilot`:
- `uncertainty.global`: `count=2000`, `mean=0,4328`, `max=0,9984`, `min=0,0`.
- Casos extremos disponíveis no JSON (`high_uncertainty_cases_top` e `low_uncertainty_cases_top`) com `id_exame`, `id_predicao`, `confidence_score`, `uncertainty_score`.
- `llm_called_rate=0,391`, `llm_error_rate=0,0`.
- `decision_source_distribution`: `hybrid_calibrated=1218`, `llm_router_llm_positive=713`, `llm_router_llm_negative=69`.

### 8.5 Conclusão executiva

- Em base rotulada estrita (1626), o piloto LLM atinge **91,57%** de acerto com **FN=26** (vs `86` no calibrado) e **FP=111** (vs `132` no calibrado).
- Há **ganho real** de qualidade para o perfil `fn_priority`.
- Próxima frente de melhoria: reduzir FP na classe `3` sem degradar recall dos positivos.

### 8.6 Comprovação estatística (subset pareado com chave comum)

Para teste estatístico pareado (McNemar) foi usado o subconjunto com chave comum entre os três audits e o gold no recorte de 2000, com `cod_achado_relevante` em `1/2/3` (**n=813**).

| Cenário | n | Acertos | Acurácia | IC95% (Wilson) |
|---------|--:|--------:|---------:|:---------------|
| baseline | 813 | 480 | 0,5904 | [0,5563; 0,6237] |
| S5_hybrid_calibrated | 813 | 704 | 0,8659 | [0,8408; 0,8876] |
| S5_hybrid_calibrated_llm_pilot | 813 | 745 | **0,9164** | **[0,8953; 0,9335]** |

**McNemar pareado (`S5_hybrid_calibrated` vs `S5_hybrid_calibrated_llm_pilot`):**
- `a_only=12` (calibrado acerta e piloto erra)
- `b_only=53` (piloto acerta e calibrado erra)
- `chi2=24,6154`  → **significativo a 5%** (`p < 0,05`)

Leitura: o ganho do piloto LLM sobre o calibrado não é só numérico; é estatisticamente significativo no subset pareado analisado.

### 8.7 Nova validação operacional (`cod123`) — formato de saída e leitura rápida

Corrida curta para validar telemetria e métricas por classe no `print-table`:

- comando: `run_hepatologia_diamond_bench.py --mode matrix --max-rows 200 --only-cod-123 --print-table ...`
- cenários: `baseline`, `S5_hybrid_calibrated`, `S5_hybrid_calibrated_llm_pilot`

**Resultados (MR/FP/FN):**
- baseline: `0,5540 / 5 / 57`
- calibrado: `0,8849 / 10 / 6`
- piloto LLM: `0,9137 / 8 / 4` (**vencedor**)

**Indicadores `cod123` no terminal (novo):**
- baseline: `n=139`, `acc=0,554`, `rec_pos=0,5547`, `rec_neg=0,5455`
- calibrado: `n=139`, `acc=0,8849`, `rec_pos=0,9531`, `rec_neg=0,0909`
- piloto LLM: `n=139`, `acc=0,9137`, `rec_pos=0,9688`, `rec_neg=0,2727`

**Observabilidade LLM (piloto):** `llm_called_rate=0,365`, `llm_error_rate=0,0`.

Template para próximas corridas (preencher ao fim):
- `max_rows=500`: `cod123 n=416`, `acc=0,8990`, `rec_pos=0,9574`, `rec_neg=0,35`
- `max_rows=2000`: `cod123 n=813`, `acc=0,8868`, `rec_pos=0,9405`, `rec_neg=0,3425`

### 8.8 Rerun operacional (2000, pós-fix da API key)

Comando executado:
- `run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 --promotion-profile fn_priority --print-table --only-cod-123 --scenarios-yaml configs/hepatologia/scenarios/strategy_matrix_calibration_layers.yaml --only-scenarios baseline,S5_hybrid_calibrated,S5_hybrid_calibrated_llm_pilot`

Resultado consolidado:
- baseline: `0,5904 / FP=58 / FN=608`
- `S5_hybrid_calibrated`: `0,8659 / FP=132 / FN=86`
- `S5_hybrid_calibrated_llm_pilot`: `0,8856 / FP=98 / FN=88`
- observabilidade piloto: `llm_called_rate=0,391`, `llm_error_rate=0,0`
- promoção (`fn_priority`): vencedor `S5_hybrid_calibrated_llm_pilot` (cap FP=116)

Leitura:
- após reconfigurar `OPENAI_API_KEY`, a camada LLM voltou a contribuir (não ficou mais empatada com o calibrado).
- ganho principal no rerun: melhora de acurácia global (`+0,0197`) e redução de FP vs calibrado.

**Sanidade de testes (antes da corrida):** `pytest tests/test_llm_router_backend.py tests/test_strategy_matrix_calibration_layers.py -q` -> `7 passed`.

**Config canónico (regras):** `config_version` **0.1.9** = PR1 mantido, PR2 revertido.

### PR1 aplicado (validado)

| Item | Ajuste |
|------|--------|
| Arquivo | `plataform/nlp_engine/configs/hepatologia/config.yaml` |
| `config_version` | `0.1.7-diamond-pack2b-antifp-v1-pr1-neg-window` |
| Negação | removido termo genérico `preservado` de `negation_phrases` |
| Janela de negação | `negation_window`: **7 → 5** |
| Hipótese | reduzir FN por sobre-negação em laudos longos (`noise_negated_stones`) |

**Resultado PR1 (2000 linhas)**

| Cenário | MR | FP | FN |
|--------|----:|---:|---:|
| baseline | 0,554 | 284 | 608 |
| S5_hybrid_calibrated | 0,737 | 440 | 86 |

**Delta vs estado anterior do campeão (antes PR1)**  
MR: `-0,004` | FP: `+16` | FN: `-8`

### PR2 (validado — **revertido no repo**)

| Item | Ajuste (histórico) |
|------|--------|
| Arquivo | `plataform/nlp_engine/configs/hepatologia/config.yaml` |
| `config_version` testado | `0.1.8-diamond-pack2b-antifp-v1-pr2-vesicula-ambiguous` |
| Escopo | bloco `colelitíase` (findings + regex) — **removido** na 0.1.9 |
| Hipótese | recuperar FN `ambiguous_vesicula_only` |

**Resultado PR2 (2000 linhas)**

| Cenário | MR | FP | FN |
|--------|----:|---:|---:|
| baseline | 0,579 | 304 | 538 |
| S5_hybrid_calibrated | 0,728 | 466 | 78 |

**Delta campeão PR2 vs PR1 (mesma bancada, mesmo run)**

| Métrica | PR1 campeão | PR2 campeão | Δ |
|---------|-------------|-------------|---|
| MR | 0,737 | 0,728 | **-0,009** |
| FP | 440 | 466 | **+26** |
| FN | 86 | 78 | **-8** |

**Decisão:** **reverter PR2** — ganho pequeno em FN com **custo alto** em FP e MR. Baseline também subiu (MR/FP/FN) porque o patch YAML afeta rule-only; o desvio global confirma excesso de positivos.

**Estado actual do ficheiro:** `config_version` **0.1.9-diamond-pack2b-antifp-v1-pr2-reverted-pr1-only** (só PR1 ativo).

*(Nota de execução: no YAML de camadas a Camada 3 está definida como `S5_hybrid_sim085_S3_260` e `S5_hybrid_sim085_S4_7` — por isso o run inclui também `S5_hybrid_sim085` para referência.)*
