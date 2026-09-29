# Calibração Hepatologia — gate de produto e camadas (v0)

Documento operacional para o plano **match ≥ 80%** com medição por camada. Dados sensíveis não entram aqui — só agregados.

## 1. Meta e referência

| Item | Valor |
|------|--------|
| **Meta principal** | `match_rate` ≥ **0,80** na bancada fixa (`query_hepato_validate.csv`, `max_rows=2000`). |
| **Baseline YAML** | [`configs/hepatologia/config.yaml`](../../../plataform/nlp_engine/configs/hepatologia/config.yaml) |
| **Cenários calibração** | [`strategy_matrix_calibration_layers.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix_calibration_layers.yaml) |

### Evidência de referência (2000 linhas, agregados)

Valores ilustrativos — substituir pela última corrida oficial.

| Ponto | MR | FP (N→S) | FN (S→N) | Notas |
|-------|-----|----------|----------|--------|
| baseline | 0,515 | 228 | 742 | Rule-only efectivo |
| S5_hybrid_calibrated | 0,741 | 424 | 94 | Orçamento FP típico perfil `fn_priority` |

## 2. Limites aceitáveis (fechar com clínica / produto)

- **FP:** perfil `fn_priority` usa `fp_ratio_max` (default **2,0** × baseline FP) em [`strategy_matrix.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix.yaml) — ajustar se o filtro humano mudar.
- **FN:** não regressar vs melhor ponto semântico sem PR + evidência (excepto trade explícito FP↔FN).
- **MR:** paragem quando **≥ 0,80** *e* FP/FN dentro dos limites acordados nesta secção.

Sem números nesta tabela, cada camada corre risco de “afinar à cabeça”.

## 3. Camadas e gates

| Camada | Conteúdo | Gate para avançar |
|--------|-----------|-------------------|
| **0** | Baseline congelado | MR/FP/FN registados |
| **1** | Grelha `similarity_threshold` sobre S5 hybrid | Escolher **campeão L1** (ficheiro YAML + linha na tabela abaixo) |
| **2** | `fallback` + `ambiguity_band` | Melhor ponto vs campeão L1 |
| **3** | Campeão L1/L2 + **um** knob S3 ou S4 | MR parcial + FP/FN dentro dos limites |
| **4** | `findings` / `findings_regex` (RPI) | Ver secção 5 |
| **5** | Gate motor (opcional) | Ver [`doc-gate-embedding-rule-min-v0.md`](../_fundacao/design/doc-gate-embedding-rule-min-v0.md) |

### Tabela de evidência (preencher a cada corrida)

| Data | Cenário | MR | FP | FN | vs baseline ΔMR | Notas |
|------|---------|-----|-----|-----|-------------------|--------|
| | | | | | | |

## 4. Comandos (submatriz — poucos cenários)

A partir de `plataform/nlp_engine`:

**Camada 1 (exemplo — ajustar IDs à grelha):**

```powershell
.\.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix `
  --max-rows 2000 --promotion-profile fn_priority --print-table `
  --scenarios-yaml configs\hepatologia\scenarios\strategy_matrix_calibration_layers.yaml `
  --only-scenarios baseline,S5_hybrid_calibrated,S5_hybrid_sim082,S5_hybrid_sim085,S5_hybrid_sim088,S5_hybrid_sim090
```

**Camada 2 (fallback + banda):**

```powershell
--only-scenarios baseline,S5_hybrid_calibrated,S5_fallback_calibrated,S5_fallback_band_narrow,S5_fallback_sim085
```

**Camada 3 (combo — campeão provisório `sim085`; trocar pelo ID real após L1):**

```powershell
--only-scenarios baseline,S5_hybrid_sim085,S5_hybrid_sim085_S3_260,S5_hybrid_sim085_S4_7
```

Saída: `_local_samples/exports/hepatologia_diamond_bench/hepatologia_strategy_matrix.json`.

## 5. Camada 4 — regras (RPI)

- Inventário notebook legado vs [`configs/hepatologia/config.yaml`](../../../plataform/nlp_engine/configs/hepatologia/config.yaml) (`findings`, `findings_regex`).
- **Uma alteração clínica por PR**; rerodada audit+compare 2000.
- Ver matriz de gaps: [`s06-hepatologia-matriz-mapeamento-v0.md`](s06-hepatologia-matriz-mapeamento-v0.md).

## 6. Camada 5 — motor

Se MR ≥ 0,80 **não** for atingível só com YAML dentro dos limites: SPEC em [`doc-gate-embedding-rule-min-v0.md`](../_fundacao/design/doc-gate-embedding-rule-min-v0.md) + task no board.
