# Baseline congelado — tuning FP classe 3 (referência motor)

**Objetivo:** linha de base explícita antes de iterar knobs YAML (plano FP classe 3).  
**Escopo:** hepatologia como referência; o **protocolo** é agnóstico — mesmos tipos de cenário aplicam-se a outras especialidades.

**Estado (2026-05-08):** protocolo FP classe 3 **encerrado**. Candidato vencedor na bancada: **`fp3_f4_llm_neg_patterns`** (`strategy_matrix_fp_class3_tuning.yaml`), full 2000 + rerun documentados no tracker.

## Pré-condições de medição

- Mesmo CSV de entrada e mesmo `--only-cod-123` quando o protocolo for gold estrito 1/2/3.
- `OPENAI_API_KEY` (ou variável em `nlp.llm_router.api_key_env`) definida **na sessão** do terminal que corre o bench.
- Critério de validade LLM: `llm_error_rate = 0` no cenário piloto.

## Congelamento de referência (última validação estável documentada)

Valores de referência para **comparar regressões** em iterções futuras (recorte `--max-rows 2000`, `--only-cod-123`, `strategy_matrix_calibration_layers.yaml`, `fn_priority`):

| Cenário | match_rate | FP | FN | Notas |
|---------|------------|----|----|--------|
| baseline | 0,5904 | 58 | 608 | |
| S5_hybrid_calibrated | 0,8659 | 132 | 86 | embeddings hybrid + calibrated_hybrid |
| S5_hybrid_calibrated_llm_pilot | 0,8856–0,8862 | 97–98 | 88 | variar ligeiramente entre reruns; usar JSON da corrida como verdade |

**Protocolo FP3 — candidato vencedor (`fp3_f4_llm_neg_patterns`), mesmo recorte:**

| Run | match_rate | FP | FN | Notas |
|-----|------------|----|----|--------|
| full run 1 | 0,8887 | 98 | 83 | usar JSON da corrida como verdade |
| full rerun | 0,8881 | 99 | 83 | variacao pequena (MR/FP) |

**Subset cod123 (n=813):** métricas agregadas no print-table (`acc`, `rec_pos`, `rec_neg`) — comparar sempre com o último JSON aceite.

**Corrida só baseline + piloto LLM** (validação chave API): `match_rate` piloto ~0,8862, `llm_called_rate` ~0,391, `llm_error_rate` **0**.

## Comando canónico (matriz completa)

```text
.\.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 --promotion-profile fn_priority --print-table --only-cod-123 --scenarios-yaml configs\hepatologia\scenarios\strategy_matrix_calibration_layers.yaml --only-scenarios baseline,S5_hybrid_calibrated,S5_hybrid_calibrated_llm_pilot --out-json _local_samples\exports\hepatologia_diamond_bench\hepatologia_strategy_matrix_<tag>.json
```

## Comando cenários FP classe 3 (protocolo dedicado)

Ver [`strategy_matrix_fp_class3_tuning.yaml`](../../../plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix_fp_class3_tuning.yaml).

Smoke rápido: `--max-rows 500` com o mesmo `--only-scenarios` restrito ao cenário em teste + baseline.

## Gates (resumo)

- **G1 Smoke:** sem queda relevante de `acc` cod123 vs último aceite **ou** FP classe 3 melhora sem explosão de FN em 1/2.
- **G2 Full:** critério acima em `max_rows` alvo + McNemar quando aplicável.
- **Rollback:** `match_rate` abaixo do aceite; FN 1/2 acima do limiar acordado; `llm_error_rate > 0` no piloto LLM.

## Artefactos

- Guardar sempre `--out-json` com sufixo datado ou tag (`_rerun`, `_fp3_iter1`, etc.).
- Não sobrescrever JSON de referência sem renomear.

## Checklist — protocolo FP classe 3 (encerrado 2026-05-08)

- [x] Fases F1–F5 exercitadas conforme matriz (smoke e/ou full conforme gate).
- [x] Finalistas F3 vs F4 confrontados no full 2000.
- [x] Combo F5 validado (full + rerun); decisão: **manter F4** sobre F5.
- [x] Rerun estabilidade no candidato **F4** (full 2000).
- [x] Decisão final documentada no tracker de bancada.

**Candidato vencedor:** `fp3_f4_llm_neg_patterns`.
