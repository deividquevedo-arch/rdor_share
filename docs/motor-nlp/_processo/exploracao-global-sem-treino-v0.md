# Exploração assertiva sem treino — defaults globais vs Hepatologia

Objetivo: evoluir o motor **só com YAML + comportamento genérico** (`engine.py`, `rule_engine.py`, `semantic_expand.py`), sem calibrador supervisionado por especialidade. Hepatologia é **bancada de medição**; o que promove deve ser replicável noutras especialidades via `configs/<especialidade>/config.yaml`.

## Onde está a matriz

| Artefacto | Caminho |
|-----------|---------|
| Catálogo S1–S7, regras de promoção, cenários YAML | `plataform/nlp_engine/configs/hepatologia/scenarios/strategy_matrix.yaml` |
| Runner (audit + compare + McNemar + bootstrap) | `plataform/nlp_engine/scripts/run_hepatologia_strategy_matrix.py` |
| Orquestrador bancada `_local_samples/standard/hepatologia` | `plataform/nlp_engine/scripts/run_hepatologia_diamond_bench.py` |
| Relatório JSON (exemplo fixture 5 linhas) | `plataform/nlp_engine/tests/fixtures/hepatologia_strategy_matrix_report_e2e.json` |

## Defaults globais vs overrides

- **Globais (partilhados):** `plataform/nlp_engine/configs/shared/organs.yaml` — universo de órgãos; o motor não hardcoda órgãos nem keywords clínicos fora do que o YAML injeta.
- **Por especialidade:** ficheiro `configs/<specialty_id>/config.yaml` com `specialty_id`, `config_version`, bloco `nlp` (findings, negação, embeddings, segmentação, `score_policy_version`, `finding_organ_max_chars`, etc.).
- **S1/S2 (incerteza / cascata):** reservados no catálogo da matriz; ainda não são cenários YAML-only — exigem `feature_flags` e lógica no motor quando alinhado em backlog.
- **Calibração leve híbrida:** `nlp.feature_flags.calibrated_hybrid: true` só deve ser ligado quando `embeddings.use_embeddings` e modo híbrido estiverem alinhados ao contrato da especialidade (ver `engine.py`).

## Promoção (teto FP + match)

Critérios no `strategy_matrix.yaml` → `matrix_spec.promotion`:

1. `match_rate` ≥ baseline.
2. `legacy_N_motor_S` (FP motor vs gold) ≤ baseline.

O runner escolhe `winner_under_promotion_rules` entre cenários não baseline que cumpram ambos; desempate: maior `match_rate`, menos FP, menos FN.

**Perfis de promoção** (em `strategy_matrix.yaml` → `matrix_spec.promotion` ou CLI `--promotion-profile`):

- `fp_ceiling` (default): `match_rate` ≥ baseline e FP ≤ baseline_FP.
- `fn_priority`: **melhoria real** vs baseline = FN estritamente menor, `match_rate` ≥ baseline, FP ≤ `baseline_FP * fp_ratio_max` (default 2.0); vencedor = **menor FN**, depois maior MR, depois menor FP. Adequado quando o filtro de FP é humano e o custo principal é **S perdido** (FN).

Exemplo: `run_hepatologia_diamond_bench.py --mode matrix --promotion-profile fn_priority --print-table`

## Bancada Diamond 2000/2000 (gerar + medir + comparar ao legado)

Os CSVs reais ficam em `_local_samples/` (gitignored).

### 0) Amostra única `query_hepato_validate.csv` (fluxo que já usam)

Export tipo query Diamond com **o mesmo ficheiro** para audit e para gold: colunas relevantes incluem `proced_laudo_exame` (texto do laudo), `id_predicao`, `id_exame`, `cod_achado_relevante` (gold médico: primeiro carácter 1/2 → S, 3 → N; ver `audit_legacy_compare.legacy_s_n_from_row`).

Coloque o ficheiro em `plataform/nlp_engine/_local_samples/diamond/query_hepato_validate.csv` (ou defina `NLP_HEPATO_QUERY_VALIDATE` com caminho absoluto). O script `run_hepatologia_diamond_bench.py` **usa automaticamente** esse CSV para input e legado quando não passa `--input-csv` / `--legacy-csv` e a amostra standard (passo 1) ainda não existe. Para forçar sempre este ficheiro: `--from-query-validate`.

```text
cd plataform\nlp_engine
.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode baseline --max-rows 2000
```

### 1) Montar `hepatologia_standard_{input,expected}.csv` (2000 linhas estratificadas) — alternativa

A partir do export lake tipo `tb_diamond_mod_hepatologia_saida.csv` (ajuste o caminho ao teu ficheiro):

```text
cd plataform\nlp_engine
.venv\Scripts\python.exe scripts\build_hepatologia_standard_sample.py ^
  --source diamond ^
  --legacy-csv _local_samples\diamond\tb_diamond_mod_hepatologia_saida.csv ^
  --out-dir _local_samples\standard\hepatologia ^
  --max-positive 1000 --max-negative 1000
```

Isto gera **1000 S + 1000 N** (ou menos se o ficheiro não tiver tantas linhas de cada classe). Para usar **todas** as linhas disponíveis com recorte proporcional, use `--max-rows 2000` em vez de `--max-positive` / `--max-negative`.

### 2) Orquestrador: baseline rápido ou matriz completa

Script: `scripts/run_hepatologia_diamond_bench.py`. Resolução de caminhos: **passo 0** (`query_hepato_validate.csv`) tem prioridade sobre o passo 1 se não indicar `--input-csv`/`--legacy-csv` explícitos.

**Só baseline** (config atual `configs/hepatologia/config.yaml` vs legado): `match_rate`, matriz de confusão, `n_joined`.

```text
.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode baseline --max-rows 2000
```

**Matriz de cenários** (mesmo pipeline que `run_hepatologia_strategy_matrix.py`), com tabela-resumo no ecrã e tempos por cenário no JSON:

```text
.venv\Scripts\python.exe scripts\run_hepatologia_diamond_bench.py --mode matrix --max-rows 2000 --print-table
```

**Reduzir custo** (só baseline + um cenário, por exemplo híbrido calibrado):

```text
.venv\Scripts\python.exe scripts\run_hepatologia_strategy_matrix.py ^
  --input-csv _local_samples\standard\hepatologia\hepatologia_standard_input.csv ^
  --legacy-csv _local_samples\standard\hepatologia\hepatologia_standard_expected.csv ^
  --max-rows 2000 --only-scenarios baseline,S5_hybrid_calibrated --print-table
```

CSV noutro sítio: passe `--input-csv` e `--legacy-csv` ao `run_hepatologia_diamond_bench.py` ou ao `run_hepatologia_strategy_matrix.py`. O compare usa `id_predicao` quando existir; caso contrário faz fallback por `id_exame` (ver `audit_legacy_compare.py`).

Saída típica da matriz: `generated_configs/*.yaml`, `*_audit.csv`, `*_compare.json`, agregado `hepatologia_strategy_matrix.json` (McNemar baseline vs cada cenário, IC bootstrap do `match_rate`, distribuições legado/motor, `elapsed_seconds`).

Cenários **S5** com embeddings ligados podem carregar pesos do Hugging Face na primeira execução (aviso de token não autenticado é esperável em dev).

## Fixture E2E (sem PHI)

O ficheiro `hepatologia_strategy_matrix_report_e2e.json` reflete a matriz completa sobre `tests/fixtures/hepatologia_e2e_*.csv` (amostra sintética). Serve para regressão de **formato** do relatório; decisões de produção devem usar a bancada Diamond completa.

## Pacote vencedor (Hepatologia)

Até correr a matriz nos 2000 casos reais, **não alterar** `configs/hepatologia/config.yaml` só com base na fixture. Quando o JSON da bancada indicar um vencedor estável sob teto FP:

1. Copiar apenas os `nlp.*` relevantes do YAML gerado em `out_dir/generated_configs/<winner>.yaml` para o `config.yaml` oficial.
2. Bump de `config_version` e registo no PR/commit.

Na fixture atual, o runner apontou `S5_hybrid_calibrated` como vencedor (empate perfeito com baseline neste conjunto minúsculo).
