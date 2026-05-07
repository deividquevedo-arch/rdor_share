# S06 — Validação motor Hepatologia (Grupo 1a) (v0)

**Objetivo:** runtime YAML **Hepatologia** alinhada a `configs/shared/organs.yaml` (órgão `figado`) e cadeia de validação replicável no mesmo padrão do piloto Pulmão: amostra local → `audit` → `compare` opcional → registo de evidência (agregados).

**Backlog:** base de ficheiro e validação encaixa em **S03 T03.3** (extrair CONFIG do notebook para YAML); a paridade 1:1 com legado 1a exige RPI com o notebook e leitura da amostra Diamond.

## Configuração

- `plataform/nlp_engine/configs/shared/organs.yaml` — `organs.figado` (seeds + `header_aliases`).
- `plataform/nlp_engine/configs/hepatologia/config.yaml` — `specialty_id: hepatologia`, `target_organs: [figado]`, `config_version` (p.ex. `0.1.0-piloto`).

## Geração da amostra padrão (input + expected)

**Default (paridade com o lake):** export **gold Hive** em `_local_samples/gold/hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv` (delimitador `,`, colunas `idExame`, `idPaciente`, `laudoExame`, `flgRelevante` TRUE/FALSE, …). O script mapeia para o mesmo par `hepatologia_standard_{input,expected}.csv` que o audit/compare já usam; o legado na comparação e o `flgRelevante` (convertido para S/N).

**Formato alternativo (Diamond):** mesmas colunas que Pulmão — usar `--source diamond` e apontar `--legacy-csv` para o CSV Diamond.

```powershell
Set-Location -LiteralPath '...\Projects\plataform\nlp_engine'
# gold Hive (default: --source gold_hive, ficheiro acima)
.venv\Scripts\python.exe scripts\build_hepatologia_standard_sample.py
# explicito
.venv\Scripts\python.exe scripts\build_hepatologia_standard_sample.py --source gold_hive --legacy-csv _local_samples\gold\hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv
# Diamond
.venv\Scripts\python.exe scripts\build_hepatologia_standard_sample.py --source diamond --legacy-csv <path_saida_diamond.csv>
```

Opcional: `--out-dir _local_samples\standard\hepatologia`, `--max-rows 60` (0 = todas as linhas). Nao versionar o CSV gold (PHI); evidencia = agregados abaixo.

Ficheiros gerados:

- `hepatologia_standard_input.csv`
- `hepatologia_standard_expected.csv`

Regra de chave `id_exame` (igual Pulmão): ver nota [s06b-amostra-padrao-pulmao-v0.md](s06b-amostra-padrao-pulmao-v0.md).

## Cadeia: audit + compare

```powershell
.venv\Scripts\python.exe scripts\audit_engine_from_csv.py --csv _local_samples\standard\hepatologia\hepatologia_standard_input.csv -o _local_samples\exports\hepatologia_standard_audit.csv --config-yaml configs\hepatologia\config.yaml --validate-output-invariants

.venv\Scripts\python.exe scripts\compare_hepatologia_audit_vs_legacy.py --audit-csv _local_samples\exports\hepatologia_standard_audit.csv --legacy-csv _local_samples\standard\hepatologia\hepatologia_standard_expected.csv --out-json _local_samples\exports\hepatologia_compare_matrix.json
```

Comparação **genérica** (reutilizável): `plataform/nlp_engine/scripts/audit_legacy_compare.py` (`read_rows_semico_first`, `run_compare_audit_vs_legacy`, `legacy_s_n_from_row` com `expected_encaminhamento` ou `flgRelevante`). Pulmão continua a expor `compare_pulmao_audit_vs_legacy.py` com a mesma lógica.

**Leitura CSV:** prioridade `;` (evita `Sniffer` com `,` quando há JSON com vírgulas em `exm_laudo_resultado`); ver testes em `tests/test_hepatologia_compare_and_sample.py` e `tests/test_pulmao_compare_and_sample.py`.

## Testes (pytest, sem PHI)

- `tests/test_hepatologia_runtime_config.py` — merge YAML + `organs`
- `tests/test_hepatologia_compare_and_sample.py` — build + compare
- `tests/integration/test_hepatologia_runtime_integration.py` — frases sintéticas + invariants
- **E2E audit + comparação (como Pulmão no pipeline manual):** `tests/integration/test_hepatologia_e2e_audit_compare.py` — lê `tests/fixtures/hepatologia_e2e_{input,expected}.csv`, corre `run_audit` com `configs/hepatologia/config.yaml` + invariants, depois `run_compare_audit_vs_legacy`; exige `n_joined=5` e `n_mismatch=0` (frases e gold alinhados ao `fl_relevante` actual do motor).
- **Gold Hive sintético (CI):** `tests/integration/test_hepatologia_hive_gold_synthetic_e2e.py` — gera CSV mínimo no estilo Hive, `run_build` com `source=gold_hive`, audit e compare (sem dados reais).

## Evidência — tabela (preencher após rodada local)

Apenas agregados; sem trechos de laudo nem identificadores reais em partilha pública.

| Data | `config_version` | `n_joined` | `n_match` | `n_mismatch` | `match_rate` | TP (S/S) | FN (S/N) | FP (N/S) | TN (N/N) | Notas |
|------|------------------|------------|-----------|--------------|-------------|----------|----------|----------|----------|-------|
| 2026-04-23 | `0.1.0-piloto` | 100 | 91 | 9 | 0.91 | 0 | 0 | 9 | 91 | Gold Hive 100 linhas: legado **N** em todas; motor **S**=9, **N**=91. Matriz: só **FP** (N→S) e **TN** (N→N); ver `hepatologia_compare_matrix.json` local. |
| 2026-04-23 | `0.1.0-piloto` (baseline Diamond) | 100 | 89 | 11 | 0.89 | 1 | 1 | 10 | 88 | Diamond (`tb_diamond_mod_hepatologia_saida.csv`) com legado misto (`N`=98, `S`=2). Baseline oficial para iteracoes de ajuste. |

### Detalhe da rerodada (2026-04-23)

- Fonte: `_local_samples/gold/hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv` → `build` (`gold_hive`) → `audit` (`engine_version` default `audit-local`) → `compare`.
- `legacy_distribution`: N=100 (neste export, `flgRelevante` passou todo a **N** no mapeamento).
- `motor_distribution`: S=9, N=91.
- **Interpretação:** os 9 mismatches são **falsos positivos** do motor face ao gold (motor marcou relevante onde o legado marcou não relevante). Revisão clínica / YAML (**S03 T03.3**) para decidir se o motor ou o rótulo gold prevalece caso a caso.

### Baseline Diamond (2026-04-23)

- Fonte: `_local_samples/diamond/tb_diamond_mod_hepatologia_saida.csv` com mapeamento `fl_relevante` (`true/false`, `1/0`) -> `expected_encaminhamento` (`S/N`).
- Compare baseline: `n_joined=100`, `n_match=89`, `n_mismatch=11`, `match_rate=0.89`.
- Matriz baseline: TP=1, FN=1, FP=10, TN=88.
- Leitura baseline: principal gap = FP de lexico/regra (10 casos); 1 FN para analise semantica/contextual.

## Gates de aceite (A/B/C)

- **Gate A (quantitativo minimo):** nao piorar baseline e reduzir erro total (FP+FN) ou manter com justificativa clinica.
- **Gate B (incerteza legado):** divergencias com evidencias de possivel FP/FN do legado nao contam regressao silenciosa.
- **Gate C (governanca):** toda divergencia relevante recebe decisao explicita (`aceite`, `pendente clinico`, `reverter`).

Matriz tecnica detalhada (versao/config/metodos/gaps): `docs/motor-nlp/notas/s06-hepatologia-matriz-mapeamento-v0.md`.

## Riscos (ver plano)

- O export **gold Hive** ja traz o schema; o export **Diamond** exige mapear colunas ao copiar. Paridade **motor vs legado (gold)** nao e garantida na primeira corrida: o `compare` mede o gap; iterar o YAML (**S03 T03.3**).
- Não prometer paridade 1:1 com o modelo no lake se o `ClinicalNlpEngine` Fase 1 divergir do pipeline que rotulou o gold.
