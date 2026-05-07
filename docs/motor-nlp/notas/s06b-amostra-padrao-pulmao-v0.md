# S06b — Amostra padrao Pulmao para validacao local (v0)

**Objetivo:** gerar um par canônico `input` + `expected` a partir dos CSVs legados disponiveis, permitindo validar comportamento do motor no mesmo conjunto.

## Regra de construcao

- Fonte de verdade local: `dev_tb_diamond_mod_pulmao_saida.csv` (contendo texto do laudo + classificacao legado).
- Arquivos gerados:
  - `pulmao_standard_input.csv` (entrada para o motor);
  - `pulmao_standard_expected.csv` (saida esperada para comparacao).
- Chave canonica (join motor vs `pulmao_standard_expected.csv`):
  - `id_exame = id_pct` (fallback `exm_an` quando nao houver `id_pct`);
  - se o mesmo `id_pct` repetir no CSV fonte, `id_exame = {id_pct}__{exm_an}__{i}` (indice de linha) para evitar colisao no pareamento (audit e compare usam `id_exame`).

## O que deve / nao deve acontecer

- `expected_encaminhamento = S` -> `expected_fl_relevante = 1` e `expected_behavior = deve_acontecer`.
- `expected_encaminhamento = N` -> `expected_fl_relevante = 0` e `expected_behavior = nao_deve_acontecer`.

## Comando de geracao

```powershell
Set-Location -LiteralPath '...\Projects\plataform\nlp_engine'
.venv\Scripts\python.exe scripts\build_pulmao_standard_sample.py
```

Opcional para recorte:

```powershell
.venv\Scripts\python.exe scripts\build_pulmao_standard_sample.py --max-rows 60
```

## Decisao registrada

Como `dev_tb_diamond_mod_pulmao.csv` e `dev_tb_diamond_mod_pulmao_saida.csv` nao tem intersecao por `id_pct` no recorte atual, a amostra padrao de validacao passa a ser derivada da saida legado (que contem o texto de entrada efetiva e a classificacao esperada).

## Delta matrix (S06b T06.6 / T06.7) -- ferramentas e paridade

- `scripts/compare_pulmao_audit_vs_legacy.py` -- confusao `legacy S/N` vs `motor fl_relevante` (S=1, N=0). Legacy: `id_exame` (ou `id_pct` se nao houver `id_exame`), `exm_encaminhamento_nlp` ou `expected_encaminhamento`. **Leitura de CSV:** prioriza `;` (export do `audit`); o `csv.Sniffer` sozinho pode escolher `,` por causa de virgulas *dentro* do JSON em `exm_laudo_resultado`, o que partia as colunas e deixava `id_exame` vazio (`bloqueado_sem_chave_comum` com `audit_key_examples: []`); ver teste `test_read_pulmao_csv_semico_depois_de_json_com_virgulas` em `tests/test_pulmao_compare_and_sample.py`.
- `scripts/bucket_pulmao_mismatches.py` -- classifica so linhas *mismatch* (heuristicas: negacao, escopo abdome, lexico/ancora); opcional `--input-csv` com texto.
- Regerar `pulmao_standard_*.csv` se mudar o script de amostra; `config_version` Pulmao ver `configs/pulmao/config.yaml` (ex. `0.1.1-piloto` com expansao de `findings` / `findings_regex` e seeds de `organs.pulmao` para anatomia toracica e termos de laudo).

**Comandos (a partir de `plataform/nlp_engine`, com o mesmo venv usado no pytest):**

```powershell
.venv\Scripts\python.exe scripts\audit_engine_from_csv.py --csv _local_samples\standard\pulmao\pulmao_standard_input.csv -o _local_samples\exports\pulmao_standard_audit.csv --config-yaml configs\pulmao\config.yaml --validate-output-invariants
.venv\Scripts\python.exe scripts\compare_pulmao_audit_vs_legacy.py --audit-csv _local_samples\exports\pulmao_standard_audit.csv --legacy-csv _local_samples\standard\pulmao\pulmao_standard_expected.csv --out-json _local_samples\exports\pulmao_compare_matrix.json
.venv\Scripts\python.exe scripts\bucket_pulmao_mismatches.py --audit-csv _local_samples\exports\pulmao_standard_audit.csv --legacy-csv _local_samples\standard\pulmao\pulmao_standard_expected.csv --input-csv _local_samples\standard\pulmao\pulmao_standard_input.csv -o _local_samples\exports\pulmao_mismatch_buckets.csv
```

**Gaps motor vs legado (decisao de produto / fase) -- nao conflitam com o YAML Fase 1 se explicitados:**

| ID | Situacao | O que decidir |
|----|----------|----------------|
| G1 | Taxonomia legado (`exm_class` A, A01, …) vs motor so por keywords/regex | Paridade de classe exige regras mapeando classe ou fica fora do escopo Fase 1. |
| G2 | Ancora por frase: laudo toracico sem seed ainda (apos expansao, casos resolviveis com YAML) | Muito generico: feature em codigo (p.ex. "inferir torax" na especialidade) ou S09. |
| G3 | Positivo legado sem overlap lexical (embeddings / semantica no pipeline antigo) | Marcar dependencia S09; nao e regressao nao explicada da regra. |
| G4 | Uma linha: gold positivo por outro orgao/segmento do exame (multi-tema) | Escopo Pulmao-only vs "igual ao export do legado"; sign-off (S08) se for espelhar legado. |

## Evidencia — matriz

Apos `build` (se aplicavel) + `audit` + `compare` + `bucket`, registar **apenas agregados** (sem trechos de laudo nem ids reais em copia-paste publica).

Com a correcao de leitura `;` vs JSON, o `n_joined` deve coincidir com o numero de linhas do input (100 na amostra padrao actual) quando as chaves `id_exame` alinham.

O script `bucket_pulmao_mismatches.py` deve ser executado a partir de `plataform/nlp_engine` (o script adiciona a raiz em `sys.path` para importar `scripts.*` quando se corre `python scripts\…`).

### Rerodada local (2026-04-23)

- `config_version`: `0.1.1-piloto` (`configs/pulmao/config.yaml`); `engine_version` no audit: default `audit-local`.
- Amostra: 100 linhas; encaminhamento legado na amostra: S=67, N=33.
- Compare (`pulmao_compare_matrix.json` / stdout): `n_joined=100`, `n_match=33`, `n_mismatch=67`, `match_rate=0.33`.
- Distribuicao motor (S = `fl_relevante=1`): S=4, N=96.
- Matriz (legado S/N x motor S/N): TP `legacy_S_motor_S`=2, FN `legacy_S_motor_N`=65, FP `legacy_N_motor_S`=2, TN `legacy_N_motor_N`=31.
- Buckets heuristicos (67 mismatches): `contexto_comparativo_sem_achado_lexical_obvio`=44, `lexico_pulmao_ou_gate_regra`=20, `escopo_extra_pulmao_suspeita_abdomen_ou_pelve`=1, `fp_outro`=2. (Saida: `mismatches=67 by_bucket={...}`.)

### Modelo para proximas rerodadas

- Data e `config_version`; `n_joined`, `n_match`, `n_mismatch`, `match_rate`.
- `confusion_matrix_legacy_vs_motor` (TP / FN / FP / TN como acima).
- Resumo do bucket.

**Deep-dive (Grupo 1b vs motor Fase 1, RPI, paridade A/B/C, S09):** ver [`s06b-deep-dive-pulmao-v0.md`](s06b-deep-dive-pulmao-v0.md).
