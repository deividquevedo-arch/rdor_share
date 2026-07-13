# S01/S02 — Paridade e auditoria (evidencia v0)

**Objetivo:** comandos reprodutiveis e registo de resultados para validar o motor vs proxy de legado (HML `laudo_tratado`) e para auditar saida do engine em lote.

**Contrato motor rule-based (entrada / `nlp` / saida):** [doc-contrato-engine-rule-based-v0.md](../doc-contrato-engine-rule-based-v0.md). Gate pos-process: `audit_engine_from_csv.py --validate-output-invariants` / `validate_local_samples.py --validate-output-invariants`.

**Sem PHI no Git:** CSVs reais apenas em [`plataform/nlp_engine/_local_samples/`](../../plataform/nlp_engine/) (gitignored).

---

## 1) S01 — Paridade `to_plain(Laudo)` vs `laudo_tratado`

### Opcao A — pytest (mesmo codigo que CI local opcional)

A partir de `plataform/nlp_engine` (ver [README da lib](../../plataform/nlp_engine/README.md)):

```powershell
$env:NLP_HML_LAUDOS_CSV = (Resolve-Path "tests\fixtures\hml_parity_minimal.csv").Path
.venv\Scripts\python.exe -m pytest tests\local\test_hml_sample_to_plain_parity.py::test_hml_sample_to_plain_parity_vs_laudo_tratado -v
```

Relatorio sem falhar o job (taxa no stdout):

```powershell
$env:NLP_HML_LAUDOS_CSV = (Resolve-Path "_local_samples\diamond\SEU_EXPORT.csv").Path
$env:NLP_HML_PARITY_REPORT_ONLY = "1"
$env:NLP_HML_MAX_ROWS = "500"
.venv\Scripts\python.exe -m pytest tests\local\test_hml_sample_to_plain_parity.py::test_hml_sample_to_plain_parity_legacy_report -v -s
```

### Opcao B — script autonomo

```powershell
Set-Location -LiteralPath '...\Projects\plataform\nlp_engine'
.venv\Scripts\python.exe scripts\report_to_plain_parity.py --csv tests\fixtures\hml_parity_minimal.csv
```

### Evidencia registada (fixture versionada)

| Ficheiro | Linhas comparadas | Taxa match | Notas |
|----------|-------------------|------------|--------|
| `tests/fixtures/hml_parity_minimal.csv` | 2 | **1.0000** | `Laudo` e `laudo_tratado` identicos por linha; `to_plain` preserva texto plain; nenhum mismatch esperado. |

**Smoke local (confirmado):** `report_to_plain_parity.py` e `audit_engine_from_csv.py` na mesma fixture — 2 comparadas / 2 match; auditoria escrita em `_local_samples/exports/audit_hml_minimal.csv` (2 linhas).

Preencher tabela abaixo apos correr amostras em `_local_samples/`:

| Ficheiro (local) | Data | Linhas | Taxa | Causas top divergencia |
|------------------|------|--------|------|-------------------------|
| `diamond/dev_tb_diamond_mod_colon_entrada.csv` | 2026-04-15 | 100 | **1.0000** (`--parity-relaxed`) | Sem divergencia apos normalizacao RTF de aviso WebRIS + `\\-` (inicio/inline) so na comparacao. |

**Validacao focal T01.6 (engine/config):**

- `pytest tests/test_engine_process.py tests/test_config_loader.py` -> **12 passed**.

Classificacao sugerida de causas: Pandoc/RTF, rodape/boilerplate, normalizacao Unicode/espacos, coluna gold desatualizada vs pipeline atual.

---

## 2) S02 — Auditoria `ClinicalNlpEngine.process` em CSV

Requer `pip install -e ".[dev]"` (PyYAML). A partir de `plataform/nlp_engine`:

**Config demo (bexiga / calculos — alinhada ao `demo_engine_e2e` modo CSV):**

```powershell
.venv\Scripts\python.exe scripts\audit_engine_from_csv.py --csv tests\fixtures\hml_parity_minimal.csv -o _local_samples\exports\audit_hml_minimal.csv
```

**Saida smoke:** `_local_samples/exports/audit_hml_minimal.csv` (gitignored) — 2 linhas; usar para revisao ou join a gold.

**Com YAML de especialidade** (merge de `shared_organs_path` se o ficheiro existir):

```powershell
.venv\Scripts\python.exe scripts\audit_engine_from_csv.py --csv _local_samples\diamond\entrada.csv -o _local_samples\exports\audit_colon.csv --config-yaml "CAMINHO\para\config.yaml"
```

Ajustar caminhos conforme a maquina. O CSV de saida inclui `exm_laudo_texto_tratado`, `fl_relevante`, `confidence_score`, contagens e JSON para **join** com coluna gold quando existir.

### Lote sobre `_local_samples/` (varios CSVs)

A partir de `plataform/nlp_engine`:

```powershell
.venv\Scripts\python.exe scripts\validate_local_samples.py
.venv\Scripts\python.exe scripts\validate_local_samples.py --verbose
.venv\Scripts\python.exe scripts\validate_local_samples.py --write-audits --min-parity-rate 1.0
.venv\Scripts\python.exe scripts\validate_local_samples.py --root _local_samples\diamond --config-yaml (Resolve-Path "..\..\colon_config.yaml").Path
```

- Coloque exports Diamond/HML em `_local_samples/diamond/`, `exports/` ou `gold/` (estrutura sugerida no README da lib).
- Ficheiros **sem** `Laudo`+`laudo_tratado` ficam com paridade `skip` (normal para CSVs so de entrada).
- **Taxa de paridade abaixo de 100%** em `dev_tb_diamond_*` e comum ate o proxy `laudo_tratado` estar alinhado com o `to_plain` actual (Pandoc, rodapes, normalizacao) — mede **gap**, nao obrigatoriamente bug; use `--verbose` para ver o primeiro diff e classificar causas.
- **Aviso RTF / WebRIS:** muitos exports trazem no `laudo_tratado` a linha institucional *«Este Laudo pode não estar completo… WebRIS»* que o `to_plain` ja nao inclui no texto limpo — o diff mostra exactamente isso. Para uma **metrica de paridade mais alinhada ao corpo clinico**, use `--strip-rtf-notice` em `report_to_plain_parity.py` ou `validate_local_samples.py` (remove essa linha em ambos os lados **só na comparacao**).
- **Hifen escapado RTF:** no inicio de linha (lista) ou no meio da linha (ex.: dois telefones separados por barra invertida antes do hifen), o `laudo_tratado` pode diferir do `to_plain` apenas por ` \- ` vs ` - `. `--normalize-rtf-list-markers` ou **`--parity-relaxed`** alinham **so na comparacao**.
- Com `--min-parity-rate`, o processo falha (exit 1) se alguma paridade calculada for inferior — util como **gate** apenas depois de acordar meta de taxa ou sobre subset ja alinhado.
- `--config-yaml` tem de existir no disco; caminho placeholder faz o script sair com erro **antes** de processar (evita auditorias `error` em silencio).
- Pastas `exports/validate_<timestamp>/` sao ignoradas na descoberta para nao revalidar saidas de auditoria.

---

## 3) Ligacao ao plano de gaps

Matriz legado vs motor e IDs de gap: [paridade-legado-matriz-gaps-v0.md](paridade-legado-matriz-gaps-v0.md).

Decisao T02.4 (schema completo): [t02-4-quality-guard-monitoring-v0.md](t02-4-quality-guard-monitoring-v0.md).

---

## 4) Check de equivalencia legado (Grupo 1b) — 2026-04-20

**Escopo do check:** criterio hibrido (paridade comportamental + taxa de paridade em amostra).

### Baseline interno do motor

- `pytest tests -q --ignore=tests/local` -> **86 passed** (terminal local do dev).

### Amostras disponiveis para varredura `_local_samples`

- No momento deste check, a pasta `_local_samples/` do workspace estava sem CSV/TSV descobriveis para corrida em lote (`validate_local_samples.py`), portanto a varredura completa de Grupo 1b ficou **bloqueada por dado indisponivel**.

### Gate por especialidade (Grupo 1b)

| Especialidade | Status | Evidencia atual | Nota |
|---------------|--------|-----------------|------|
| Colon / DII | ok | `diamond/dev_tb_diamond_mod_colon_entrada.csv` = 1.0000 (`--parity-relaxed`) | Sem mismatch na amostra registada |
| Biliar | bloqueado | sem CSV local disponivel nesta corrida | Necessario disponibilizar export para gate |
| Hepatologia | bloqueado | sem CSV local disponivel nesta corrida | Necessario disponibilizar export para gate |
| Reumatologia | bloqueado | sem CSV local disponivel nesta corrida | Necessario disponibilizar export para gate |
| Neuroimunologia | bloqueado | sem CSV local disponivel nesta corrida | Necessario disponibilizar export para gate |

### Classificacao de divergencias (corrida atual)

- Nao houve mismatch novo observado nesta corrida (sem varredura de amostras locais).
- Mantem-se classificacao de referencia para paridade: normalizacao RTF/WebRIS e `\\-` (quando aplicavel), gaps de configuracao YAML e regressao funcional real.

### Proxima acao para fechar gate completo Grupo 1b

1. Repor CSVs de referencia em `_local_samples/diamond/` por especialidade.
2. Executar: `validate_local_samples.py --parity-relaxed --verbose --validate-output-invariants`.
3. Atualizar esta secao com taxa por especialidade e status final `ok` / `ok_com_gap_documentado` / `bloqueado`.

---

## 5) Piloto Pulmao (S03/S06b) — runtime YAML + comparacao no mesmo input

- O piloto Pulmao adota gate funcional: **mesmo input -> resultado semelhante ou superior ao legado**.
- Para Pulmao, a ausencia de `laudo_tratado` no CSV de entrada **nao bloqueia** a validacao; o gate ocorre por comparacao de **saida motor vs saida legado**.
- A execucao do piloto deve usar `--config-yaml` real da especialidade (sem fallback demo).
- Decisoes, microetapas e SPEC da execucao estao em: `notas/s03-piloto-pulmao-runtime-v0.md`.
- Execucao local atual:
  - auditoria com config real (`configs/pulmao/config.yaml`) -> **100 linhas** escritas;
  - testes da especialidade -> **4 passed**;
  - comparacao com legado -> `joined=0` (`bloqueado_sem_chave_comum`), sem base para classificar divergencia funcional nesta corrida.
