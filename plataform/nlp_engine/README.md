# nlp_engine

Biblioteca de processamento NLP (TextPipeline, engine, config loader, scoring). Recebe `dict` injetado pelo notebook (Composition Root); nao le ficheiros YAML.

Documentacao: [docs/motor-nlp/README.md](../../docs/motor-nlp/README.md)

### Amostras locais (nao versionadas)

**Pasta `_local_samples/`** na raiz desta lib: coloque ai todos os CSVs/exports (gold, diamond, `hive_metastore.*`, etc.). Esta no [`.gitignore`](.gitignore) — nada disto e commitado. Estrutura sugerida: `exports/`, `gold/`, `diamond/` (pode criar ao copiar ficheiros). No **lake**, a linha **Hepatologia (hepato)** e tratada como amostra **gold**; copie esses exports para `gold/` (e nao confundir com outras pastas de tier). Para montar `hepatologia_standard_{input,expected}.csv` a partir do gold Hive: `scripts/build_hepatologia_standard_sample.py` (default `--source gold_hive` e caminho `gold/hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv`); ver [nota S06](../../docs/motor-nlp/notas/s06-hepatologia-validacao-v0.md).

**Regras:** sem dados sensiveis no repositorio; delimitadores CSV nos exports variam (`,`, `;`, TAB) — no pandas use `sep` explicito ou deteccao. Contrato de colunas do motor: [doc-contrato-runtime-config-especialidade-v0.md](../../docs/motor-nlp/doc-contrato-runtime-config-especialidade-v0.md), [anexo02-arquitetura-motor-nlp-v0.md](../../docs/motor-nlp/anexo02-arquitetura-motor-nlp-v0.md).

**Paridade HML opcional:** env `NLP_HML_LAUDOS_CSV` com caminho absoluto para um CSV dentro de `_local_samples/` — ver secao mais abaixo e `tests/local/`.

**Copiar referencias da pasta Downloads** para `_local_samples/exports/`: [scripts/copy_downloads_to_local_samples.ps1](scripts/copy_downloads_to_local_samples.ps1) (PowerShell; ajuste o caminho ao repo).

**Nota:** `tests/fixtures/local_samples/` nao deve receber CSVs versionados — ver [tests/fixtures/local_samples/README.md](tests/fixtures/local_samples/README.md).

**CI:** em push/PR que alteram `plataform/nlp_engine/`, o workflow [.github/workflows/nlp_engine.yml](../../.github/workflows/nlp_engine.yml) roda `ruff check` e `pytest`.

### S01 (T01.1–T01.5) — modulo de teste

Gate formal da historia **S01** na CI: `ruff check .` e `pytest tests -q --ignore=tests/local` (os testes em `tests/local/` sao opcionais e exigem env).

| Task (anexo03) | Cobertura principal |
| -------------- | ------------------- |
| **T01.1** `to_plain` | `tests/text_pipeline/test_to_plain.py`, `test_to_plain_fixtures.py` |
| **T01.2** segmentacao / headers / ancoras | `tests/text_pipeline/test_by_headers.py`, `test_anchors.py` |
| **T01.3** negacao | `tests/text_pipeline/test_negation.py` |
| **T01.4** boilerplate | `tests/text_pipeline/test_boilerplate.py` |
| **T01.5** laudos sinteticos integrados | `tests/text_pipeline/test_s01_t015_synthetic_laudos.py` |

Paridade CSV HML/Diamond (opcional, maquina local): `tests/local/test_hml_sample_to_plain_parity.py` — ver secao abaixo.

**Evidencia de fecho S01 (nota para o board):** [docs/motor-nlp/notas/s01-fecho-evidencia-v0.md](../../docs/motor-nlp/notas/s01-fecho-evidencia-v0.md).

## Desenvolvimento

**Python 3.12.x** (fixo no `pyproject.toml`; alinhado a CI, legado; extras `[dev]` / `[spacy]`).  
Se `py -3.12` nao existir (`py -0p`), instale **Python 3.12.x** em [python.org/downloads](https://www.python.org/downloads/) (escolha a release **3.12**) ou, no Windows, `winget install Python.Python.3.12 -e`; depois confirme com `py -0p`.

Ambiente virtual em **`.venv/`** (nao commitado). Recomendado: no Cursor/VS Code, escolher o interpretador  
`plataform/nlp_engine/.venv/Scripts/python.exe`.

**`.venv` já criado:** a partir de `plataform/nlp_engine`, sincronize deps com  
`.venv\Scripts\python.exe -m pip install -e '.[dev]'` e use o mesmo executavel para `ruff`/`pytest` (ver abaixo). Não precisa voltar a correr o bootstrap só por isso.  

**Router LLM (opcional):** para segunda passagem HTTP OpenAI-compatible (`nlp.llm_router.mode: llm`), instale também  
`.venv\Scripts\python.exe -m pip install -e '.[llm]'` (extra `httpx`). Contrato e YAML: [doc-llm-router-v0.md](../../docs/motor-nlp/doc-llm-router-v0.md).  

Se o `pip` acusar `requires-python` (ex. 3.13 vs `<3.13,>=3.12`), apague `.venv` e recrie com **`py -3.12 -m venv .venv`** antes do `pip install`.

### Opcao A — script (evita problemas de path com apóstrofo)

Com **Python 3.12.x** (ex.: `py -3.12`), a partir de `plataform/nlp_engine`:

```bash
py -3.12 scripts/bootstrap_venv.py
```

Depois use **sempre** o Python do venv para ruff/pytest (nao o global):

```bash
.venv\Scripts\python.exe -m ruff check .
.venv\Scripts\python.exe -m pytest tests -q
```

### Opcao B — manual

```bash
cd plataform/nlp_engine
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -e '.[dev]'
.venv\Scripts\python.exe -m ruff check .
.venv\Scripts\python.exe -m pytest tests -q
```

(Com `activate`, o `python` da shell passa a ser o do venv; acima evita confusao se a shell nao estiver ativada. No **PowerShell**, use aspas **simples** em `-e '.[dev]'` — com aspas duplas o `[` e tratado como padrao e o `pip` falha.)


Extras: **`[dev]`** inclui PyYAML, pytest, ruff e **spaCy** (mesmo conjunto que a CI e os testes de ancoragem). **`[spacy]`** continua disponivel para instalar so o motor de linguagem sem ferramentas de dev. A lib e **dict-in** (YAML lido fora da lib).

**Validar spaCy:** `python -m pytest tests/test_spacy_runtime.py -q` (import + `blank('pt')` com sentencizer).

**Windows — `DLL load failed` / `numpy_ops` (thinc):** o spaCy depende de binarios nativos (Thinc/blis). O comando `thinc` **nao existe** no PATH (e normal).

1. **Instale o runtime certo:** na pagina [Latest supported VC++ Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist), na secao **Visual Studio 2015, 2017, 2019 e 2022**, descarregue **`VC_redist.x64.exe`** (nao basta um ficheiro generico `vcredist_x64.exe` de outro ano ou de terceiros). Execute, reinicie o PC se o instalador pedir, abra um **PowerShell novo**.
2. Confirme Python **64 bits:** `python -c "import struct; print(struct.calcsize('P')*8)"` → `64`.
3. No venv:  
   `pip install --force-reinstall --no-cache-dir numpy blis cymem preshed murmurhash thinc spacy`  
   e teste `python -c "import spacy; print(spacy.__version__)"`.

**Ainda falha:** abra o ficheiro `numpy_ops*.cp312-win_amd64.pyd` em `\.venv\Lib\site-packages\thinc\backends\` com a ferramenta [Dependencies](https://github.com/lucasg/Dependencies) (mostra qual **DLL** falta — muitas vezes `VCRUNTIME140_1.dll` ou `MSVCP140.dll`). **Alternativas:** ambiente **Conda/Mamba** com `spacy` (wheels diferentes no Windows), ou desenvolvimento com spaCy em **WSL2** (Linux), onde este erro e raro.

**Caminho com apostrofo** (`D'Or`) e improvavel como causa, mas pode clonar o repo para `C:\work\Projects\` para descartar.

### Paridade opcional CSV HML / Diamond (`Laudo` vs `laudo_tratado`)

Validacao **local** (nao versionar CSVs reais; `_local_samples/` esta no `.gitignore`).

1. Defina o caminho **absoluto** do ficheiro com colunas `Laudo` e `laudo_tratado`. Separador `;`, `,` ou **TAB** — detetado por `csv.Sniffer` no teste.
2. **Paridade:** compara `to_plain(Laudo)` com `laudo_tratado` (apos normalizacao). Se `laudo_tratado` vier do **legado HML** e nao for o mesmo resultado que `to_plain` gera hoje, o teste **falha por definicao** — mede **gap de paridade**, nao necessariamente bug do `to_plain`. Para fechar S01 com criterio relaxado, documente no board/PR a **taxa de match** ou trate divergencias como WIP ate alinhar gold ou o pipeline.
3. **Diamond:** copie os exports por especialidade para `_local_samples/diamond/` e aponte `NLP_HML_LAUDOS_CSV` para cada ficheiro (colon, biliar, pulmao, etc.), um de cada vez. O mesmo teste serve; nao e necessario um comando por especialidade alem de trocar o caminho.
4. **`NLP_HML_MAX_ROWS`:** inteiro positivo opcional — limita quantas **linhas de dados** sao avaliadas (cabecalho nao conta). Util para ficheiros com milhares de linhas e feedback rapido.
5. Smoke versionado no repo: `tests/fixtures/hml_parity_minimal.csv` — use `Resolve-Path` / caminho absoluto em `NLP_HML_LAUDOS_CSV`.
6. Execute os testes em `tests/local/`:

**PowerShell** (a partir de `plataform\nlp_engine`; se estiver na raiz do repo, faça `cd plataform\nlp_engine` **uma vez** — não duplique o `cd` se já estiver nessa pasta)

```powershell
$env:NLP_HML_LAUDOS_CSV = (Resolve-Path "tests\fixtures\hml_parity_minimal.csv").Path
pytest tests\local\ -v
```

**Opcional — comparacao mais estrita** (menos normalizacao de espacos/linhas): `$env:NLP_HML_PARITY_STRICT = "1"`

**Opcional — amostra das primeiras N linhas:** `$env:NLP_HML_MAX_ROWS = "500"`

**Relatorio vs legado (nao falha por mismatch):** `$env:NLP_HML_PARITY_REPORT_ONLY = "1"` e corra com `pytest -s` para imprimir **taxa de match** e primeiros exemplos de divergencia. Opcional: `$env:NLP_HML_MIN_MATCH_RATE = "0.95"` para **falhar** se a taxa for inferior (0..1).

Exemplo com ficheiro Diamond local (ajuste o nome do ficheiro):

```powershell
$env:NLP_HML_LAUDOS_CSV = (Resolve-Path "_local_samples\diamond\colon_diamond.tsv").Path
$env:NLP_HML_MAX_ROWS = "2000"
pytest tests\local\test_hml_sample_to_plain_parity.py -v
```

Relatorio sobre parte da amostra (stdout + exit 0 mesmo com mismatches, salvo `MIN_MATCH_RATE`):

```powershell
$env:NLP_HML_LAUDOS_CSV = (Resolve-Path "_local_samples\diamond\dev_tb_diamond_mod_colon_entrada.csv").Path
$env:NLP_HML_MAX_ROWS = "500"
$env:NLP_HML_PARITY_REPORT_ONLY = "1"
.venv\Scripts\python.exe -m pytest tests\local\test_hml_sample_to_plain_parity.py::test_hml_sample_to_plain_parity_legacy_report -v -s
```

Sem `NLP_HML_LAUDOS_CSV`, o teste e **ignorado** (CI e clones sem ficheiro continuam verdes).

**Scripts de evidencia (local):**

- Paridade S01 (CSV com `Laudo` + `laudo_tratado`): [`scripts/report_to_plain_parity.py`](scripts/report_to_plain_parity.py) — `--strip-rtf-notice` (aviso WebRIS), `--normalize-rtf-list-markers` (inicio de linha e inline ` \- ` vs ` - `), ou `--parity-relaxed` (os dois); só afecta a metrica de comparacao.
- Auditoria S02 (CSV + saida processada): [`scripts/audit_engine_from_csv.py`](scripts/audit_engine_from_csv.py) — requer `pip install -e ".[dev]"` (PyYAML). Opcional `--config-yaml` para YAML de especialidade (merge de `shared_organs_path` se existir). Opcional `--validate-output-invariants` (falha se linha de saida violar `output_invariants` + JSON minimo em `exm_laudo_resultado`).
- **Lote em `_local_samples/`:** [`scripts/validate_local_samples.py`](scripts/validate_local_samples.py) — percorre `*.csv`/`*.tsv`, corre paridade onde houver `Laudo`+`laudo_tratado`, auditoria onde houver coluna de texto; `--verbose` imprime o primeiro diff de paridade; `--strip-rtf-notice` / `--normalize-rtf-list-markers` / `--parity-relaxed` (ver acima); `--config-yaml` tem de ser caminho real (senao o script termina com erro); opcional `--write-audits`, `--min-parity-rate` (exit 1 se abaixo), `--validate-output-invariants`. Ignora `exports/validate_*` de corridas anteriores.
- **Hepatologia — bancada motor vs legado (CSV local):** [`scripts/run_hepatologia_diamond_bench.py`](scripts/run_hepatologia_diamond_bench.py) — `--mode baseline` (um audit + compare) ou `--mode matrix` (matriz de cenarios YAML). Se existir `_local_samples/diamond/query_hepato_validate.csv`, usa-o como entrada e gold no mesmo ficheiro; senao, `hepatologia_standard_{input,expected}.csv` (ver [exploracao-global-sem-treino-v0.md](../../docs/motor-nlp/notas/exploracao-global-sem-treino-v0.md)). **`--promotion-profile fn_priority`** escolhe vencedor por **menor FN** com orcamento de FP (`configs/hepatologia/scenarios/strategy_matrix.yaml`); omitir perfil mantem `fp_ceiling` do YAML. Matriz directa: [`scripts/run_hepatologia_strategy_matrix.py`](scripts/run_hepatologia_strategy_matrix.py). **Reavaliar promocao sem reauditar:** [`scripts/recompute_strategy_matrix_promotion.py`](scripts/recompute_strategy_matrix_promotion.py) sobre `hepatologia_strategy_matrix.json`. **Calibração em camadas (L1–L3, match ≥ 0,80):** [`configs/hepatologia/scenarios/strategy_matrix_calibration_layers.yaml`](configs/hepatologia/scenarios/strategy_matrix_calibration_layers.yaml) + nota [calibracao-hepatologia-camadas-v0.md](../../docs/motor-nlp/notas/calibracao-hepatologia-camadas-v0.md) — usar `--scenarios-yaml` e `--only-scenarios` no bench.

**Contrato motor rule-based (SPEC):** [doc-contrato-engine-rule-based-v0.md](../../docs/motor-nlp/doc-contrato-engine-rule-based-v0.md). **T02.4b composition:** [doc-quality-guard-t024b-composition-v0.md](../../docs/motor-nlp/doc-quality-guard-t024b-composition-v0.md). Tipos documentais: [`nlp_engine/contracts.py`](nlp_engine/contracts.py).

Documentacao: [notas/s01-s02-paridade-evidencia-v0.md](../../docs/motor-nlp/notas/s01-s02-paridade-evidencia-v0.md), [matriz e gaps](../../docs/motor-nlp/notas/paridade-legado-matriz-gaps-v0.md).

### RTF e Pandoc (`to_plain`)

A conversao RTF tenta, nesta ordem: **pypandoc** (se importavel) com binario **Pandoc** no PATH, depois **striprtf**, depois fallback heuristico em codigo.

- `pip install -e ".[pandoc]"` instala apenas o pacote **pypandoc**; sem o executavel **pandoc** no ambiente, a chamada pode falhar e o fluxo segue para `striprtf` / fallback (comportamento esperado).
- Em Databricks/CI Linux, instale o pandoc pelo gerenciador do SO ou imagem base se quiser priorizar o mesmo caminho do notebook legado.

### Motor (S02)

- **T02.1–T02.3:** `ClinicalNlpEngine` em [`nlp_engine/engine.py`](nlp_engine/engine.py) — `process(...)` aplica `to_plain`, depois [`process_rule_based`](nlp_engine/rule_engine.py): `findings`, `findings_regex` opcional, `use_spacy_matcher` (default ligado), `finding_organ_max_chars` opcional (proximidade orgao–achado), `target_organs`, `organs`, negação. Scoring via [`scoring.confidence_rule_based`](nlp_engine/scoring.py) com `score_policy_version` (`v1_bins_legacy` | `v2_density`). `feature_flags.rule_engine: false` desliga o rule-based. SPEC: [spec-rule-engine-t022-v0.md](../../docs/motor-nlp/spec-rule-engine-t022-v0.md), [spec-scoring-t023-v0.md](../../docs/motor-nlp/spec-scoring-t023-v0.md).
- **Testes:** [`tests/test_engine_process.py`](tests/test_engine_process.py), [`tests/test_rule_engine.py`](tests/test_rule_engine.py).
- **T02.4a / T02.4b:** invariantes locais + JSON minimo em `output_invariants`; T02.4b completo em `monitoring` (pendente); composition root ate la — [nota](../../docs/motor-nlp/notas/t02-4-quality-guard-monitoring-v0.md), [doc-quality-guard-t024b-composition-v0.md](../../docs/motor-nlp/doc-quality-guard-t024b-composition-v0.md); ver [anexo03](../../docs/motor-nlp/anexo03-historias-e-tasks-v0.md).
- **Checkpoint local S02:** [s02-checkpoint-fecho-local-v0.md](../../docs/motor-nlp/notas/s02-checkpoint-fecho-local-v0.md) e Gate C em [s02-gate-c-paridade-v0.md](../../docs/motor-nlp/notas/s02-gate-c-paridade-v0.md).

Referencia: [anexo03-historias-e-tasks-v0.md](../../docs/motor-nlp/anexo03-historias-e-tasks-v0.md), [anexo02-arquitetura-motor-nlp-v0.md](../../docs/motor-nlp/anexo02-arquitetura-motor-nlp-v0.md).
