# Gate C — paridade local com amostras (v0)

Complementa gates A+B (ruff + pytest versionado). **Nao** bloqueia CI por defeito.

## Comando sugerido

A partir de `plataform/nlp_engine`, com CSV local (Diamond/HML) e caminho absoluto:

```powershell
$env:NLP_HML_LAUDOS_CSV = (Resolve-Path "_local_samples\diamond\...").Path
$env:NLP_HML_MAX_ROWS = "500"
$env:NLP_HML_PARITY_REPORT_ONLY = "1"
.venv\Scripts\python.exe -m pytest tests\local\test_hml_sample_to_plain_parity.py::test_hml_sample_to_plain_parity_legacy_report -v -s
```

Opcional: `NLP_HML_MIN_MATCH_RATE`, `NLP_HML_PARITY_STRICT`.

## O que registar no board/PR

- Ficheiro e especialidade.
- Linhas avaliadas (`NLP_HML_MAX_ROWS`).
- Taxa de match e interpretacao (ex.: rodape WebRIS vs `to_plain`).
- Decisao: aceite, WIP ou accao corretiva (gold, pipeline ou criterio).

## Relacao com S02

Paridade **texto** (`to_plain` vs coluna legada) nao substitui testes do **rule_engine**; serve como integracao do TextPipeline sobre dados reais.

## Evidencia local (execucao registrada)

- Especialidade/amostra: `dev_tb_diamond_mod_colon_entrada.csv` (`_local_samples/diamond`).
- Execucao: `NLP_HML_PARITY_REPORT_ONLY=1`, `NLP_HML_MAX_ROWS=500`.
- Resultado observado no relatorio local:
  - `Linhas no lote`: 100
  - `Comparadas`: 100
  - `Match`: 32
  - `Mismatch`: 68
  - `Taxa de match`: `0.3200`
- Interpretacao principal: divergencias recorrentes por remocao de aviso final WebRIS no `to_plain` versus permanencia desse trecho no `laudo_tratado` legado.
- Decisao de engenharia: manter Gate C como evidencia complementar (nao bloqueante de CI), com classificacao de divergencias por tipo/impacto no board.
