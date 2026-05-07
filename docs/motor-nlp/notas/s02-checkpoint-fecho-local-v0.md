# S02 checkpoint local (v0)

Checkpoint de desenvolvimento local para S02 apos ajuste robusto do matcher.

## Gate A/B

- `ruff check .`: verde.
- `pytest tests -q --ignore=tests/local`: verde (77 passed).

## Coerencia com SPEC T02.2

Referencia: [spec-rule-engine-t022-v0.md](../spec-rule-engine-t022-v0.md)

- `rule_engine` agrega spans por 3 caminhos: `findings` por token normalizado, `findings_regex` e `Matcher` spaCy.
- Dedupe aplicado por `(categoria, start, end)` antes de negação/proximidade.
- Filtro de proximidade opcional (`finding_organ_max_chars`) implementado.
- `rule_engine_version` retornado no payload de resultado.

## Coerencia com SPEC T02.3

Referencia: [spec-scoring-t023-v0.md](../spec-scoring-t023-v0.md)

- Politicas `v1_bins_legacy` (default) e `v2_density` implementadas.
- `normalize_score_policy` faz fallback seguro para default.
- `fl_relevante` derivado de `n_positive_spans > 0`.

## Contrato de saida (doc runtime)

Referencia: [doc-contrato-runtime-config-especialidade-v0.md](../doc-contrato-runtime-config-especialidade-v0.md)

Campos minimos presentes no `process`:

- `id_predicao`
- `dt_execucao`
- `specialty_id`
- `config_version`
- `engine_version`
- `fl_relevante`
- `confidence_score`
- `exm_laudo_resultado`
- `exm_laudo_texto_tratado`

## Gate C

Ver evidencia e interpretacao em [s02-gate-c-paridade-v0.md](s02-gate-c-paridade-v0.md).
