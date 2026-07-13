# Baseline S02 congelado (v0)

Registo para execucao segura: gates minimos antes de evoluir `rule_engine` / scoring.

## Gates A + B (obrigatorios por PR)

A partir de `plataform/nlp_engine`:

```text
.venv\Scripts\python.exe -m ruff check .
.venv\Scripts\python.exe -m pytest tests -q --ignore=tests\local
```

## Comportamento referencia (pre-T02.2 estendido)

- `process_rule_based`: sentencas via spaCy `blank("pt")` + `sentencizer`; achados por alinhamento de tokens normalizados (`norm`); filtro opcional `target_organs` + `organs`; negação `negation_phrases` + `negation_window`.
- Saida: `summary_compact`, `n_positive_spans`, `n_negated_spans`.

## Data de congelamento

Documento criado na iteracao do plano **Execucao segura S02**; atualizar quando houver breaking change consciente no contrato.

## Artefactos SDD relacionados

- SPEC rule engine: [spec-rule-engine-t022-v0.md](../spec-rule-engine-t022-v0.md)
- SPEC scoring: [spec-scoring-t023-v0.md](../spec-scoring-t023-v0.md)
- Gate C (paridade local): [s02-gate-c-paridade-v0.md](s02-gate-c-paridade-v0.md)
