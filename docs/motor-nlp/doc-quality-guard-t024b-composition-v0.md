# T02.4b — Quality guard ate lib `monitoring` (composition root v0)

**Status:** caminho operacional para **teste e validacao** sem bloquear o MVP rule-based.

## Decisao

Enquanto o pacote **`monitoring`** (wheel) nao existir no feed interno, o **quality guard** de schema de linha e aplicado no **Composition Root** (notebook Databricks / script de orquestracao) **apos** `ClinicalNlpEngine.process`, usando apenas funcoes puras da lib `nlp_engine`:

- `nlp_engine.output_invariants.validate_engine_output_row(row)`
- `nlp_engine.output_invariants.validate_exm_laudo_resultado_json(raw_json_str)`

**Sem import cruzado:** o motor nao depende de `monitoring`; `monitoring` (futuro) podera importar apenas tipos/contratos documentados ou duplicar validacao leve, conforme alinhamento EngML.

## Exemplo (notebook)

```python
from nlp_engine import ClinicalNlpEngine
from nlp_engine.output_invariants import (
    validate_engine_output_row,
    validate_exm_laudo_resultado_json,
)

engine = ClinicalNlpEngine()
out_rows = engine.process(batch, nlp_cfg, specialty_id=..., config_version=...)
for row in out_rows:
    errs = validate_engine_output_row(row)
    errs.extend(validate_exm_laudo_resultado_json(row["exm_laudo_resultado"]))
    if errs:
        raise ValueError(errs)
```

## Proximo passo (T02.4b definitivo)

Migrar validacao para `monitoring.quality_guard` quando o repo existir, mantendo o mesmo contrato de entrada (`dict` linha) e lista de erros — ver [notas/t02-4-quality-guard-monitoring-v0.md](notas/t02-4-quality-guard-monitoring-v0.md).

**Owner:** EngML / monitoring (data alvo a fechar no board).
