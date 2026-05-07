# T02.4 quality_guard vs lib monitoring (v0 — alinhamento)

**Status:** decisao de desenho para desbloquear implementacao sem duplicar responsabilidades.

## Contexto

- [anexo03](../anexo03-historias-e-tasks-v0.md) lista **T02.4** como validacao do output contra schema esperado.
- [anexo02](../anexo02-arquitetura-motor-nlp-v0.md) define tres libs: `nlp_engine`, `data_manage`, `monitoring`, sem imports cruzados.

## Decisao recomendada (ate revisao do time)

1. **Schema / contrato de linha de saida** (campos obrigatorios, tipos, intervalos): responsabilidade da lib **`monitoring`** ou camada de composicao (notebook), **nao** um modulo pesado dentro de `nlp_engine`.
2. **`nlp_engine`**: garante **invariantes locais** via testes e tipagem do `ClinicalNlpEngine.process` (ex.: `confidence_score` em [0,1], `fl_relevante` em {0,1}, JSON serializavel em `exm_laudo_resultado`). Sem dependencia da lib `monitoring`.
3. **Implementacao futura T02.4:** ou (a) funcao pura em `monitoring` que recebe `dict` e valida, ou (b) `quality_guard.py` em `nlp_engine` **apenas** se o time decidir que a validacao e parte do pacote distribuido do motor — registar essa escolha no board antes de codar.

## Acao imediata

Fechar com **EngML / tech lead** uma linha no PR: *"Validacao de schema de saida em `monitoring`; `nlp_engine` mantem-so contrato via testes."*

**Anexo03:** tasks **T02.4a** (invariantes na lib `nlp_engine`) e **T02.4b** (schema / quality guard em `monitoring`) substituem a linha unica antiga **T02.4**.

## Telemetria minima para `monitoring` (payload em `exm_laudo_resultado`)

Campos recomendados para agregacao e KPIs (nao substituem schema Delta):

| Campo | Uso |
| --- | --- |
| `summary_compact` | evidencia textual agregada |
| `n_positive_spans` / `n_negated_spans` | sinais de cobertura e negação |
| `rule_engine_version` | regressao entre versoes do motor rule-based |
| `score_policy_version` | explicabilidade do `confidence_score` |

Validacao estrita de schema de linha (tipos, campos obrigatorios Delta) permanece candidata a `monitoring` ou composition root.

## Invariantes locais no `nlp_engine`

Ver [`output_invariants.validate_engine_output_row`](../../plataform/nlp_engine/nlp_engine/output_invariants.py) e testes em `tests/test_output_invariants.py`.

Validacao estrutural minima do JSON em `exm_laudo_resultado`: [`validate_exm_laudo_resultado_json`](../../plataform/nlp_engine/nlp_engine/output_invariants.py) (integrada em `validate_engine_output_row`).

## T02.4b — caminho composition root (ate existir `monitoring`)

Implementacao documentada e operacional: [doc-quality-guard-t024b-composition-v0.md](../doc-quality-guard-t024b-composition-v0.md). Scripts: `audit_engine_from_csv.py --validate-output-invariants`, `validate_local_samples.py --validate-output-invariants`.
