# SPEC — Scoring S02 T02.3 (v0)

## Responsabilidade

Converter contagens do rule-based em `confidence_score` continuo no intervalo **[0.0, 1.0]**, com politica **explicita e versionada** via config.

## Input

- `n_positive_spans`, `n_negated_spans` (inteiros >= 0).
- `policy: str` — vem de `nlp_config["score_policy_version"]` normalizado por `normalize_score_policy`.

## Politicas

| ID | Comportamento resumido |
| --- | --- |
| `v1_bins_legacy` (default) | >0 positivos → 0.9; senao se >0 negados → 0.35; senao 0.0 |
| `v2_density` | positivos → `min(1, 0.52 + 0.09 * min(n_pos, 6))`; sem positivos → 0.35 se negados senao 0.0 |

Valores desconhecidos caem em `v1_bins_legacy`.

## Invariantes

- Sempre `0.0 <= score <= 1.0`.
- `fl_relevante` continua derivado apenas de `n_positive_spans > 0` (fora deste modulo, em `fl_relevante_from_counts`).

## O que NAO faz

- Calibracao estatistica por especialidade (fica para dados + monitoring).
- Metricas agregadas (F1, drift) — lib `monitoring`.
