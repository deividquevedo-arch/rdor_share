# Playbook Global — Evolucao Paridade -> Producao

## Objetivo
Definir um protocolo reutilizavel para evoluir qualquer especialidade com seguranca:
- congelar baseline,
- diagnosticar FN/FP por causa-raiz,
- aplicar tuning incremental,
- validar gate com evidencias reproduziveis.

## Escopo global vs local
- Global (core):
  - protocolo de medicao (`build -> audit -> compare`);
  - gate estatistico e matriz de confusao;
  - ordem de ciclo (diagnostico -> ajuste -> revalidacao).
- Local (especialidade):
  - `findings`, `findings_regex`, negação e thresholds no YAML.

## Protocolo padrao (6 passos)
1. **Freeze baseline**
   - Congelar dataset, versoes, saidas e metadados.
2. **FN deep-dive**
   - Top N FN por subtipo de exame + cluster clinico + causa-raiz.
3. **Quick-fix packs**
   - Um pacote por vez, sem misturar mudancas, com delta medido.
4. **A/B embeddings**
   - Baseline vs modelos candidatos, com comparacao full.
5. **Gate de producao**
   - Duas rodadas estaveis no mesmo critério de aceite.
6. **Handoff global**
   - Registrar o que e reutilizavel no core e o que fica local.

## Pipeline unico A/B (obrigatorio no motor)
Sequencia canonica para qualquer especialidade:

1. `audit` (motor com config base/cenario)  
2. `compare` (pareamento com gold)  
3. `matrix` (baseline + cenarios, bootstrap e McNemar)  
4. `promotion` (`fp_ceiling` ou `fn_priority`)

Regras:
- mesma base de entrada e mesmo filtro de gold para todos os cenarios;
- baseline sempre incluido;
- promocao sempre explicita no relatorio.

## Pacote minimo de metricas (toda rodada)
- `match_rate`
- `FP/FN` (`legacy_N_motor_S`, `legacy_S_motor_N`)
- `metrics_by_cod_123` (quando gold tiver cod 1/2/3)
- `llm_called_rate`, `llm_error_rate`
- `mcnemar_vs_baseline.significant_0_05`

## Contrato minimo de cenarios YAML
- `matrix_spec.baseline_scenario_id`
- `matrix_spec.promotion.active_profile` (`fp_ceiling|fn_priority`)
- `scenarios[].id`
- `scenarios[].description`
- `scenarios[].config_patch` (deep-merge sobre config base)

Evitar:
- cenario sem baseline comparavel;
- mistura de mudancas clinicas e estruturais no mesmo patch;
- promocao sem regra declarada.

## Critérios de qualidade minimos
- Reprodutibilidade:
  - mesma entrada deve produzir metricas equivalentes.
- Rastreabilidade:
  - cada ganho ligado a um pacote de ajuste identificavel.
- Seguranca:
  - manter invariantes de output e limites de score.

## Anti-padroes (evitar)
- Tunar varios eixos ao mesmo tempo.
- Aprovar por `match_rate` isolado sem olhar matriz.
- Promover modelo sem rodar baseline comparavel na mesma base.
- Globalizar termo clinico especifico de uma especialidade.

## Artefatos recomendados por ciclo
- `*_compare.json` por cenario.
- nota de decisao (GO/NO-GO) com deltas.
- matriz de causa-raiz de FN priorizada (Pareto).
- template de cenarios por especialidade: `plataform/nlp_engine/configs/_templates/specialty_strategy_matrix.template.yaml`.

