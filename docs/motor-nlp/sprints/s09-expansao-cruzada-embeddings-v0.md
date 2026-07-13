# S09 — Plano de expansao cruzada apos aceite no piloto hepato

## Escopo

- Piloto 0: hepatologia (calibracao e gate de aceite).
- Expansao oficial de validacao legado: biliar, neuroimunologia, reumatologia e colon.

## Pre-condicoes para iniciar expansao

- Benchmark A/B em hepato concluido (baseline sem embeddings, MiniLM, BioBERTpt).
- Decisao formal do gate (`aceite`, `ajuste`, `rollback`) com:
  - F-beta(2),
  - matriz TP/FN/FP/TN,
  - McNemar vs baseline,
  - IC95% das metricas-chave,
  - sem regressao nao documentada.

## Sequencia recomendada

1. Congelar config vencedora do piloto (modelo, threshold, modo de decisao).
2. Replicar config para biliar, neuroimunologia, reumatologia e colon via YAML.
3. Executar `build -> audit -> compare` por especialidade.
4. Classificar divergencias por bucket (config, semantico, incerteza legado).
5. Consolidar baseline multi-especialidade para rollout.

## Gates por especialidade

- Gate A (funcional): sem quebra de contrato/invariantes.
- Gate B (paridade): match rate e F-beta(2) sem regressao relevante.
- Gate C (aceite): sign-off tecnico + clinico em divergencias relevantes.

## Artefatos obrigatorios

- JSON de comparacao por especialidade (`*_compare.json`).
- Matriz consolidada multi-especialidade (TP/FN/FP/TN + F-beta2).
- Nota de decisao de rollout por especialidade.
