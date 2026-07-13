# S10 — Hepatologia Gate de Producao (v1)

## Contexto
- Base congelada: `_local_samples/standard/hepatologia_parity_baseline_v1/`.
- Distribuicao alvo no baseline: `S=996`, `N=1000` (total `1996`).
- Objetivo de gate: `match_rate >= 0.85` em 2 rodadas estaveis.

## Fase 0 — Baseline congelado
- Build: `sample_rows=1996`, `pick_S=996/996`, `pick_N=1000/1000`.
- Compare baseline (motor vs legado): `match=1116`, `mismatch=880`, `rate=0.5591`.
- Matriz baseline:
  - `legacy_S_motor_S=148`
  - `legacy_S_motor_N=848`
  - `legacy_N_motor_S=32`
  - `legacy_N_motor_N=968`

## Fase 1 — Deep-dive FN
- Dominante: FN (`legacy_S_motor_N`) muito acima de FP.
- Padrões recorrentes observados em amostra de mismatch:
  - hepatopatia cronica / hipertensao portal;
  - vias biliares / colangio;
  - colelitiase/colecistopatia.
- Artefato de apoio criado:
  - `scripts/fn_deep_dive_hepatologia.py` (classificacao por cluster/exame).

## Fase 2 — Quick-fix packs aplicados
- Ajuste 1 (config Hepato):
  - ampliacao de `target_organs` para escopo hepatobiliar;
  - sementes para `vesicula_biliar` e `vias_biliares`;
  - inclusao de `colelitíase` e `hepatopatia` em `findings`.
- Ajuste 2 (robustez engine):
  - clip de `semantic_score` para `[0,1]` em `nlp_engine/engine.py`
  - evita violacao de `confidence_score_out_of_range` em benchmark.

## Fase 3 — A/B embeddings (full)
- Baseline split (sem embeddings): `1098/1996`, `rate=0.5501`.
- MiniLM (embeddings ON): `1134/1996`, `rate=0.5681`.
- BioBERTpt (embeddings ON): `1122/1996`, `rate=0.5621`.
- Ranking:
  1. MiniLM
  2. BioBERTpt
  3. Baseline

## Fase 4 — Gate de producao
- Resultado: **NO-GO** para producao.
- Motivo: melhor cenario (MiniLM) ainda distante do gate (`0.5681 << 0.85`).
- Conclusao operacional:
  - promover MiniLM como melhor opcao de experimento,
  - manter ciclo de reducao de FN por clusters clinicos antes de producao.

## Proximos passos priorizados
1. Rodar `fn_deep_dive_hepatologia.py` com `top_n=300` e fechar matriz causa-raiz.
2. Aplicar tuning incremental por pacote (1 pacote = 1 medicao) com foco em FN.
3. Revalidar A/B full apos cada pacote e publicar delta por matriz de confusao.
4. Repetir duas rodadas completas somente quando o melhor cenario aproximar do gate.

