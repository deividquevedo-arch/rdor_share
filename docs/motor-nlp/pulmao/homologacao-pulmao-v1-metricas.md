# Homologação — Transplante de Pulmão V1 (piloto junho/2026)

> Métricas consolidadas do piloto. **Base ouro** (Fase 0 da arquitetura alvo). Só agregados —
> sem texto de laudo / sem idExame (LGPD: gabaritos brutos ficam fora do repo).

## Fontes (gabarito humano — fora do repo)
- `mini_homologacao_carol.xlsx` :: aba `homologacao` — 388 laudos (127 positivos do motor + 261
  negativos), coluna gold "Achado Relevante (Sim; Não)" → 126 Sim / 262 Não.
- `recall_transplante_pulmao (1).xlsx` :: aba `recall_amostral` — 1470 negativos do motor
  (988 `medida_acima` + 482 `sem_medida`); gold "Deveria ser Relevante?" → **branco = concorda
  com o motor (TN)** (confirmado pela revisora; 4 marcados "Não" explícito, 0 "Sim").

## Matriz de confusão
| | View A — Homologação (388) | View B — Consolidado run (n=1854) |
|---|---|---|
| TP (relevante correto) | 126 | 126 |
| FP (falso alarme) | 1 | 1 |
| FN (perdeu relevante) | 0 | 0 |
| TN (excluiu certo) | 261 | 1727 |

## Métricas
| Métrica | View A (388) | View B (consolidado) |
|---|---|---|
| Acurácia | 0,9974 | 0,9995 |
| Precisão | 0,9921 | 0,9921 |
| Recall (sensibilidade) | 1,0000 | 1,0000 |
| Especificidade | 0,9962 | 0,9994 |
| F1 | 0,9960 | 0,9960 |
| F2 | 0,9984 | 0,9984 |
| MCC | 0,9942 | 0,9958 |

## Reconciliação com a run
Run de junho processou **~1852** laudos. O consolidado auditado dá **n=1854** = 127 positivos
(Carol) + 1727 negativos (recall 1470 ∪ 261 negativos da homologação, overlap de 6 idExame
deduplicado). Diferença de ~2 laudos = amostragem do recall + variância de não-determinismo do LLM
(±2 entre runs, temperatura 0 não é 100%).

## Erros e limites
- **1 FP** (único erro): VEF1 **0,68%** interpretado como valor (erro de digitação de **68%**).
  É bug de **extração** (ratio vs percentual), não de lógica de limiar. Guard opcional pendente.
- **0 FN dentro do escopo V1 (quantitativo).** O *gap narrativo* (laudos qualitativos "grave",
  em litros) é limite **declarado** (Carol manteve V1 quantitativo-only por medo de FP na fila) —
  não conta como FN aqui porque está fora do escopo de negócio da V1.

## Backlog aberto (resolver depois — decisão do head 2026-07-17)
- **[FP-01] 1 falso-positivo — VEF1 0,68% (digitação de 68%).** Guard determinístico no código:
  `VEF1 < ~10% ⇒ implausível ⇒ rejeitar/flag` (razão VEF1/CVF ~0,68 confundida com %). Melhora
  precisão sem tocar em lógica de limiar. Prioridade baixa (~0,8% dos relevantes, 1/127). Não
  bloqueia o congelamento da base ouro (a base ouro registra o FP como está).

## Fixture Fase 0 (base ouro)
Harness `baseohro_pulmao.py` (job tmp, lê os 2 arquivos de fora do repo): recomputa a matriz e
compara com os números CONGELADOS acima. É o gate de **comportamento** (não byte) para validar
mudanças das Fases 3/4 sem regressão no piloto pulmão. Números congelados:
`A_388 = {TP:126, FP:1, FN:0, TN:261}` · `B_full = {TP:126, FP:1, FN:0, TN:1727}`.
