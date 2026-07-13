# Retro Review - Hepatologia (Slide-Ready)

**Estado:** encerrado (2026-05-08) — protocolo FP classe 3 finalizado.

## Pagina 1 - Resumo Executivo

### Objetivo

Reduzir FP em negativos (classe 3) sem perder desempenho global, com evolucao controlada e orientada por dados.

### Como iniciamos

- Baseline rule-based medido para referencia.
- Ancora operacional definida: `S5_hybrid_calibrated`.

### O que foi feito

- Protocolo em fases (YAML-only, 1 eixo por vez): F1 Semantica, F2 Regras, F3 LLM (banda), F4 LLM + regex negativos, F5 combo F3+F4 (validacao extra).
- Gates: smoke 500, full 2000, comparacao sempre contra `S5_hybrid_calibrated`, rerun de estabilidade no candidato final.

### Evolucao e decisoes

- F1/F2: sem candidato.
- F3: ganho forte vs S5; inferior a F4 no trade-off agregado do full.
- F4: melhor equilibrio no full; **candidato oficial**.
- F5: validado em smoke e full; **nao superou F4** em MR/FN no full 2000.

### Decisao final (data-based)

**Promover como candidato de configuracao:** `fp3_f4_llm_neg_patterns`.

### Proximo passo (operacao)

Migracao para YAML/matriz de producao e governanca de mudanca conforme time — fora do escopo deste bench.

---

## Pagina 2 - Metricas e Evidencias

### Referencia (full 2000, `--only-cod-123`)

| Cenario | MR | FP | FN |
|---|---:|---:|---:|
| baseline | 0.5904 | 58 | 608 |
| S5_hybrid_calibrated | 0.8659 | 132 | 86 |

### Candidato vencedor — F4 full 2000 (duas corridas)

| Run | MR | FP | FN | llm_called_rate | llm_error_rate |
|-----|----:|---:|---:|----------------:|---------------:|
| 1 | 0.8887 | 98 | 83 | 0.391 | 0.0 |
| rerun | 0.8881 | 99 | 83 | 0.391 | 0.0 |

Variacao: MR −0,0006; FP +1; FN estavel — aceitavel para estabilidade.

### Combo F5 — full 2000 (referencia; nao vencedor)

| Run | MR | FP | FN |
|-----|----:|---:|---:|
| 1 | 0.8862 | 99 | 86 |
| rerun | 0.8868 | 98 | 86 |

### Dataset (contexto rapido)

- Fonte: `query_hepato_validate.csv`
- Recorte: `--only-cod-123`
- Smoke: `max_rows=500` | Full: `max_rows=2000`
- Subset comparavel principal: `n=813`
