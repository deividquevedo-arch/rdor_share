# Relatorio Executivo Resumido - Retro Review (Hepatologia / Motor NLP)

**Versao:** encerramento 2026-05-08 — protocolo FP classe 3 **fechado**.

## 1) Linha do tempo (o que foi feito, em ordem)

- Partida com baseline rule-based e pipeline atual para estabelecer referencia objetiva de MR/FP/FN.
- Calibracao por camadas ate fixar `S5_hybrid_calibrated` como referencia operacional.
- Execucao do protocolo FP classe 3 (YAML-only, um eixo por vez):
  - F1 Semantica (`similarity_threshold` 0.80/0.82)
  - F2 Regras (`finding_organ_max_chars`, `negation_window`)
  - F3 LLM (uncertainty band narrow)
  - F4 Regex/LLM patterns (padroes negativos adicionais)
  - F5 Combo F3+F4 (validacao extra)
- Gates por etapa: smoke 500, full 2000, comparacao contra `S5_hybrid_calibrated`, rerun de estabilidade no candidato final (`fp3_f4_llm_neg_patterns`).

## 2) Metricas principais por etapa (evolucao)

### Referencia full (`max_rows=2000`, `--only-cod-123`)

- `baseline`: `MR 0.5904 | FP 58 | FN 608`
- `S5_hybrid_calibrated`: `MR 0.8659 | FP 132 | FN 86`

### F1 / F2 (smoke 500)

- F1 rejeitada; F2 sem candidato (neutro ou regressao vs S5).

### F3 full 2000

- `fp3_f3_llm_band_narrow`: `0.8831 | 103 | 87` — ganho vs S5; inferior ao F4 no trade-off agregado.

### F4 full 2000 (vencedor)

- Run 1: `0.8887 | 98 | 83`
- Rerun: `0.8881 | 99 | 83`
- `llm_called_rate≈0.391`, `llm_error_rate=0`

### F5 full 2000 (combo; referencia)

- `0.8862 / 99 / 86` e rerun `0.8868 / 98 / 86` — **nao supera F4** em MR/FN.

## 3) Decisoes tomadas (abordagens do motor)

- **Candidato oficial para promocao (config):** `fp3_f4_llm_neg_patterns`.
- F3 permanece como alternativa documentada; F5 combo **nao promovido** sobre F4 neste recorte.
- Proxima frente de ganho (fora deste protocolo): plano de dados S04/S05 (WoE, features estruturadas com anti-leakage, homologacao).

## 4) Foco por componente tecnico (retro)

- Rule base + embeddings calibrados: ancora estavel (`S5`).
- LLM com `uncertainty_band` + `negative_context_patterns`: maior ganho medido nesta rodada.
- Threshold semantico isolado e alguns knobs de regra: sem candidato neste protocolo.

## 5) Dataset hepato (contexto breve)

- Fonte operacional: `query_hepato_validate.csv`
- Recorte de validacao: `--only-cod-123`
- Smoke: `max_rows=500` | Full: `max_rows=2000`
- Subset comparavel principal: `n=813`

## 6) Mensagem final para a retro

- Protocolo FP3 **encerrado com evidencia**: gates, full, combo e reruns documentados.
- **Decisao:** promover `fp3_f4_llm_neg_patterns` como candidato de configuracao.
- Evolucao futura de metrica espera mais ganho do **plano de dados** (gold, variaveis estruturadas validadas) do que de mais um knob isolado no YAML.
