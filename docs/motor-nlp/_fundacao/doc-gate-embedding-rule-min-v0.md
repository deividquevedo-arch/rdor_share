# SPEC — Gate mínimo regra + embedding (v0, rascunho)

**Estado:** proposta para backlog quando a calibração **só YAML** não atingir `match_rate` ≥ alvo com FP/FN aceitáveis.

## Problema

No modo `hybrid`, `fl_relevante` pode passar a **S** apenas com `semantic_score >= similarity_threshold` sem evidência de regra forte, gerando **FP** difíceis de cortar só subindo o limiar (perda de **FN**).

## Direção de solução (contrato)

Promover relevante por embedding apenas se **todas** se verificarem (exemplo — valores via YAML):

1. `semantic_score >= similarity_threshold_embed`
2. `rule_score` (ou score rule-based pré-híbrido) **≥** `min_rule_score` **ou** `n_positive_spans >= 1` com outro critério explícito
3. Opcional: não estar em região de negação dominante (`n_negated_spans` cap)

## Config YAML (proposta)

```yaml
nlp:
  embeddings:
    gate_embedding_with_rule_evidence: true
    min_rule_score_for_semantic_promote: 0.15   # exemplo
```

## Implementação

- **Lib:** [`plataform/nlp_engine/nlp_engine/engine.py`](../../plataform/nlp_engine/nlp_engine/engine.py) — ramo hybrid após `semantic_evidence`.
- **Testes:** frases sintéticas sem PHI; sem regressão nos testes actuais de hybrid.
- **Governança:** task explícita no board / anexo03; não implementar sem acordo.

## Não faz

- Não substitui calibração de `similarity_threshold` / `fallback`.
- Não activa LLM; router continua opcional e separado.
