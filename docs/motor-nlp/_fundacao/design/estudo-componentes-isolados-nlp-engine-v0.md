# Estudo — expor componentes isolados do `nlp_engine` (uso modular, tipo pandas/matplotlib)

**Data:** 2026-07-16 · **Status:** ESTUDO (considerações; nada implementado). Pedido do head: avaliar
usar UMA função/classe da lib sem rodar o pipeline completo, como opção de evolução modular.

## Achado principal
**Viável e vale a pena** — o refactor DecisionState (P0-A, v0.5.0) já entregou o **desacoplamento**
(funções-folha são chamadas como steps; `engine.py` fino). Falta só **curar a superfície pública**.

**A lib JÁ não é pipeline-only:** o `__init__` **aninhado** (`nlp_engine.nlp_engine`) já exporta ~24
nomes (`to_plain`, `process_rule_based`, `extract_rads_summary`, família `confidence_*`, negação,
segmentação). **MAS** o `__init__` **top-level** (`src/nlp_engine/__init__.py`) só exporta `__version__`.
→ o obstáculo é o **duplo-aninhamento `nlp_engine.nlp_engine`**: hoje `from nlp_engine.text_pipeline
import to_plain` **falha**; só `from nlp_engine.nlp_engine.text_pipeline import to_plain`.

## Candidatos por natureza
- **Autônomos (sem config/rede) — 1ª leva ideal:** `text_pipeline.to_plain`/`norm`/negação; **todo
  `scoring.*`** (o módulo mais "pandas-like"); pérolas puras do `quantitative` (`compare`,
  `normalize_value`, `parse_criterion`, `evaluate_criterion`) — hoje **nem exportadas**.
- **Config-fragment (mini-dict):** `process_rule_based`, `extract_rads_summary`, `segment_*`.
- **Side-effect (LLM/spaCy/creds) — 2ª leva:** `assess_criterion`, `semantic_evidence` — LGPD/creds
  in-tenant; fora da fronteira DS pura.

## O que atrapalha hoje
1. duplo-aninhamento `nlp_engine.nlp_engine`; 2. módulos ricos (`quantitative`) fora do `__all__`;
3. `_`-privados reusáveis (o `engine.py` importa 2 privados do `decision_pipeline` — fronteira já
borrada); 4. acoplamento a `nlp_config` (dict) sem schema tipado exposto; 5. singletons spaCy + LLM;
6. **falta `py.typed`** (typing extenso não vale no consumidor apesar do classifier "Typed");
7. docstrings sem exemplo de uso isolado.

## Opções de design
- **(a) API pública curada + submódulos temáticos estáveis** (recomendada): `__all__` no top-level
  resolvendo o aninhamento + `nlp_engine.text_pipeline`/`.scoring`/`.quantitative` reexportados.
- (b) expor tudo — **não** (congela internos como contrato; contraria "menos é mais").
- **(c) facade `nlp_engine.api`** com assinaturas amigáveis + docstrings de exemplo (desacopla contrato
  público da árvore interna; ponto único de semver/deprecação).

## Recomendação (incremental, curada)
1. (a)+(c): `__all__` curado no top-level + submódulos temáticos; promover `quantitative` puro.
2. **1ª leva = só puro/determinístico** (`to_plain`, `norm`, `scoring.*`, `compare`, `normalize_value`,
   `parse_criterion`, `evaluate_criterion`). **Nada de LLM/spaCy** nesta leva (LGPD + fronteira DS↔MLOps).
3. Adicionar **`py.typed`** + **teste de superfície pública** (importabilidade/assinatura) + docs com
   exemplos por função.
4. **Fixar semver antes do 1.0** (hoje 0.5.0/alpha — janela ideal): API pública = contrato backward-compat.
5. Side-effect (LLM/embeddings) só numa **2ª leva atrás de facade** com config-fragment tipado.

## Tensões a respeitar
- "Menos é mais": curar por tema, não virar "monte de função solta".
- LGPD: privilegiar as partes **determinísticas/puras** (limiar em código), não a extração LLM.
- Fronteira DS↔MLOps: funções puras (limpeza/negação/scoring/limiar) = território DS seguro para uso
  avulso; orquestração/LLM/spaCy = mais sensível.

> Não é reestruturação — o difícil (desacoplar) o DecisionState já fez. É **curadoria de superfície**.
> Encaixa como uma trilha de evolução separada do P0/P1 (opt-in, quando o head priorizar).
