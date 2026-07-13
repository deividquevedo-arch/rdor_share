# Modelo de decisão do motor NLP (v0) — matemática e fluxo

**Escopo:** `ClinicalNlpEngine` (`plataform/nlp_engine`, pacote `fabrica_ia`).  
**Config:** YAML por especialidade (ex. hepatologia `0.1.10-hep-novo-lex-parity-v1`).  
**Parenteses:** documentação de referência; não altera código.

---

## 1. O que o motor calcula (duas saídas)

| Saída | Tipo | Papel |
|-------|------|--------|
| `fl_relevante` | **0 ou 1** | Decisão binária — laudo entra no filtro humano? |
| `confidence_score` | **[0, 1]** | **Score heurístico** de confiança (não é probabilidade calibrada de modelo estatístico). |

Tudo deriva de: texto tratado → achados léxicos → (opcional) similaridade semântica → (opcional) meta-calibração → (opcional) LLM na **banda de incerteza**.

---

## 2. Fluxo step-by-step (por laudo)

```
1. to_plain(laudo)           → normalização / remoção de rodapés (regex YAML)
2. segmentação               → blocos por órgão (headers / anchors / full_doc)
3. rule_engine (por bloco)   → n_pos, n_neg, summary_compact
4. fl_rule                   → 1 se n_pos > 0, senão 0
5. score_rule                → política v1_bins ou v2_density
6. embeddings (se ligado)    → s_sem = max cos_sim(trechos, termos YAML)
7. híbrido / fallback        → combina rule + semântica; pode promover fl 0→1
8. calibrated_score          → mistura fixa + bónus/penalidades (determinístico)
9. llm_router (se ligado)    → só se score ∈ [lo, hi]; pode mudar fl
10. gravação                 → fl_relevante final, confidence_score = score final
```

---

## 3. Camada A — Regras (determinística)

**Por frase:** match de `findings` (spaCy Matcher + frases normalizadas) + `findings_regex`.

**Filtros para contar um span positivo (`n_positive_spans`):**

- Menção ao `target_organs` na frase;
- Proximidade órgão–achado ≤ `finding_organ_max_chars` (gap em caracteres);
- **Não** negado (`negation_phrases` numa janela de `negation_window` tokens).

**Decisão rule:**

\[
fl_{\text{rule}} = \mathbb{1}[n_{\text{pos}} > 0]
\]

**Score rule** (`score_policy_version: v1_bins_legacy`, default hepatologia):

| Condição | `score_rule` |
|----------|----------------|
| \(n_{\text{pos}} > 0\) | **0,9** |
| \(n_{\text{pos}} = 0\) e \(n_{\text{neg}} > 0\) | **0,35** |
| caso contrário | **0,0** |

*(Política `v2_density`: crescimento suave com \(n_{\text{pos}}\); ver `scoring.py`.)*

---

## 4. Camada B — Embeddings (opcional, YAML `embeddings.use_embeddings`)

Trechos = frases do laudo (split `.!?;\n`). Termos = união de `findings` (ou `semantic_terms`).

**Similaridade (backend principal):** cosseno entre embeddings L2-normalizados (SentenceTransformer):

\[
s_{\text{sem}} = \max_{i,j} \cos(\mathbf{e}_{\text{trecho}_i}, \mathbf{e}_{\text{termo}_j}) \in [0,1]
\]

Fallback sem modelo: **Jaccard** sobre tokens normalizados (≥3 chars).

**Modo `hybrid`** (hepatologia):

\[
score \leftarrow \frac{w_r \cdot score_{\text{rule}} + w_s \cdot s_{\text{sem}}}{w_r + w_s}
\quad (w_r{=}0{,}7,\; w_s{=}0{,}3 \text{ típico})
\]

Se \(fl_{\text{rule}}=0\) e \(s_{\text{sem}} \geq \tau\) (`similarity_threshold`, ex. 0,78) → \(fl \leftarrow 1\).

**Modo `fallback`:** só promove \(fl\) se \(fl=0\), \(score_{\text{rule}}\) na `ambiguity_band` e \(s_{\text{sem}} \geq \tau\).

---

## 5. Camada C — Meta-calibração (`feature_flags.calibrated_hybrid`)

Score contínuo **determinístico** (não treinado em amostra nesta versão):

\[
score_{\text{cal}} = \mathrm{clip}_{[0,1]}\bigl(0{,}62\cdot score + 0{,}38\cdot s_{\text{sem}} + 0{,}03\cdot \min(n_{\text{pos}},4) - 0{,}02\cdot \min(n_{\text{neg}},4) + \delta_{\text{modalidade}}\bigr)
\]

`confidence_score` gravado passa a ser \(score_{\text{cal}}\) (`decision_source` → `hybrid_calibrated` se não houver LLM).

---

## 6. Camada D — LLM router (opcional, perfil `llm_http` no Databricks)

**Gatilho:** `llm_router.enabled` e

\[
score_{\text{cal}} \in [lo,\, hi] \quad \text{(ex. } [0{,}35,\, 0{,}65] \text{)}
\]

Fora da banda → mantém \(fl\) e fonte `hybrid_calibrated`.

**Dentro da banda (`mode: llm`):** modelo devolve JSON `{"relevante": bool}` → define \(fl \in \{0,1\}\) (`llm_router_llm_positive` / `_negative`).

**Falha HTTP / JSON inválido:** `fallback_policy` (ex. `positive_in_band` no piloto) pode forçar \(fl=1\) — impacta assertividade; configurável.

**Importante:** \(fl\) pode **mudar** sem \(n_{\text{pos}}>0\) (ex. esteatose só via LLM) — daí divergências vs legado v2.

---

## 7. Validade estatística (leitura honesta)

| Afirmação | Válido? |
|-----------|---------|
| Pipeline **audítavel** e **reprodutível** com YAML + versão fixa | **Sim** |
| `confidence_score` é **P(relevante \| laudo)** estimada por ML calibrado | **Não** — é score composto por bins, pesos fixos e clip |
| Thresholds (\(\tau\), bandas) têm **garantia frequentista** (α, β, IC) no código | **Não** — são **hiperparâmetros de produto** ajustados em bancada/clínica |
| Comparar `match_rate` com legado mede **acurácia clínica** | **Parcial** — mede **concordância** com outro sistema; adjudicação humana (ex. Carol) é a referência para assertividade |
| LLM com `temperature=0` | Mais estável, mas **não** torna o motor um teste estatístico clássico |
| Embeddings | Similaridade **geométrica** em espaço de frases; limiar \(\tau\) não implica sensibilidade/especificidade conhecidas sem estudo |

**Uso correto do KPI:** homologação = **concordância de decisão 0/1** + análise de discordantes; calibração = ajustar YAML/bandas/prompt até **meta de negócio** (ex. MR ≥ 80% na bancada **e** veredito clínico nos FP/FN).

---

## 8. Hepatologia (config atual — resumo)

- **Relevante rule:** ≥1 achado não negado (esteatose, colelitíase, nódulo, etc.) no léxico YAML.
- **Semântica:** híbrido 70/30, \(\tau \approx 0{,}78\).
- **LLM:** banda 0,35–0,65; contexto “achado hepato-biliar relevante”; muitos discordantes com legado = \(fl_{\text{motor}}=1\) por LLM com esteatose/conclusão explícita.

---

## 9. Referências no repo

- Código: `plataform/nlp_engine/nlp_engine/engine.py`, `scoring.py`, `rule_engine.py`, `semantic_expand.py`, `llm_router_backend.py`
- Contratos: `doc-contrato-engine-rule-based-v0.md`, `doc-llm-router-v0.md`
- Calibração: `notas/calibracao-hepatologia-camadas-v0.md`
