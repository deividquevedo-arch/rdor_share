# Arquitetura e Configuração — xxRADS no NLP Engine (v0)

**Escopo:** global vs por sistema, inputs de config, fluxo do motor com/sem RADS, limites e estado de implementação.
**Fonte de verdade:** `fabrica-ia-lib` (`src/fabrica_ia/nlp_engine/`), wheel `fabrica_ia-0.5.7+`.
**Vinculado a:** `doc-regras-clinicas-rads-v0.md`, `doc-plano-implementacao-rads-extraction-v0.md`

---

## 1. Global (`nlp.rads_extraction`)

- `enabled` — liga/desliga o extractor; `false` ou ausente ⇒ saída idêntica ao motor sem RADS
- `aggregation_policy` — hoje só `max_category`
- `negation.tokens` / `negation.window_tokens` — ou herda `negation_phrases` / `negation_window` globais
- `llm_fallback` — `enabled`, `trigger` (`alias_without_category`), `confidence`; reusa `nlp.llm_router` para conexão

---

## 2. Por sistema (`systems.<prefixo>`)

| Campo | Função |
|---|---|
| `aliases` | Termos do sistema; usados no LLM fallback |
| `categories` | Lista **ordenada** (ranking para `max_category`) |
| `patterns` | Regex com 1 grupo de captura |
| `normalization.roman_to_arabic` | Romanos → arábicos |
| `aggregation_exclude` | Fora do `max_by_system` numérico (ex.: LR-M, LR-TIV) |
| `relevance_policy.promote_categories` | Promovem `fl_relevante=1` e `decision_source=rads_promotion` |

---

## 3. Inputs adicionais

| Input | Uso no RADS |
|---|---|
| Texto tratado (`to_plain`) | Entrada do `extract_rads_summary` |
| `nlp_config` completo | Bloco `rads_extraction` + herança de negação |
| `llm_router` | Conexão do `llm_fallback` |
| `exm_mod` | Passa pelo engine; **não** filtra o extractor hoje |

---

## 4. Promoção a partir de grau X

Sim — via lista explícita em `promote_categories` (sem sintaxe `>=`):

```yaml
relevance_policy:
  promote_categories: ["4","4A","4B","4C","5","6"]
```

---

## 5. Nódulo > X cm

**Fora do escopo RADS V1.** Exige regex de medida no `rule_engine` ou módulo dedicado. O motor captura a **categoria** que o radiologista já atribuiu.

---

## 6. Payload auditável (`exm_laudo_resultado`)

Quando `rads_extraction.enabled: true`:

| Campo | Conteúdo |
|---|---|
| `rads_mentions` | Menções válidas |
| `rads_max_by_system` | Agregação `max_category` por system |
| `rads_invalid_candidates` | Match sem categoria válida (ex.: BI-RADS 9) |
| `rads_llm_errors` | Falhas HTTP do LLM RADS (nunca bloqueiam) |

Flag off ⇒ nenhum campo RADS no payload (byte-identical).

---

## 7. Fluxo do `ClinicalNlpEngine.process`

### Sem RADS

1. TextPipeline (`to_plain`)
2. Segmentação
3. Rule engine
4. Scoring
5. Semantic expand (opcional)
6. Calibração (opcional)
7. LLM router (opcional)
8. Montagem do resultado

### Com RADS

1. TextPipeline
2. **RADS extraction** (regex → normalização → validação → negação → agregação → LLM fallback opcional)
3. Segmentação → Rule engine → Scoring → Embeddings → Calibração → LLM router
4. **Promoção RADS** (`rads_promoted_systems`) — autoritativa quando `fl=0`
5. Montagem + campos auditáveis RADS

---

## 8. Estado de implementação (0.5.7)

| Fase | Estado |
|---|---|
| 0–1 Extractor + config_loader + testes | Entregue |
| 2 Integração engine + contrato | Entregue |
| 3 LLM fallback RADS | Entregue |
| 4 Piloto BI-RADS + bench | Em curso — `configs/nlp/mama/config.yaml`, `notas/s11-birads-paridade-rads-v0.md`, bench sintético |
| 5 LI/PI/TI-RADS + app mama | Configs `hepatologia` (li_rads), `prostata` (pi_rads), `tireoide` (ti_rads); `birads_motor` (coexistência) |
