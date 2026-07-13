# Plano de Implementação — RADS Extraction no NLP Engine (v0)

**Status:** SPEC aprovada em 2026-06-12 (origem: `notas/linha-evolucao-xxrads-nlp-engine-v0.md`). Atualizado em 2026-06-12 com o estado real da wheel `fabrica_ia-0.5.6`.
**Backlog:** Fases 0–3 fechadas na lib (`0.5.7`); Fase 4 (piloto BI-RADS) em curso — referência `S10` / evolução xxRADS.

## 0. Estado real entregue (wheel `fabrica_ia-0.5.6`)

O motor já está **completo e em produção** na wheel 0.5.6. RADS é a única peça greenfield.

| Componente | Estado |
|---|---|
| `text_pipeline` (to_plain, RTF/HTML, acentos, anchors, headers, negação, boilerplate, footer) | ✅ Entregue |
| `rule_engine` (spaCy Matcher + regex + proximidade órgão-achado) | ✅ Entregue |
| `scoring` (rule / hybrid / `calibrated_meta`) | ✅ Entregue |
| `semantic_expand` (embeddings opcional; fallback token_overlap) | ✅ Entregue |
| `llm_router_backend` (banda de incerteza + `fallback_policy: positive_in_band`, novo na 0.5.6) | ✅ Entregue |
| `contracts` + `output_invariants` (contrato versionado) | ✅ Entregue |
| `nlp_platform.batch` (setup/entrada/process/homolog/monitoring/distribuição — **genérico multi-especialidade** via `SpecialtyConfig`) | ✅ Entregue |
| `data_manager` (io, gold, distribuição) | ✅ Entregue |
| Hepatologia homologada (cenário Carol: Recall 100% vs 21,7% legado) | ✅ Entregue |
| **RADS extraction** (`rads_extraction.py`) | 🟡 **Fases 0–3 entregues** (`fabrica_ia-0.5.7`, 55 testes `tests/nlp_engine`); pendente Fase 4–5 |

> ⚠️ **Fonte de verdade (definida pelo time, 2026-06-12): `fabrica-ia-lib` é canônica.** A wheel **`fabrica_ia-0.5.6`** é a verdade absoluta — é o que está de fato em uso pela plataforma. O repo remoto `fabrica-ia-lib` foi atualizado e o `nlp_engine` da lib (`src/fabrica_ia/nlp_engine/`) é rastreado e recebeu evolução direta (ex.: PR 6567 — fallback policy, erro HTTP, parse JSON do LLM; `quality_guard.py`, lib-native). A bancada **`plataform/nlp_engine`** está **defasada vs v0.5.6** e **não é fonte**: rodar `sync_nlp_engine_from_plataform.py` a partir dela **regrediria a lib**. **Implementar RADS direto na `fabrica-ia-lib`** (como `quality_guard.py`); o realinhamento da bancada é tema separado e não é pré-requisito (ver R1).

## 1. Decisões fechadas

| Decisão | Resolução |
|---|---|
| Escopo | Capacidade **global e genérica** do `nlp_engine` (qualquer xxRADS); promoção de `fl_relevante` explícita por config |
| Repo / fonte | Implementar em **`fabrica-ia-lib`** (`src/fabrica_ia/nlp_engine/`) — fonte canônica (wheel 0.5.6). Lib-native, como `quality_guard.py` |
| Piloto | **BI-RADS** — paridade contra legado `algoritmos/birads` (PRD via API), reusando `nlp_platform.batch.run_homolog` |
| LLM fallback | **Na V1**, dentro do fluxo completo atual — reusa `llm_router_backend`, só na banda de incerteza |

## 2. Base legada a absorver

| Origem | O que absorver | Onde entra |
|---|---|---|
| `birads` (`ntb_ia_predicao.py:292-379`, regex+NLTK) | Normalização de romanos; categorias válidas (0–6, 4A/4B/4C); agregação por máximo; limpeza de ruído (ACR/edição, dimensões, datas) | `normalization` + `aggregation_policy` no YAML |
| `birads` | Janela de contexto keyword→categoria. **Falha conhecida: sem negação** ("BI-RADS 4 descartado" conta 4) | regex com janela configurável |
| `hepatologia` (`ntb_ia_hepatologia_algoritmo.py:585-739`, spaCy) | Negação por janela de tokens; whitespace flexível / variações OCR ("lirads:4"→"lirads 4") | reuso de `negation_phrases`/`negation_window`; normalização pré-match |
| motor atual (0.5.6) | `to_plain` (RTF/HTML/acentos), segmentação, contrato versionado | extractor recebe texto já tratado pelo TextPipeline |

> Hoje: LI-RADS existe só como termo em `findings.lesao_focal` no config de hepatologia da **bancada** (0.1.12) — a lib v0.5.6 não embarca configs de especialidade (`configs/` tem apenas o template genérico). Não há extração estruturada de categoria em lugar nenhum.

## 3. Arquitetura

Módulo isolado, sem dependência de `data_manager`, `monitoring` ou notebooks:

```
fabrica-ia-lib/src/fabrica_ia/nlp_engine/rads_extraction.py
fabrica-ia-lib/tests/nlp_engine/test_rads_extraction.py
fabrica-ia-lib/tests/nlp_engine/test_engine_rads_integration.py
```

> Acrescentar o export em `nlp_engine/__init__.py` (`extract_rads_mentions`, `RadsMention`) e ao `__all__`. Módulo lib-native (mesmo tratamento de `quality_guard.py`): se o sync da bancada for executado algum dia, incluir `rads_extraction.py` na lista de preservação por nome.

### Fluxo

```
texto tratado (to_plain) → extract_rads_mentions(text, nlp_config)
  1. regex patterns por system → candidatos
  2. normalização (romanos, separadores, aliases OCR) → categoria canônica
  3. validação contra categories declaradas (inválida → descartada + auditada)
  4. negação (negation_window + negation_phrases)
  5. alias sem categoria E llm_fallback.enabled → llm_router_backend (source="llm")
→ rads_mentions: [{system, category, confidence, source, matched_text, negated}]
→ relevance_policy (opt-in): promote_categories ⇒ fl_relevante=1, decision_source="rads_promotion"
→ payload aditivo em exm_laudo_resultado.rads_mentions
```

### YAML

```yaml
nlp:
  rads_extraction:
    enabled: true
    aggregation_policy: max_category    # máximo segue a ORDEM da lista `categories`
    llm_fallback:
      enabled: true
      trigger: alias_without_category   # nunca obrigatório
    systems:
      bi_rads:
        aliases: ["BI-RADS", "BIRADS", "BI RADS", "ACR BI-RADS", "categoria"]
        categories: ["0","1","2","3","4","4A","4B","4C","5","6"]   # ordem = ranking p/ max
        # Captura AMPLA (\d) p/ auditar fora-de-faixa (regra do "9"); validação descarta inválidas.
        # Alternância ORDENADA do mais específico p/ o menos: subcategoria antes de \d; iv/vi antes de v/i{1,3}.
        patterns: ["(?:BI[- _]?RADS|BIRADS|categoria)[\\s:.°-]*(\\d(?:\\s?[ABC])?|iv|vi|v|i{1,3})"]
        normalization: { roman_to_arabic: true }
        relevance_policy: { promote_categories: ["4","4A","4B","4C","5","6"] }
      li_rads:
        aliases: ["LI-RADS", "LIRADS", "LR"]
        categories: ["LR-1","LR-2","LR-3","LR-4","LR-5","LR-M","LR-TIV"]
        patterns: ["(?:LI[- ]?RADS|LIRADS|LR)[\\s:.-]*((?:LR[- ]?)?[1-5]|M|TIV)"]
        aggregation_exclude: ["LR-M","LR-TIV"]   # promovem fl_relevante, mas FORA do max numérico
```

### Contrato (aditivo)

- `contracts.py`: `RadsMention` (TypedDict) + campo opcional `rads_mentions` em `ExmLaudoResultadoPayload`.
- `output_invariants.py`: validar `rads_mentions` quando presente (system declarado, category válida, confidence ∈ [0,1], source ∈ {regex, llm}).
- Novo `decision_source`: `rads_promotion`.
- Campos materializados (`rads_system`, `rads_category`) **fora da V1** — só se serving/analytics pedir.

## 4. Fases

| Fase | Entrega | Gate |
|---|---|---|
| 0 | SPEC fechada + criar `rads_extraction.py` **na lib** (`fabrica-ia-lib`) lib-native; registrar na lista de preservação do sync (como `quality_guard.py`) | Módulo presente na build da wheel; sync não o apaga |
| 1 | ✅ **Feito** — `rads_extraction.py` (`extract_rads_mentions`/`extract_rads_summary`/`RadsMention`): normalização (romanos, separadores, prefixo comum p/ LR-*), validação contra `categories` (inválida → auditada), negação (window, reuso de `is_negated_in_sentence_plain`), agregação `max_category` com `aggregation_exclude`; `_validate_rads_extraction_shape` no `config_loader.py` (regex compila + exige grupo de captura); export no `__init__.py` | ✅ `pytest tests/nlp_engine` 32 passed + ruff + mypy verdes (branch local `feature/nlp-engine-rads-extraction`) |
| 2 | ✅ **Feito** — Integração no `engine.py`: `extract_rads_summary`, `attach_rads_audit_fields` (`rads_mentions`, `rads_max_by_system`, `rads_invalid_candidates`, `rads_llm_errors`); promoção `rads_promotion`; `contracts`/`output_invariants` estendidos | ✅ Flag off ⇒ saída idêntica; 55 testes `tests/nlp_engine` |
| 3 | ✅ **Feito** — LLM fallback (`_llm_fallback_for_system`) reusando `call_openai_compatible_chat`; trigger `alias_without_category` (alias presente sem categoria resolvida pelo regex); prompt JSON `{system, category}`; conexão via `nlp.llm_router` + override `llm_fallback.llm`; erro HTTP ⇒ menção descartada + `summary["llm_errors"]`, nunca bloqueia; categoria inválida/abstain ⇒ `invalid_candidates` com `source="llm"`; mención `source="llm"` entra em `max_by_system` e `relevance_policy` | ✅ Testes com LLM mockado (ok, erro HTTP, JSON inválido, fora-de-faixa, alias já resolvido não chama LLM, negação) |
| 4 | Piloto BI-RADS: config + bench vs `vl_proced_birads` (`ia.tb_diamond_mod_birads_saida`) **reusando `nlp_platform.batch.run_homolog`**; classificar divergências (bug / melhoria por negação / regra do "9") | Protocolo `doc-playbook-global-evolucao-paridade-v0.md` (baseline freeze, FN deep-dive, McNemar, 2 rodadas estáveis) |
| 5 | Generalização: LI-RADS (migrar termo→sistema), PI-RADS, TI-RADS. App `birads_motor` = **`SpecialtyConfig` + notebook chamando as facades de `nlp_platform.batch`** (não é app novo), em coexistência com legado | Sem cutover |

## 5. Riscos

| # | Risco | Sev. | Mitigação |
|---|---|---|---|
| R1 | Sync a partir da bancada defasada sobrescreve `rads_extraction.py` lib-native e regride a lib | **Alta** | RADS lib-native; incluir na lista de preservação por nome do sync (igual `quality_guard.py`); bancada **não** é fonte e seu realinhamento é tema à parte |
| R2 | Paridade: legado emite LONG único (máximo + regra "9"); payload novo é lista | **Alta** | Mapeamento no bench; piloto em shadow, sem tocar API do legado |
| R3 | Negação muda resultado vs legado ("BI-RADS 4 descartado" = 4 no legado) | Média | Melhoria documentada; flag para desativar negação se a paridade exigir |
| R4 | LLM fallback: custo/latência/erro em serving | Média | Trigger restrito, nunca bloqueia, `llm_called/llm_error` auditados |
| R5 | Quebra de consumidores de `exm_laudo_resultado` | Média | Campo aditivo opcional; flag off ⇒ saída idêntica (regressão byte a byte) |
| R6 | Conflito entre sistemas no mesmo laudo | Baixa | Menções coexistem; promoção só pelo sistema declarado da especialidade |

## 6. Critérios de aceite (V1)

1. `pytest` (incl. suíte RADS) + ruff + mypy verdes em `fabrica-ia-lib/tests/nlp_engine`.
2. `rads_extraction` ausente/`enabled: false` ⇒ saída **idêntica** à atual.
3. `rads_mentions` validado por `output_invariants` quando presente.
4. Zero leitura de arquivo/notebook/PHI no extractor e nos testes.
5. Bench de paridade BI-RADS com baseline congelado e divergências classificadas.
6. Promoção de `fl_relevante` só quando declarada no YAML.
