# Hepatologia — alinhamento config YAML (v2 + documento clínico) (v0)

**`config_version`:** `0.1.12-hep-llm-prompt-clin` (sucede `0.1.11-hep-v2-clin-doc-parity`)

## Revisão 0.1.12 — prompt LLM + fallback (pós dia3 / obs. Carol)

**Motivo:** homolog dia3 (YAML 0.1.11) mostrou que **~100% dos positivos vêm do LLM** (`llm_router_llm_positive`, todos com score de regra <0,5 → banda de incerteza). O léxico calou a regra; o FP migrou para o LLM. Logo, o ajuste de léxico não reduz FP — o decisor é o prompt do LLM.

**Mudanças (config-only, isola efeito do prompt):**

| Campo | Antes (0.1.11) | Agora (0.1.12) |
|-------|----------------|----------------|
| `llm_router.specialty_context` | "relevante=true se achado hepato-biliar relevante; negação/normalidade → false" | Critério clínico explícito (Carol): true só para doença que justifica encaminhamento; **false** para fígado normal, benignos (cisto simples, hemangioma típico), inespecíficos; **esteatose sempre relevante** |
| `llm_router.prompt_system` | JSON-only genérico | Papel de triagem clínica + JSON-only |
| `llm_router.fallback_policy` | `positive_in_band` | `keep_current` (em falha do LLM não inventa positivo) |
| `uncertainty_band` | `[0.35, 0.65]` | **inalterada** (P1 fica para próxima iteração, se prompt não bastar) |
| `findings` / léxico | — | **inalterado** vs 0.1.11 |

**Próximas alavancas (não aplicadas, aguardam evidência do dia4):**
- P1 banda/score: tirar laudos só-negação (`rule_score=0.35`) da banda para nem chegarem ao LLM.
- P2 singletons/negação na regra; escopo biliar V3.

---


**Fontes (ordem de precedência para léxico rule-based):**

1. Documento clínico do time (lista activa, removidos, em estudo).
2. Notebook `algoritmos/hepatologia_NOVO/model/ntb_ia_hepatologia_algoritmo_v2.py` (`problemas_figado_novo`, `palavras_chave_laudo`, `palavras_irrelevantes`).
3. Revisão DS com formação médica — decisões abaixo assinaladas.

---

## Lista activa (documento + v2)

Termos como no algoritmo v2, **excepto** itens barrados pelo documento clínico.

**Novos (documento, verde):** `figado com ecogenicidade aumentada`, `hiperplasia nodular regenerativa` (+ alias typo `hiperpasia` nos laudos).

**Mantidos como termo solto:** `contorno irregular`, `fibrose`, `lobulado` (v2 usa matcher literal; motor deixa de depender só de regex contextual).

---

## Removidos (documento — não usar)

| Termo | Estado no YAML 0.1.11 |
|-------|-------------------------|
| `insuficiencia hepatica alcoolica` | Removido |
| `necrose hemorragica` | Removido |
| `reduzido` | Removido (já ausente) |
| `microlitiase` | Removido |
| `multiplos calculos` | Removido |

---

## Em estudo (time IA — não activar)

| Termo | Estado no YAML 0.1.11 |
|-------|-------------------------|
| `figado contorno serrilhado` | Removido (estava no v2; documento: em estudo) |

---

## v2 no notebook mas fora do documento clínico

| Termo | Decisão DS / produto |
|-------|----------------------|
| `metavir f3`, `metavir f4` | **Removidos** do YAML 0.1.11 — não constam no documento clínico fechado; reintroduzir só com sign-off médico. |

---

## Enxugamento vs 0.1.10 (pack exploratório)

Removidas categorias/termos que **não** existem na lista v2+clínica:

- `lesao_focal` genérico (`nodulo`, `lesao focal`, …) — mantidos só `nodulo hipervascular`, `lirads 4`, `lirads 5`.
- `colelitíase`, `cisto`, `colangite` e expansões de esteatose/dilatação fora do doc.
- `findings_regex` largos (nodulo contextual, esteatose ampla, METAVIR, morfologia com `serrilhado`).

**Mantido em regex:** variantes ortográficas `hiperplasia` / `hiperpasia`.

---

## Negação (paridade v2)

| Campo | Legado v2 | YAML 0.1.11 |
|-------|-----------|-------------|
| Frases | `ausencia`, `nao ha` | `ausencia`, `nao ha` + variantes mínimas (`sem`, `ausencia de`) |
| Janela | 3 tokens | `negation_window: 3` |

---

## O que **não** mudou nesta versão

- `target_organs`, órgãos/palavras-chave hepato-biliares.
- Embeddings híbridos + `llm_router` (comportamento além da regra; homolog `llm_http` continua possível).
- Para comparar **só léxico vs legado**, usar perfil `rule_only` na bancada.

---

## Verificação local

```powershell
Set-Location plataform\nlp_engine
python scripts\_audit_yaml_vs_legado_hep.py
pytest tests/test_hepatologia_lexicon_clin_doc.py -q
```

---

*Task: S03 T03.3 (CONFIG em YAML) + S06 homolog. Ficheiro canónico: `plataform/nlp_engine/configs/hepatologia/config.yaml` (espelho em `fabrica-ia-plataforma/configs/nlp/hepatologia/`).*
