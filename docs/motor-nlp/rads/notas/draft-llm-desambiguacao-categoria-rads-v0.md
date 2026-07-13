# Draft — LLM-in-the-loop para desambiguação de categoria xxRADS (diferido)

**Status:** DRAFT / diferido (evolução mínima — priorizar expansão RADS antes).
**Motiva:** os 2 erros de **super-agregação** do motor BI-RADS (motor pega o máximo do documento, incluindo categorias de exames anteriores citados). Heurísticas de regex/seção foram tentadas e **regrediram** o baseline (ver `Relatorio-homologacao-birads-bancada-v1.md` §5/§6).

---

## 1. Problema

A política `max_category` agrega o maior BI-RADS de **qualquer parte** do laudo. Quando há citação de exame anterior/comparação com categoria diferente da conclusão, o motor super-agrega. É um problema de **seleção/desambiguação** ("qual das categorias extraídas é a operativa?"), não de extração — regex não resolve sem regressão.

## 2. Tamanho do problema (bancada)

| Amostra | ≥2 categorias (ambíguo) | Motor erra |
|---|---|---|
| Representativa (1000) | 58 (5,8%) | 1 |
| Estratificada (320) | 65 (20,3%) | 1 |

> O motor **já acerta ~56/58 e ~64/65** dos ambíguos (o `max` funciona na maioria). Disparar LLM ingênuo em todo ambíguo pode quebrar os corretos → o ganho não é automático.

## 3. Desenho proposto (sem regressão por construção)

1. **Gatilho estreito** — novo trigger `category_ambiguous` em `rads_extraction.llm_fallback` (hoje só `alias_without_category`). Dispara **apenas** quando há ≥2 categorias distintas não-negadas no documento (~6% no fluxo real). Os ~94% determinísticos ficam intocados → zero regressão neles.
2. **LLM seleciona, não gera** — saída restrita às **categorias já extraídas** do texto (escolher a operativa entre os candidatos). Invariante verificável: resposta ∈ candidatos. Elimina alucinação.
3. **Shadow antes de override** — modo consultivo: registra `decision_source=llm_disambig`, candidatos e confiança **sem sobrescrever** o `max`. Mede na bancada/HML se conserta os 2 sem quebrar os ~120 ambíguos corretos. Override só após evidência de ganho líquido.

## 4. Infra já existente (reaproveitar)

- `llm_router_backend.call_openai_compatible_chat` + plumbing de `_llm_connection_cfg`.
- Auditoria: `decision_source`, `llm_called`, `llm_error` (já na saída).
- Runner deriva `llm_base_url` dos serving endpoints do workspace (HML/PRD).

## 5. Caveats

- **Não validável na bancada local** (`rule_only`, sem endpoint) — exige HML ou LLM mockado.
- Custo/latência/não-determinismo em ~6% do volume → telemetria + cache.
- Precisa de invariante: resposta do LLM ∈ candidatos extraídos.

## 6. Próximo passo (quando reaberto)

Prototipar trigger `category_ambiguous` + prompt restrito a candidatos + modo shadow com telemetria; rodar em lote de HML; promover a override só com ganho líquido medido.

> **Decisão atual:** diferido. Os 2 casos de super-agregação ficam como limitação conhecida (motor 99,8%+). Prioridade: expansão xxRADS (PI/TI/LI-RADS).
