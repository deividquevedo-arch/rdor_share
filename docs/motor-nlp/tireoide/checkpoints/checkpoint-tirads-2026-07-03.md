# Checkpoint TI-RADS / linha de cuidado Tireoide — 2026-07-03 (retomar segunda 07-07)

Sucede o run E2E v9 D=2026-06-29 e a homologação manual dos 65 relevantes < TR4.
Ver memórias [[xxrads-status-e-bancada-ab]], [[tireoide-linha-cuidado-story-v1v2]], [[camada-criterios-quantitativos]].

## Estado dos repos (LOCAL, nada pendente de push além do já dito)

- **Lib `nlp-engine-lib`:** `v0.2.1` **PUSHADA + PR mergeado em `hml`** → wheel `nlp_engine-0.2.1` **online no Volume** `/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib`. Contém o **fix do footer** ("(valor de referência:)"). Branch `fix/footer-referencia-inline`.
- **Plataforma `fabrica-ia-plataforma`, branch `test/rads-e2e-hml`:** commit LOCAL `c902146` = **fix de negação (gatilhos "não se identificam/observam/…")**, `negation_window` mantido em 5. **NÃO PUSHADO** → o Databricks Repos ainda roda config **v9** (`f950506`), sem esse fix. **Bricks está em v9.**
- Runner E2E: `apps/databricks/nlp_engine/ntb_ia_motor_e2e.py` (widgets: specialty=tirads, data_execucao_modelo=2026-06-29, perfil_motor=rule_only, nlp_engine_version=0.2.1/latest, fonte_staging=legado).

## Run E2E v9 (footer ativo, negação NÃO) — métricas reais (895 exames)

- **Captura de categoria** (universo cat_legado≥1, n=228): **228/228 = 100%**; exact-match 227/228 (99,56%); MISS=0 (footer fix resolveu os "(valor de)").
- **Relevância vs legado:** TP=67 FP=66 FN=0 TN=762 → acurácia 0,926 · precisão 0,504 · **recall 1,000** · F1 0,670 · especificidade 0,920 · MCC 0,681. **só-legado-rel=0** (era 4). llm_called=0.
- Os 66 "FP" = majoritariamente **achados benignos reais desejados na V1** + 2 motor-wins; erro real ≈ 3-4 FP de negação. Legado NÃO é gabarito (só-RADS).

## Homologação manual dos 65 relevantes < TR4 — GAPS a resolver

Categoria RADS e achado principal coerentes em todos. Gaps são de recall-secundário/contagem/negação/medida:

- **G1 — Associação órgão↔achado restritiva:** perde achado sem "tireoide" próximo. `...231037`/`...231038` (linfonodo cervical suspeito nível VI, TC pescoço) e `...47645128` ("Bócio unidolular mergulhante" — "unidolular" se interpõe entre "bócio" e "mergulhante", e conclusão curta sem órgão-âncora). Recorrente em TC pescoço/conclusões. → rule engine (âncora de órgão / linfonodo cervical relevante fora de "tireoide").
- **G2 — Contagem de lesões ≠ menções:** `n_positive_spans` conta spans (Análise + Conclusão = 2), não lesões distintas. `...10172776` (4 nódulos reportados, reais = 2 + 1 linfonodo); cisto contado 1× quando aparece 2× (`...4004762`, `...7908639`). → resolver na **camada quantitativa** (raciocínio por lesão/dedupe).
- **G3 — Negação coordenada/distante (FP):** `...2774034/35` ("Ausência de … nódulos massas" pós-tireoidectomia); `...147724` ("Não se identificam nódulos ou lesões focais" — nega nódulo mas "lesões focais" sobrevive). Fix config de negação (c902146) resolve só o adjacente → escopo de lib pendente (coordenação + só-à-esquerda).
- **G4 — >1cm / medidas (V2):** `...48106250` (nódulos 2,3 e 2,7 cm), `...7908637` (cisto 1,1 cm), etc. → **camada quantitativa** (prioridade do head).
- **G5 — Léxico "nodular" adjetivo:** "imagem/formação nodular" positivas não capturadas (regex só casa substantivo "nódulo"). Fix léxico + negação (formas "não foram visualizadas/detectadas", "não se observando") — **acoplados**, aplicar juntos (~5-10 misses vs ~12 FP se desacoplar).
- **G6 — "lesão expansiva sólida" como hiperônimo:** motor trata "lesões expansivas" (mesmo negadas/em outro órgão) como nódulo/massa (`...10172776`). Decorre de G1+G3.

## Prioridade combinada (decisão do usuário: corrigir o que funciona antes de evoluir p/ V2)

1. **Fix léxico "nodular" (G5) + negação passiva/coordenada (parte de G3)** — determinístico, simples, refino do que já roda. Aplicar léxico + gatilhos "não foram visualizadas/detectadas/observando" JUNTOS. (Config + eventual escopo de negação na lib.)
2. **Camada quantitativa (G4, + resolve G2)** — spec pronto: `docs/motor-nlp/doc-spec-camada-criterios-quantitativos-v0.md`. Piloto **nódulo >1cm**, extração por LLM row-level (Databricks Haiku, in-tenant), limiar em código, **sem cache**, `on_met` (annotate_only→gate_relevance). Decisões pendentes: `on_met` do piloto; ordem.
3. **G1 (associação órgão)** — investigar `finding_organ_max_chars`/âncora; linfonodo cervical relevante.
4. Push do `c902146` + re-run E2E quando fizer sentido agregar os fixes.

## Decisões de arquitetura já fechadas

- Camada quantitativa = **LLM row-level opt-in, config-driven**; extração no LLM (sem engenharia de padrões), **limiar/lógica composta em código**; gating por âncora+tipo de exame; `temperature=0`, in-tenant Haiku; sem cache.
- `llm_http` (padrão-ouro da lib) entra **depois** de arrumar o rule engine; fará "polimento" + é o veículo da camada quantitativa e do desambiguador de negação.
- 🔐 Rotacionar a chave OpenAI colada no chat (pendência do usuário).

## Primeiro passo na segunda
Confirmar com o usuário: começar pelo **fix léxico+negação (item 1)** OU já ir para o **piloto da camada quantitativa (item 2, on_met=annotate_only)**. Ambos são "corrigir/evoluir o que já funciona" — o item 1 melhora a base sobre a qual o item 2 opera.

---

## 2026-07-06 — ITEM 1 APLICADO LOCAL (config-only, validado) ✅

Usuário escolheu **item 1**. Feito no `ntb_ia_tirads_config.py` (fonte-de-verdade do runner), **v9→v10**:
- **G5 léxico:** `findings_regex.nodulo` += `\b(?:imagem|imagens|formac[aã]o|formac[oõ]es|les[aã]o|les[oõ]es)\s+nodular(es)?\b`. Casa "imagem/formação/lesão nodular"; NÃO casa "multinodular" (sem `\b` interno).
- **G3 negação (parte config-only):** `negation_phrases` += família **passiva** (`não foram/foi <particípio>` ×gênero/número p/ visualizar/detectar/identificar/observar/caracterizar/evidenciar) + **gerúndio** (`não se observando/identificando/caracterizando`) + **presente singular** (`não se observa/identifica/caracteriza/visualiza(m)`) → 43 frases. `negation_window` MANTIDO em 5.
- **YAML** (`configs/nlp/tireoide/config.yaml`, v8, só alimenta A/B de categoria): negação espelhada + nota (`.py` = fonte-de-verdade da camada rule-engine/achados). Não adicionei `findings_regex` parcial (evita híbrido inconsistente).

**Validação empírica** (harness `process_rule_based` na CONFIG real do `.py`): G5 recupera **4/4**, G3 nega **5/5** (baseline 0/5), guardas positivos OK. Teste de regressão novo na lib `tests/nlp_engine/test_tireoide_findings_synthetic.py` (13 casos) → **suite lib 200 passed** (era 187).

**Resíduo conhecido (bounded):** janela BIDIRECIONAL nega nódulo REAL seguido na MESMA sentença de "…, linfonodos não foram detectados" (mesma classe do "sem"). Resolução durável = negação **só-à-esquerda na lib** (deferida, mudança de lib).

**Descoberta (raiz do G1):** `_organ_spans` (proximidade) usa só seeds+nome-do-órgão e IGNORA o `regex` do órgão que `_mentions_target_organ` usa → "tireoidiano" passa em menção mas falha em proximidade.

**Git plataforma:** `test/rads-e2e-hml`, HEAD `c902146`; os 2 configs **modificados, não commitados**. **Pendente (decisão do usuário):** commit local + push + re-run E2E D=2026-06-29 p/ confirmar recuperação. Bricks ainda roda v9 (sem c902146 nem v10).
