# Plano de execução — P0 do `nlp_engine` (integrar DecisionState + consolidar duplicação)

**Escopo:** SÓ `nlp_engine` (DS). Tudo byte-compat, com **gate golden diff = 0** (harness
`.claude/jobs/56d76a3e/tmp/regress_golden.py`: tirads 895/`39dc3da6`, hepato 100/`b4dc2076`,
pirads 200/`d648e5b`) + suíte + ruff/format/mypy a cada passo. Nada de comportamento novo aqui
(evolução = P1/Etapa 2, separada).

---

## Situação atual (por que P0)
- **Linha de release** (`feat/quantitative-age-conditioning`, wheel **0.4.1**): `engine.py` **monolítico**
  (cascata com 7 mutações de `fl`) + `quantitative.py` com `extraction_hint` (0.4.0) e `threshold_by` (0.4.1).
- **Refactor DecisionState** (`refactor/decision-state-pipeline`): `engine.py` fino + `decision_pipeline.py`
  (steps puros + registry), **byte-compat golden-3/3 — porém partindo do hml 0.3.16** (NÃO tem
  extraction_hint/threshold_by). Está **isolado, não integrado**.
- **Risco:** as duas linhas divergem. `engine.py` na hml **não mudou** entre 0.3.16→0.4.1 (as mudanças
  0.4.x foram em `quantitative.py`), então a integração é limpa (bases de `engine.py` idênticas).

---

## P0-A — Integrar o refactor DecisionState na linha de release

**Objetivo:** trazer `engine.py`-fino + `decision_pipeline.py` (com o step-registry) para cima da hml
atual (0.4.1), de modo que `decision_pipeline.step_measure` chame o `quantitative.py` **0.4.1**
(extraction_hint + threshold_by) sem mudar assinatura.

**Passos:**
1. `git fetch origin hml` → branch `refactor/decision-state-pipeline-on-0.4.1` **a partir de origin/hml**.
2. **Rebase / cherry-pick** os 2 commits do refactor (`01d0be7` engine→pipeline, `d56844e` registry) sobre
   a hml 0.4.1. Conflito esperado = **nenhum** em `engine.py` (base idêntica) e `decision_pipeline.py` é
   arquivo novo. `quantitative.py`/`rule_engine.py` da 0.4.1 ficam intactos (o pipeline os chama).
3. **Sanity-check ANTES** (harness na hml 0.4.1 pura) = golden 3/3 OK (prova que o gate é fiel à 0.4.1).
4. Rodar o harness **DEPOIS** do rebase → **golden 3/3 idêntico** (o pipeline reproduz a cascata; a
   ordem dos steps == ordem do monólito). Suíte + ruff/format/mypy.
5. Conferir que `decision_pipeline` cobre os campos 0.4.x: `step_measure` → `process_quantitative_criteria`
   (mesma assinatura, agora com threshold_by/extraction_hint dentro do quantitative). `active_step_names`
   inclui `measure` quando há `quantitative_criteria`. (Nada a mudar — é chamada, não reimplementação.)
6. **Release:** bump 0.4.1 → **0.5.0** (refactor interno byte-compat = MINOR conservador, ou 0.4.2 patch;
   preferir 0.5.0 pela mudança estrutural), RELEASE.md, uv.lock, tag. PR → hml (MLOps publica o wheel).
7. Configs por especialidade: **nenhuma mudança** (byte-compat). `nlp_engine_version` sobe p/ 0.5.0
   quando MLOps publicar.

**Critério de aceite:** golden 3/3 + suíte verde + `engine.py` ~55 linhas + só `ClinicalNlpEngine`
exportado (helpers migraram p/ `decision_pipeline`). O E2E de produção (CSV) permanece idêntico.

---

## P0-B — Consolidar duplicação (sobre a base modular do P0-A)

**Objetivo:** um único ponto de verdade p/ o que hoje está copiado. Cada item é byte-compat isolado
(golden 3/3 a cada commit). Ordem do menor risco → maior.

1. **`_util.py`** — mover `_as_mapping`, `_as_str_list`, `_clip01`, `_in_band` (idênticos em
   engine/llm_router/decision_pipeline; `_as_mapping` tb em quantitative/rads). Importar de `_util`.
   Gate: golden 3/3 (funções puras, resultado idêntico).
2. **1 parse-JSON-LLM** — extrair `parse_llm_json(content)` em `llm_router_backend` (tolerante a cerca/
   texto) e substituir as 4 cópias (`_parse_llm_json`, `quantitative.parse_extraction`/`parse_qualitative`,
   `rads._parse_rads_llm_category`). **Cuidado:** os schemas diferem (measures vs relevante vs category)
   — o helper devolve o **dict** genérico; cada chamador extrai seus campos. Unificar o regex
   (`\{.*\}` DOTALL) e testar contra os payloads reais de cada um. Gate: suíte (os testes de parse já
   existem) + golden 3/3.
3. **`_safe_format`** — unificar `_safe_format_template` (router) e `_safe_format` (rads) em `_util`.
4. **Acento** — `quantitative._strip_accents` → reusar `text_pipeline.norm` (ou expor um `strip_accents`
   em norm). Validar que NFKD vs NFD não muda a canonização de unidade (testar `_canon_unit`).
5. **Unidade** — fonte única de tabela de unidades (hoje `to_plain._UNITS` vs `quantitative._canon_unit`).
   Avaliar se dá p/ unificar sem mexer no `to_plain` (que é usado por todos) — se arriscado, **deixar
   documentado e adiar** (menos-é-mais: não forçar).
6. **spaCy singleton** — 1 sentencizer pt compartilhado (hoje `engine._SEG_NLP` + `rule_engine._NLP`).
   Mover p/ um provedor único (ex.: `text_pipeline`) com cache. Gate: golden 3/3 (segmentação idêntica).
7. **Fallback de versão** — unificar `engine._package_version` ("0.5.6") e `__init__` ("0.0.0") num só.
8. **`config_loader` → `rads_extraction`** — mover `LLM_FALLBACK_TRIGGERS` p/ lugar neutro (`_util`/
   `contracts`), quebrando a dependência invertida.

**Regra:** cada item = 1 commit, 1 gate (golden 3/3 + suíte). Se algum não fechar diff-0, **reverte e
adia** (não empurrar). Itens 5/6 são os de maior risco — fazer por último, e adiar se não forem limpos.

**Critério de aceite:** zero duplicação nos itens 1-4/7-8; menos linhas na wheel; golden 3/3; suíte;
nenhuma mudança de comportamento.

---

## Sequência e gates
```
P0-A (integrar refactor)  ──[golden 3/3 + suíte]──► release 0.5.0 (PR→hml, MLOps publica)
        │
        ▼
P0-B (consolidar sobre a base modular)  ──[golden 3/3 por item]──► release 0.5.1
```
- **Rede de segurança:** o harness golden roda os 3 configs em rule_only determinístico; qualquer
  regressão aparece como SHA diferente antes de qualquer merge.
- **Fora do P0 (P1):** `llm_connection` (extrair do llm_router), validação de config simétrica,
  Etapa 2 (semântica→finding, opt-in, valida vs base ouro), pré-compilar regex, determinismo RADS.
- **Hand-off MLOps:** engine_version na Delta, perfil llm_measure, column_map, catálogo.
