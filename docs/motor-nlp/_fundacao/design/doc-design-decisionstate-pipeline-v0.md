# Design — pipeline `DecisionState` (refactor do fluxo do motor) · GLOBAL/agnóstico

**Objetivo:** reorganizar o `engine.process` (cascata monolítica) num **pipeline de steps puros sobre um
`DecisionState`**, com **fases explícitas**, **ordem de execução** e **liga/desliga por step** (perfil /
feature-flag). **Refactor byte-compat** (zero mudança de comportamento) — valida com golden diff = 0.
Motor **agnóstico**: tireoide/hepato/pirads são apenas *configs*. Esboço para revisão antes de tocar
em `engine.py`.

---

## 1. `DecisionState` (objeto único que TODO step lê/escreve)

```
DecisionState:
  # entrada
  id_exame, raw_laudo, treated_text, exam_meta{modalidade, tipo, data}, nlp_config, profile

  # evidência (steps de extração preenchem)
  findings: [ {cat, via(rule|regex|matcher|semantic), trecho, negated, excluded, span, score?} ]
  categories: {mentions, max_by_system}      # RADS/BI/PI/TI… (genérico: "categorias")
  measures:   [ {finding_ref, criterion, value, unit, met, source} ]

  # scores
  rule_score, semantic{score, matched_term, matched_sentence, backend}, calibrated_score
  llm{called, mode, verdict, error, band_hit}

  # decisão (evolui no pipeline)
  fl, decision_source, demoted_by[], trail[]   # decision_trail = auditoria (achado·via·decisão·trecho)
```

Cada step lê/escreve o state; nunca recomputa o que outro já fez; testável isolado.

## 2. Três FASES (ordem revisada — evidência ANTES de medir)

### A · EVIDÊNCIA (coleta tudo que o laudo diz — determinístico + semântico)
| step | faz | liga quando |
|---|---|---|
| `treat` | normaliza texto (to_plain) + segmenta | sempre |
| `extract_categories` | categorias estruturadas (RADS/BI/PI/TI…) | `rads_extraction.enabled` |
| `extract_findings` | achados por REGRA (léxico/regex/matcher) + **negação (dir. por-achado) + exclusão + ignore_sections** | `rule_engine` |
| `expand_semantic` | recupera achados por **similaridade** (embedding) onde a regra falhou → adiciona findings `via=semantic` | perfil ∈ {hybrid, llm_http} |

→ ao fim da fase A, `findings[]` tem TODOS os achados (regra **e** semânticos), cada um com proveniência.

### B · MEDIÇÃO + SCORE (quantifica e pontua a evidência já coletada)
| step | faz | liga quando |
|---|---|---|
| `measure` | critérios quantitativos (ex.: dimensão ≥ limiar) sobre os findings | `quantitative_criteria` |
| `score` | score calibrado a partir da evidência | `rule_engine` |

**`measure.applies_to` = `[rule]` (default) · `[rule, semantic]` (opt-in).** Medir achado **semântico**
é consistente (achado é achado) e faz o gate quantitativo valer para ele também; é **opt-in** porque
match semântico é mais fuzzy — se a medida não for clara no `matched_sentence`, `met=None` (não gateia,
conservador). Habilitado por config, validado contra golden.

### C · DECISÃO (combina, rebaixa, julga a incerteza, guarda)
| step | faz | pode mudar `fl`? | liga quando |
|---|---|---|---|
| `aggregate` | fl preliminar = OR dos achados relevantes ∨ categoria promovida | **define** | sempre |
| `gate` | rebaixa 1→0 se todos os drivers são gated & medida não atingida | **rebaixa** | `on_met=gate_relevance` |
| `llm_judge` | LLM-juiz **só na banda de incerteza**: keep/demote/promote (`fallback_policy`) | keep/demote/promote | perfil == llm_http |
| `vet` | rebaixa achado LEVE quando o exame conclui normalidade | **rebaixa** | `document_vet.enabled` |
| `finalize` | invariants + monta JSON + trail | — | sempre |

## 3. Melhor fluxo (racional)
- **Fase A junta toda a evidência** (regra + semântica) → nenhum achado "nasce tarde demais" para ser medido.
- **Fase B mede TUDO** uma vez (incl. semânticos, opt-in) → gate quantitativo uniforme.
- **Fase C decide barato→caro:** agregação determinística → gate → **LLM-juiz por ÚLTIMO, só na
  incerteza** (`keep_current`, nunca cria relevância no vazio) → VET (guarda determinístico final).
- Princípio **POR-ACHADO**: negação/exclusão são por finding; agregação é OR — achado negado nunca
  derruba um relevante co-existente.

## 4. Liga/desliga por PERFIL (perfil = lista declarativa de steps)
| step | rule_only | hybrid | llm_http |
|---|---|---|---|
| treat · extract_categories · extract_findings · measure · score · aggregate · gate · vet · finalize | ✅ | ✅ | ✅ |
| **expand_semantic** | ❌ | ✅ | ✅ |
| **llm_judge** | ❌ | ❌ | ✅ |

Hoje isso está espalhado em `apply_runtime_profile` + flags; vira **uma lista de steps ativos** num lugar só.

## 5. Embedding como step de BACKEND TROCÁVEL (funde G-INFRA2)
`expand_semantic` recebe backend por config:
```
embeddings: { backend: sentence_transformers | volume | databricks_serving, ... }
```
- `sentence_transformers` — HF (baixa ~36min/restart).
- `volume` — modelo carregado de path local no Volume (sem HF).
- `databricks_serving` — endpoint HTTP (espelha o `llm_router`).

Mesmo modelo → mesmo `semantic.score`; escolha por config. Resolver o HF é o MESMO trabalho de
organizar este step.

## 6. Rede de segurança (byte-compat)
1. **Golden local determinístico** (rule_only): `{id: {fl, decision_source, summary, measures.met, categories, trail_n}}`
   por config. **Congelados:** tireoide (895, sha 39dc3da6) · hepato (100, 0.1.12, sha b4dc2076) ·
   pirads (200, rads_only, sha d648e5b). Diff before/after = **0**.
2. **Golden de produção** (llm_http): CSV do E2E (tireoide = `v22.5-3.16.csv`). Diff via re-run.
3. Steps não-determinísticos (`expand_semantic`, `llm_judge`): unit-test com backend/LLM **stubbados**.

## 6b. REVISÃO CRÍTICA (2026-07-14) — correções ao design

1. **Byte-compat ≠ reordenação.** O comportamento ATUAL: semântica eleva score/promove, mas NÃO cria
   finding mensurável (quantitativa ancora em findings de regra). Logo, "evidência antes de medir +
   medir semântico" é **MUDANÇA DE COMPORTAMENTO**, não byte-compat. **Separar em 2 etapas:**
   - **Etapa 1 — refactor PURO** (DecisionState + steps) que **reproduz o fluxo de hoje** → golden diff 0.
   - **Etapa 2 — evolução opt-in** (semântica→finding + `measure.applies_to=[rule,semantic]`) → muda
     output de propósito, valida contra **base ouro** (não golden). Nunca as duas juntas.
2. **`structured_extraction` genérico** (não "RADS"): o sistema de categorias vem da config; especialidade
   sem categorias estruturadas só desliga o step. (RADS = uma instância; pulmão terá outra.)
3. **Step registry + lista declarativa por perfil:** o runner executa uma LISTA de nomes de steps; add
   step novo (ex.: exames de sangue V2) = registrar + incluir na lista, sem tocar o runner.
4. **Defaults + template mínimo:** config mínima (`target_organs` + `findings`) roda; todo o resto
   (quantitativa, VET, negação por-achado, exclusão, semântica, llm) é **opt-in com default seguro**.
   Onboarding de especialidade parte de template limpo, não da config gigante do tireoide.

## 7. Escopo (o que NÃO muda)
- Nenhuma lógica de decisão nova. Funções-folha (rule engine, semantic, llm_router, quantitativa, vet)
  permanecem — passam a ser chamadas como steps.
- Config atual continua válida (byte-compat).
- **Exceção controlada:** `measure.applies_to=[rule,semantic]` é comportamento NOVO → entra **desligado
  por default** (byte-compat) e só liga/valida numa etapa própria, depois do refactor puro.

## 8. Plano
1. Golden dos 3 ✅.
2. `DecisionState` + runner de steps; migrar a cascata **fase a fase, diff 0 a cada passo**. ✅
   **CONCLUÍDO 2026-07-14** — `nlp_engine/decision_pipeline.py` (DecisionState + EngineContext +
   11 steps na ordem atual + `run_row`); `engine.py` fino (~55 linhas). Branch `refactor/decision-
   state-pipeline` (de `origin/hml`), commits `01d0be7` + `d56844e`, PUSHADA. **Gate: golden SHA
   idêntico 3/3** (tirads 895/`39dc3da6`, hepato 100/`b4dc2076`, pirads 200/`d648e5b`) + suíte 300 +
   ruff/format/mypy. Harness de regressão: `.claude/jobs/56d76a3e/tmp/regress_golden.py`.
3. Perfil = lista declarativa de steps. ✅ **CONCLUÍDO 2026-07-14** (mesma branch lib + plataforma
   `9d8e3bc`): step-registry (`Step` dataclass + `PRELUDE`/`RULE_PIPELINE` + gates) fonte única da
   ordem; perfil EMERGE dos gates (`active_step_names(ctx)` p/ observabilidade); `step_extract_categories`
   documentado como slot genérico (não RADS-hardcoded); template mínimo documentado (mínimo vs opt-in
   + camadas opt-in do DecisionState). Testes: config mínima/vazia rodam com defaults (suíte 305).
   Gate golden 3/3 mantido.
4. Backend trocável no `expand_semantic` (fecha G-INFRA2). **HF-no-Volume já resolvido via config**
   (embedding_model → Volume, v22.6/hepato 0.1.13); falta só formalizar `backend:` no step.
5. Re-run E2E → diff 0 vs golden de produção → merge (quando publicar a lib).
6. (Pós-refactor, opt-in) ligar `measure` sobre achados semânticos e validar contra golden/base ouro
   = **ETAPA 2** (muda output de propósito; nunca junto do refactor puro).
