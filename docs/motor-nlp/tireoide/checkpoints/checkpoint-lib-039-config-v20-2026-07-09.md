# Checkpoint — Lib 0.3.9 + Config v20 (PRs abertos, aguardando E2E) — 2026-07-09

**Status:** lib `0.3.9` e config `v20` **pushados, PRs abertos**; aguardando publicação da wheel +
run E2E de validação. Sucede `checkpoint-camada-quantitativa-v2-ON-2026-07-08.md`.
Ver memória `[[camada-criterios-quantitativos]]`, `[[xxrads-status-e-bancada-ab]]`.

---

## Estado atual (pushado / PR aberto — NÃO mergeado ainda)

| Peça | Versão / commit | Branch | Situação |
|---|---|---|---|
| Lib `nlp-engine` | **0.3.9** (`7722b15`) + 0.3.8 (`c33a0b7`) | `fix/finding-organ-scope-block` → `main` | PR aberto; tag `v0.3.9` publicada |
| Config `tirads` | **v20** (`3b3bdd9`) | `test/rads-e2e-hml` | PR aberto |

**Wheel 0.3.9 no Volume:** ⏳ pendente (ação MLOps/pipeline) + restart de cluster.
Lembrar do mismatch cosmético do `__version__` (metadata é a fonte autoritativa).

## O que evoluiu desde o v18/0.3.5

### Lib
- **0.3.6** — G1: `_organ_spans` passa a considerar o regex do órgão (âncora de proximidade
  simétrica com `_mentions_target_organ`).
- **0.3.7** — endurece prompt de extração dimensional (medida é da **lesão**, nunca
  lobo/glândula/istmo/volume/linfonodo; multi-nódulo → retorna a **maior**) + audit mais limpo.
- **0.3.8** — **`finding_organ_scope: "sentence"(default) | "block"`**. O gate órgão↔achado era
  sempre por sentença → nódulo/cisto descrito em frase que não repete "tireoide" não disparava
  → camada dimensional pulada → ≥1cm escapava. `block` mede proximidade em chars absolutos do
  bloco. Default `sentence` preserva byte-compat (BI-RADS intacto).
- **0.3.9** — **output enxuto** (`exm_laudo_resultado`), sem mudança de decisão:
  - audit qualitativo parava de despejar o **prompt inteiro** no `threshold` (~600 chars);
    agora rótulo curto `criterion` → bloco linfonodo **665→294 chars (−56%)**.
  - `value/unit/threshold` só em `dimensional`; `llm_error/llm_model` omitidos quando vazios.
  - **`semantic_evidence`**: expõe o trecho que gerou a similaridade (antes só o termo → dúvida).

### Config
- **v19** — threshold semântico 0.92→0.88 (recall).
- **v20** — `finding_organ_scope: block` + expansão léxica **ASSUMIDA** (confirmar médico):
  nódulo `formação/imagem hipoecoide/sólida/ovoide`; bócio `tireoidopatia difusa`,
  `alteração (eco)textural difusa`, `dimensões difusamente aumentadas`.
  **Não** adicionado `tireoidopatia` nu (risco FP).

## Validação do fix (block + léxico) nos laudos reais

**5/7** casos antes-perdidos passam a disparar (validado com `process_rule_based` sobre o laudo
real do CSV v19, escopo `sentence` vs `block`):

| Caso | sentence | **v20 (block)** |
|---|---|---|
| nódulo = 1cm | 0 | **2** ✅ |
| nódulo 2,2 cm | 0 | **4** ✅ |
| N1 sólido-cístico | 0 | **1** ✅ |
| bócio multinodular / tireoidopatia difusa | 1 | **4** ✅ |
| nódulo = 1 cm | 0 | **1** ✅ |
| sinais de tireoidopatia (parenquimatosa) | 0 | 0 ⚠️ |
| tireoidopatia parenquimatosa | 0 | 0 ⚠️ |

⚠️ **Correção de rumo importante:** o "ainda 0 no v19" que eu havia diagnosticado antes era
**artefato de `laudo` vazio no CSV de homologação** (o revisor anotou olhando o WebRIS, não o
texto). Nos laudos reais o fix recupera os achados.

## Testes / qualidade
Suíte **272 verde** · ruff limpo · mypy limpo · BI-RADS byte-compat preservado.
Novos testes: `test_finding_organ_scope_block_vs_sentence`,
`test_assess_qualitative_audit_omits_prompt_and_measure_fields`, evidência semântica.

## Resultado E2E v20 (D=2026-06-29, llm_http, 0.3.9 + config v20) — 895 linhas

| Métrica | Valor | Leitura |
|---|---|---|
| fl_motor | **208** (v18=126, gold-spot=261) | block-scope recuperou nódulos que sumiam |
| `quantitative_gate` | 117 | rebaixados pela V2 |
| met=True | **71 dim + 14 qualit** | camada agora dispara de verdade |
| TP-loss no gate (gated c/ met=True) | **0** ✅ | garantia mantida |
| `semantic_evidence` presente | **895/895** ✅ | output enxuto no ar |
| `llm_error` (quant) / sinal 429 | 12 / 26 linhas | resíduo rate-limit FMAPI (fail-safe met=None) |

**Contra a verdade médica (23 casos de regressão — set enriquecido c/ os difíceis):**
**TP=8, FP=0, FN=14, TN=1**. FP=0 (threshold 0.88 não reinflou aqui). Os 14 FN:

| Causa | Casos | Ação |
|---|---|---|
| A. **Cedilha nos regex v20** (formaÇão/alteraÇão não casavam — texto cru, `c` literal) | 2 | 🐞 **CORRIGIDO v20.1** (`e5bff8d`, pushado) |
| B. **Linfonodo não promove** (`gate_relevance` só rebaixa; router negativo + fl=0 → qualitativo pulado) | ~5 | ⚙️ decisão médico (CSV) |
| C. **Léxico deferido** (tireoidite crônica, tireoidopatia sem "difusa") | 3 | 👨‍⚕️ decisão médico (CSV) |
| D. **Sub-medição multi-nódulo** (extrator pegou N2=0,7cm; há >1cm) | 1 | 🎯 backlog extração |
| E. **Não-tireoide** (testículo) — motor=0 correto | 2 | ruído da verdade |

### Fix v20.1 (config, pushado `e5bff8d`)
`formac[aã]o`/`formac[oõ]es`/`alterac[aã]o` → `forma[çc]…`/`altera[çc]…` (aditivo). O rule engine
aplica `findings_regex` no texto **cru** (sem normalizar acento) → cedilha precisa ser explícita.
Validado no rule engine real: "formação hipoecoide"/"alteração ecotextural difusa" agora disparam.
**Backlog lib:** avaliar normalizar acento também no path de `findings_regex` (hoje só o Matcher/norm
normaliza) — evitaria a classe inteira desse bug para qualquer config futura.

### CSV para o médico — `medico-politicas-abertas-v20-2026-07-10.csv` (122 linhas)
2 políticas em aberto. **Método (DRY):** busca o termo (linfonodo/adenomegalia) → decide
presente vs ausente com a **negação da própria lib** (`is_negated_in_sentence_plain` +
`negation_phrases`/`negation_window` da config) — mesma semântica do motor, sem regex de negação
reinventado. **leak = 0** (verificado com o mesmo primitivo). O único regex do script é a
classificação em bucket (relevância = decisão do médico, não do filtro).
- **linfonodo (109 não-negados):** linfonodomegalia=22, reacional=18, proeminência=11,
  suspeito/atípico=7, inespecífico=6, aumentado=1, normal/habitual=10, outro=34.
- **tireoidite/tireoidopatia sem "difusa" (13).**
**Divisão de responsabilidade:** a lib remove só os NEGADOS (ausência/não há/não foram
identificadas/sem…); benignos-normais (reacional/habitual/proeminente) **são mantidos** — não é
papel do filtro decidir que não contam; o médico decide por bucket. Casos-chave da regressão
(linfonodomegalia 1,2cm, proeminência, inespecífico, aumentado) presentes.
Se médico=SIM p/ um bucket: `linfonodo_suspeito` vira `promote` (resgata 0→1) + ajuste do prompt;
tireoidite entra no léxico.
**Lição (clean code):** a 1ª tentativa reinventou negação com regex ad-hoc (NEG/BEN/VETO) e
vazava — corrigido reusando o primitivo da lib. Vale para todo o código: negação tem fonte única.

## Lib 0.3.10/0.3.11 + Config v21 — regras do médico (2026-07-10)

Médico decidiu (CSV, 40 casos): linfonodomegalia/proeminência/suspeito/aumentado/tireoidite = 1;
**reacional/reativo = 0 (demotor duro, vence tamanho)**. Ver [[tireoide-linha-cuidado-story-v1v2]].

- **Lib 0.3.10** `findings_exclusion_terms` (qualificador demove achado; reusa negação; unless=override).
- **Lib 0.3.11** `findings_skip_organ_gate` (linfonodo cervical isento do gate de tireoide — a causa
  real do "linfonodo sub-detecta 0/895" era o GATE, não o léxico).
- **Config v21** (`320eedc`): linfonodo driver (+proeminência) + skip_organ_gate + exclusion
  (reacional unless necrose/atípico/suspeito) + tireoidite driver + `linfonodo_suspeito`→annotate_only.

**Validação determinística vs médico (38 casos): 74%→92%** (TP=23 FP=2 FN=1 TN=12). Os 3 erros =
fronteira reacional/proeminência (inespec+reac que dispara em proeminente; 1 over-exclusão sentença-wide).
**Esse é o resíduo ambíguo do v22.**

**v22 (planejado):** `on_met: decide` (LLM qualitativo BIDIRECIONAL — promove OU rebaixa) ancorado só
no resíduo ambíguo. Capacidade nova na lib; construir sabendo o que o determinístico deixou (decisão
do usuário: "opção 1, LLM promove ou rebaixa cfme necessidade").

Branch lib `fix/finding-organ-scope-block`: 0.3.8→0.3.11 (últimos 2 commits `e8e4e84`, `8775858`)
**aguardam push**. Config v21 `320eedc` (`test/rads-e2e-hml`) aguarda push.

## Pendências

1. **Wheel 0.3.9** no Volume + restart cluster (MLOps).
2. **E2E** `nlp_engine_version=0.3.9` + config v20 → validar contra os **23 casos de regressão**
   (`regression-extracao-dim-2026-07-09.csv`): (a) 5/7 achados recuperados; (b) output enxuto
   na prática; (c) threshold 0.88 sem reinflar FP (v19 tinha 6 FP vs 3 no v18).
3. **Médico (não bloqueante):** os 2/7 restantes são "tireoidopatia parenquimatosa"/"sinais de
   tireoidopatia" **sem "difusa"** — decisão sobre ampliar léxico (risco FP) ou manter fora.
   Herda a pendência do bócio difuso do checkpoint v18 (`medico-bocio-difuso-v2-2026-07-08.csv`).

## Backlog técnico (parecer da lib — deferido)
Externalizar o prompt de extração p/ config (agnosticidade — hardcode de tireoide foi introduzido
por mim); `confidence_score` refletir a decisão final; unificar fonte de schema/decision_source;
logging + sanitizar `llm_error` (LGPD); refatorar `process()` (god-method); Matcher 1x;
negação **left-only** na lib (permite janela maior sem sobre-negar) — janela=5 bidirecional é
trade-off conhecido, NÃO é a causa dos misses atuais (esses eram escopo de órgão, já corrigido).

## Artefatos
- CSV run v19: `Downloads/ntb_ia_motor_e2e_full_v19.csv`
- Regressão: `regression-extracao-dim-2026-07-09.csv`
- Pendência médico: `medico-bocio-difuso-v2-2026-07-08.csv`
