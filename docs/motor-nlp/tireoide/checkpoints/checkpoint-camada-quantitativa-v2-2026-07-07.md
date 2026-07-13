# Checkpoint — Camada de Critérios Quantitativos V2 (retomar aqui)

**Data:** 2026-07-07 · **Status:** V2 implementada e validada LOCAL; **falta push + run de validação**.
**Spec:** `doc-spec-camada-criterios-quantitativos-v0.md` · **Régua/decisões:** memória `[[tireoide-linha-cuidado-story-v1v2]]`, `[[camada-criterios-quantitativos]]`.

---

## Régua V2 (travada com negócio + médico)

Relevante = **nódulo ≥ 1cm** OU **cisto ≥ 1cm** OU **massa / linfonodo(SUSPEITO) / tumor / bócio** (a palavra já basta) OU **TR4 / TR5 / TR6**.
- Operador **`>=`** (definido; ajustável depois).
- **Linfonodo reacional/normal NÃO é relevante** (só suspeito/patológico) — decisão do médico.
- massa/tumor/bócio e TR4-6 **nunca** são gateados por tamanho.

## Onde está (tudo LOCAL, sem push)

**Lib `nlp-engine` → 0.3.2** — branch `nlp-engine-lib/feat/quantitative-coordinated-gate`
- `b34032d` Slice 2.1 — **gate coordenado** (`_coordinated_gate_demotes`): rebaixa só se todos os drivers presentes forem dimensionais/qualitativos e falharem; `rads_relevant` blinda TR4-6; `met=None` = fail-safe mantém.
- `4d4007e` **critério qualitativo** (`kind: qualitative`, bump 0.3.2 + RELEASE): LLM julga predicado booleano `{relevant, evidence}`; base do linfonodo suspeito.
- Suíte **264 passed**; ruff/format/mypy limpos.

**Config `tirads` v15** — branch `fabrica-ia-plataforma/test/rads-e2e-hml`, commit `764e488` (requer nlp_engine≥0.3.2)
- `nodulo_maior_1cm` op `>` → **`>=`**
- **`cisto_maior_1cm`** (dimensional, `>=1cm`)
- **`linfonodo_suspeito`** (`kind: qualitative`, anchor `linfonodo`)
- **Fix F**: pattern `\bTR\s?([1-6])\b` (captura "TRx" isolado, ex.: `(TR4)`)
- **Os 3 critérios em `on_met: annotate_only`** → gold spot v13 intocado. Flip → `gate_relevance` = **V2 ON**.

## Capacidade da camada (geral, config-driven)

Dois tipos de critério, ambos com **`all_of` (E) / `any_of` (OU)** e `annotate_only|gate_relevance|promote`:
- **dimensional** (medida + limiar em código; LLM só extrai valor+unidade+evidência): nódulo≥1cm, cisto≥1cm, **PMAP>25 E RVP>3**, VEF1, mmHg…
- **qualitativo** (LLM julga predicado booleano + evidência): linfonodo suspeito, e futuros.
- Opt-in / byte-compat; gating por âncora + tipo de exame (não chama LLM à toa); fail-safe (LLM falha → `met=null` → não altera decisão).

## Validação já feita (run v14 v3, D=2026-06-29, annotate_only nódulo)

- Extração de **alta precisão**: `A x B x C` → maior; **mm→cm** (6,8mm→0,68); multi-nódulo → maior; negação/pós-op/indicação → `met=None`; TC "lesão focal" → ok.
- 89 âncora-nódulo: 38 >1cm / 46 ≤1cm / 5 None (todos corretos). `fl` idêntico ao v13 (annotate_only = no-op). 429 zerado (backoff 0.3.1). CSV: `homolog-quantitativo-nodulo1cm-v3-2026-07-07.csv`.

## PRÓXIMOS PASSOS (retomar amanhã — push só com autorização)

1. **Push lib** `feat/quantitative-coordinated-gate` → **PR → hml** → wheel `nlp_engine-0.3.2` no Volume. Tag `v0.3.2` no merge.
2. **Push config v15** (`test/rads-e2e-hml`) — só depois do wheel 0.3.2 no Volume.
3. **Run `annotate_only`** perfil `llm_http`, `nlp_engine_version=0.3.2`, D=2026-06-29 → auditar `exm_laudo_resultado.quantitative`:
   - **cisto**: medida + evidência corretas? (esperado ~74 laudos, quase todos <1cm)
   - **linfonodo_suspeito**: LLM separa reacional (false) de suspeito (true)? checar os ~19 linfonodo-only (≈3 reacionais / 11 suspeitos / 5 fronteira)
   - **Fix F**: os 2 "(TRx)" isolados agora extraem categoria
   - **concorrência reduzida** no run (mesmo com backoff) p/ evitar 429
4. Validado → **flip os 3 critérios para `gate_relevance`** (V2 ON) → re-rodar e **medir impacto no `fl`** vs v13 (quantos nódulos/cistos <1cm saem; quantos linfonodo reacionais saem).
5. Depois: 2º caso composto **PMAP > 25 E RVP > 3** (outra especialidade) p/ exercitar `all_of` em produção.

## Lembretes
- 🔐 **Rotacionar chave OpenAI** colada em sessão antiga (pendência do usuário).
- Etiqueta `nlp_engine.__version__` (interno) está hardcoded desatualizada — cosmético; confiar em `importlib.metadata`. (Hygiene opcional: torná-la dinâmica.)
