# Checkpoint — Camada Quantitativa V2 LIGADA (run v18) — 2026-07-08

**Status:** V2 ativa (`gate_relevance`) validada no E2E; **1 decisão de negócio pendente** (bócio difuso).
Sucede `checkpoint-camada-quantitativa-v2-2026-07-07.md`. Ver memória `[[camada-criterios-quantitativos]]`.

---

## Estado (tudo mergeado / no ar)

| Peça | Versão / commit | Onde |
|---|---|---|
| Lib `nlp-engine` | **0.3.5** (wheel no Volume) | hml (via PRs 0.3.2→0.3.5) |
| Config `tirads` | **v18** (`gate_relevance` nos 3 critérios) | `test/rads-e2e-hml` `9764db4` |

**Régua V2:** relevante = **nódulo ≥1cm OU cisto ≥1cm OU massa/linfonodo(SUSPEITO)/tumor/bócio OU TR4/5/6**.
Critérios da camada: `nodulo_maior_1cm` (dim, `>=1cm`), `cisto_maior_1cm` (dim, `>=1cm`), `linfonodo_suspeito` (qualitativo, âncora **texto**). Gate coordenado; `met=None`/RADS/`promote`/massa-tumor-bócio nunca rebaixam.

## Resultado do run v18 (D=2026-06-29, llm_http, 0.3.5)

| Métrica | Valor | Leitura |
|---|---|---|
| fl final | **126** (gold-spot 261) | V2 removeu o que é <1cm / reacional |
| `quantitative_gate` | **120** | rebaixados pela V2 |
| gated com `met=True` | **0** | ✅ nenhum ≥1cm/suspeito rebaixado (zero TP-loss no gate) |
| `llm_router_llm_fallback` (429) | **0** (era 36) | ✅ `skipped_not_relevant` cortou o flood do linfonodo |

**Eliminados do gold-spot (261→126 = −135):** CSV `eliminados-gold-spot-v2-2026-07-08.csv`
- **120 pela V2 gate** — 103 limpos (nódulo/cisto <1cm + linfonodo reacional = drops corretos) + **17 bócio/difuso** (ver pendência).
- **15 por variância do router** (llm_positive↔negative run-a-run, borderline) — não é V2.

## Garantias confirmadas
- **Zero TP-loss no gate:** 0 gated com `met=True`; TR4-6 aparentes eram tabela-legenda (cat real TR1/2/3); "massa" era negada ("sem massa").
- **429 resolvido** pela otimização "gate pula LLM quando fl==0" (linfonodo: 660→só relevantes).
- Lib agnóstica (default biliar removido em 0.3.4).

## ⚠️ Pendência única — bócio/tireoidopatia difusa (decisão do médico)

**17 exames** rebaixados têm **"dimensões aumentadas" / "tireoidopatia difusa"** (bócio difuso), que a **regra não detecta como finding `bócio`** (só pega a palavra "bócio"/"tireomegalia") → quando o exame só tinha um nódulo pequeno, o gate rebaixou.
- **Não é bug do gate** — é **gap de léxico** do finding `bócio`.
- CSV para o médico: `medico-bocio-difuso-v2-2026-07-08.csv` (id, laudo, coluna "Relevante para captação?").
- **Se médico disser SIM** (difuso = bócio relevante): ampliar léxico do finding `bócio` (config) com `dimensões aumentadas`/`tireoidopatia difusa` → viram driver incondicional → gate os mantém. Fix config-only.
- **Se NÃO:** os 17 estão corretos e a V2 está fechada.

## Próximos passos
1. **Médico** homologa os 17 difusos (CSV) → decide se bócio difuso conta.
2. Se sim: fix de léxico do `bócio` (config v19) + re-run.
3. Negócio homologa o conjunto V2 (126 relevantes + os 135 eliminados) para sign-off.
4. (Futuro) resolver a variância do router nos 15 borderline se incomodar; G1 (âncora de órgão) para reduzir dependência do router.

## Artefatos
- `eliminados-gold-spot-v2-2026-07-08.csv` (135) · `medico-bocio-difuso-v2-2026-07-08.csv` (17)
- CSV do run: `Downloads/ntb_ia_motor_e2e_full_v18_dim_v6.csv`
