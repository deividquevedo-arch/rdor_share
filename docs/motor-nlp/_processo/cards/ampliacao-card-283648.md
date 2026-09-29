# `283648` [P0-29] — ampliação proposta

> 17/09/2026. Rascunho local, **não postado**. Card: *Fabrica IA/NLP Engine - [P0-29] Impedir que o
> juiz LLM promova sem evidência de regra* · Task, P1, sem critério de aceite.

**Duas lacunas:** o campo de aceite está **vazio**, e o escopo cobre só o juiz — enquanto três
medições de 16 e 17/09 mostram que a promoção sem evidência tem **duas vias**, e a segunda não é
alcançável por banda.

---

# 1. O que mudou desde que o card foi escrito

## 1.1 A via do juiz, confirmada em mais duas linhas

| linha | medição | resultado |
|---|---|---|
| **hepatologia** (prd, 30 dias, 16/09) | `fl = 1` · `llm_called = true` · `n_positive_spans = 0` | **118 laudos**; 36 correntes (`llm_router_llm_positive`, ~1/dia útil, 2 no dia da medição) e 82 do incidente do 403 |
| **ateromatose** `0.2.0` (dev, 7.500 laudos, 16/09) | banda `[0.35, 0.65]` | juiz chamado em **6.111 de 7.500 (81,5%), todos sem achado**; aprovou **655**; `match_rate` 96,2% → **87,8%** |

Os 36 correntes da hepatologia têm score **0,367 a 0,566** — abaixo do teto analítico de 0,597 que o
card já cita. O contorno (piso da banda acima do teto) **não estava aplicado** ali, e a linha roda em
produção desde então.

## 1.2 🔴 A via nova: a semântica promove, e o juiz nunca é consultado

| linha | medição | resultado |
|---|---|---|
| **ateromatose** `0.2.1` (lote rotulado de 493, 16/09) | banda `[0.75, 1.0]` | a semântica promoveu **33 dos 44** laudos sem achado; precisão contra a médica **0,143** |
| **cancer_rim** (dev, 10.000 laudos, 16/09) | `similarity_threshold: 0.92` | **2 laudos** com `fl = 1`, `n_positive_spans = 0` e **`llm_called = false`** |

Em `decision_mode: 'hybrid'`, `fl = 0` com `semantic_score >= similarity_threshold` **vira `fl = 1`**.
O código marca a promoção como *"pendente de arbitragem — ver `step_decide_llm`"*, mas a arbitragem
só ocorre **dentro da banda**. Fora dela, a pendência nunca é resolvida e a promoção sai entregue.

🔴 **E não existe banda que feche as duas vias:**

| banda | via do juiz | via semântica |
|---|---|---|
| **larga** (`[0.35, 0.65]`) | juiz vê 81,5% dos laudos — custo proibitivo, `match_rate` despenca | coberta |
| **estreita** (`[0.75, 1.0]`) | fechada | **passa livre, sem árbitro** |

O contorno que o card descreve — subir o piso da banda — **fecha uma via e abre a outra**. Por isso
a correção não é de config.

⚠️ **E a proveniência não denuncia:** `decision_source` sai `hybrid_calibrated`, sobrescrito pelo
passo seguinte. `semantic_promoted` existe no estado interno e **não é emitido**. Identificar esses
casos no dado exige cruzar três campos.

# 2. Título — proposta

O atual fala só do juiz:

> [P0-29] Impedir que o juiz LLM promova sem evidência de regra

Proposto:

> **[P0-29] Impedir promoção de relevância sem evidência determinística — juiz e camada semântica**

⚠️ Mudar título quebra reconhecimento em comentários e commits que já citam o card. Vale se o escopo
novo for aceito; se não, fica como está e a via semântica vira card irmão.

# 3. Critério de aceite — proposta (o campo está vazio)

- **CA1** — Nenhuma promoção `fl 0 → 1` acontece sem evidência determinística, por **qualquer** via:
  juiz, camada semântica ou promoção ordinal. Garantido em código, não por parâmetro de config.
- **CA2** — A simetria com `_tem_evidencia_dura` está fechada: assim como o juiz não derruba evidência
  dura, ele e a semântica não criam relevância do nada.
- **CA3** — A promoção semântica pendente de arbitragem **não entrega** quando o juiz não é chamado.
  Hoje ela entrega; o comportamento passa a ser explícito (não promove, ou promove e marca).
- **CA4** — `semantic_promoted` (ou equivalente) passa a ser **emitido no blob**, para que o caso seja
  identificável sem cruzar três campos.
- **CA5** — Teste que mata o mutante: config com banda estreita e `similarity_threshold` baixo **não**
  produz `fl = 1` com `n_positive_spans = 0`.
- **CA6** — Impacto medido nas linhas com a camada ligada, **antes** de subir: quantos laudos mudam de
  decisão, por linha e por via.
- **CA7** — O que deixar de ser promovido é **contabilizado e reportado**, não some em silêncio.

⚠️ **CA6 é o que decide o tamanho da entrega.** Na ateromatose a via semântica respondia por 33 de 44;
no cancer_rim, por 2 em 10.000. A diferença é a similaridade média do corpus — TC de tórax tem
similaridade alta em todo laudo (média 0,72). **Medir por linha antes, não estimar.**

# 4. O que NÃO muda

- A alocação na `0.14.0` segue.
- O contorno atual (piso da banda acima do teto) continua válido **para a via do juiz**, e deve ser
  aplicado na hepatologia agora — ela está em produção com a banda `[0.35, 0.65]`.

---

📄 Evidência: `_processo/medicoes/medicao-p0-29-juiz-sem-evidencia-2026-09-16.md` ·
`_processo/medicoes/diagnostico-embeddings-run-joao-2026-09-16.md` ·
`_processo/revisoes-pr/auditoria-pr-7275-terceira-revisao.md` · changelog da config `0.2.1` da ateromatose.
