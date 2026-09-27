# Homologação — Câncer de Estômago `0.6.1`

**Data:** 2026-08-20 · **Revisão clínica:** Carol · **Status:** 🔴 **aguardando retorno do lote de validação**
**SPEC:** [`spec-negocio-cancer-estomago-v1.md`](spec-negocio-cancer-estomago-v1.md)

---

## 1. Versão

| item | valor |
|---|---|
| `config_version` | **`0.6.1-cancer_estomago`** |
| branch | `cancer_estomago/feature/migracao-plataforma` (`fabrica-ia-nlp-platform`), commit `e371036` |
| `nlp_engine` | ≥ 0.9.4 |
| plataforma | **nova** (`diamond_fabrica_ia_dev.cancer_estomago`) |
| janela avaliada | 2026-05-01 a 2026-06-30 — 61 dias, **10.783** endoscopias digestivas altas |

---

## 2. O que mudou desde a `0.1.8`

| # | mudança | efeito medido |
|---|---|---|
| 1 | **úlcera gástrica entra** (decisão do Targa) | escopo novo — 148 laudos com o achado |
| 2 | regex de úlcera: `\b` inicial removido | +101 laudos casando; a palavra vinha colada (`deúlcera`) em 27% dos casos |
| 3 | **cascata regra → juiz**: banda `[0.35, 0.97]` → `[0.60, 0.97]` | juiz chamado 3.199 → **345**; run 169 → **45 min** |
| 4 | `ignore_sections` reconhece `Nota:` no singular | resolve o falso positivo de MALT em seguimento |
| 5 | `skip_organ_gate` em `ulcera` e `tne` | recupera 7 laudos de úlcera gástrica real |
| 6 | **TNE entra**, qualquer tipo | achado `tne` criado; 4 laudos, 1 entregue |
| 7 | úlcera de cicatriz (Sakita S) e de anastomose **fora** | −12 laudos |

---

## 3. Resultado no lote de 37 revisado pelo negócio

| | `0.1.8` | `0.4.0` | `0.6.1` |
|---|---|---|---|
| recall | 0,533 | 0,467 | **0,600** |
| precisão | 0,727 | 0,700 | **1,000** |
| falsos positivos | 3 | 3 | **0** |

⚠️ **O recall de 0,600 é o teto alcançável contra este gabarito, não uma limitação da régua.** A
anotação do negócio é **anterior** ao critério explícito: dos 15 laudos que ele marcou relevantes,
cinco ficam fora **por decisão posterior** (área elevada, subepitelial, órgão, cicatriz) e um era
promoção do juiz sem evidência de regra, hoje bloqueada por arquitetura. Ver §8 da SPEC.

**Nenhum laudo entregue sai com a coluna de achado vazia** — eram 19 na versão anterior.

---

## 4. Volumetria

**75 laudos relevantes em 61 dias — 1,2 por dia.**

Para dimensionar: a tireoide entrega ~94 laudos/dia. A capacidade de absorção não é restrição nesta
linha.

⚠️ **41% do corpus são laudos sem texto na origem** — teto de recall independente da régua.

---

## 5. Lote de validação enviado

**133 laudos**, em dois blocos, para medir as duas coisas separadamente:

| bloco | n | o que mede | pergunta na planilha |
|---|---|---|---|
| **A** | 75 | **precisão** | o motor entregou — deveria ir para a fila? |
| **B** | 58 | **recall** | o motor recusou apesar do achado — deveria ter entregado? |

O bloco B é amostra estratificada dos **270** laudos que têm achado de regra e o juiz recusou —
é onde um falso negativo pode estar escondido. Mínimo de 3 por estrato, para os achados raros não
sumirem: `neoplasia` 33 · `Úlcera` 15 · `recidiva_tumoral` 3 · `TNE` 3 · `tumor` 2 · `linfoma` 1 ·
`lesao_suspeita` 1.

---

## 6. Métricas da revisão

🔴 **A preencher com o retorno da Carol.**

| | motor: SIM | motor: NÃO |
|---|---|---|
| **revisão: SIM** | | |
| **revisão: NÃO** | | |

| métrica | valor |
|---|---|
| Recall | |
| Precisão (VPP) | |
| Especificidade | |
| VPN | |
| F1 | |
| MCC | |

---

## 7. Questões abertas levadas junto

Duas divergências em que a régua contraria uma marcação do Targa **sem respaldo posterior** — são as
únicas, e vão para ele com o lote:

1. **MALT em seguimento** conta como progressão? Ele marcou não relevante um laudo com aumento de
   número e extensão das áreas; a régua trata recidiva com precedência absoluta.
2. **Achado maligno fora do estômago** (orofaringe): ele marcou relevante; a régua recusa por órgão.

---

## 8. Pendências antes de hml

- [ ] retorno da Carol sobre os 133 laudos
- [ ] resposta do Targa às duas questões da §7
- [ ] schema `cancer_estomago` provisionado em **hml** (é do time da Fábrica; hoje só existe em dev)
- [ ] PR para hml — **somente depois de validado**
