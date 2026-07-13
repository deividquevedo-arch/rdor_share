# Homologação de negócio — relevantes do motor TI-RADS (v22.3)

**Para:** médico especialista · **Data:** 2026-07-13 · **Arquivo:** `homologacao-negocio-relevantes-v22.3-2026-07-13.csv`

## O que é

O motor classificou **153 laudos** como **relevantes** (devem ser captados na linha de cuidado de
tireoide). Pedimos sua homologação: para cada linha, confirmar se **é realmente relevante** segundo o
critério clínico/de negócio. Isso oficializa a **precisão** do motor.

## Como preencher (2 colunas)
- **`relevante_medico`** → `sim` (é relevante) ou `nao` (não deveria ser captado / falso-positivo)
- **`observacao_medico`** → comentário curto quando `nao` (ex.: "linfonodo reacional", "nódulo <1cm")

Contexto por linha: `tipo_exame`, `ti_rads_motor`, **`criterio_relevancia`** (por que o motor marcou),
`achados_detectados`, `laudo` (texto completo limpo).

## Critério de relevância V1 (referência)
Relevante = **TI-RADS 4 ou 5** · **Nódulo/Cisto ≥ 1 cm** · **Massa / Linfonodo / Tumor** · **Bócio** —
em exames de imagem de tireoide/pescoço. Órgão = tireoide.

## Distribuição dos 153 (por critério principal) — onde focar
| critério | n | atenção |
|---|---|---|
| TI-RADS 4 | 58 | confirmação padrão |
| Nódulo ≥1cm | 34 | confirmação padrão (medida no critério) |
| **Linfonodo** | **22** | **decisão-chave: suspeito/real vs reacional** (a spec deixou aberto) |
| TI-RADS 5 | 12 | confirmação padrão |
| **Nódulo/cisto sem medida confirmada** | **10** | **conferir se realmente ≥1cm** (medida não extraída) |
| **Semântico/LLM (sem achado de regra)** | **9** | **maior risco de falso-positivo — revisar com atenção** |
| Cisto ≥1cm | 4 | confirmação padrão |
| Bócio | 2 | confirmar bócio nodular (difuso já é excluído) |
| Massa | 2 | confirmação padrão |

> Os 3 grupos em **negrito** são os de maior dúvida — se puder priorizar, comece por eles (linfonodo,
> nódulo/cisto sem medida, semântico). Os demais são achados de alta confiança.

## O que fazemos com o retorno
Calculamos a **precisão homologada** (quantos dos 153 são realmente relevantes) e atualizamos a base
ouro. Os `nao` viram falsos-positivos a corrigir (a maioria já está mapeada em `mapa-gaps-tirads-v0.md`).

---

# Parte 2 — validação de RECALL (negativos duvidosos)

**Arquivo:** `homologacao-negocio-duvidosos-negativos-v22.3-2026-07-13.csv` (~69 casos)

Aqui o motor marcou **NÃO relevante**. Pedimos que confirme se **deixou passar algum relevante**
(falso-negativo). Não são os laudos normais óbvios — são os **casos-limite** onde o motor encontrou
algo e decidiu não captar. Mesmas colunas de preenchimento: **`relevante_medico`** (`sim` = o motor
ERROU, era relevante / `nao` = o motor acertou) + **`observacao_medico`**. A coluna **`motivo_nao`**
diz por que o motor não captou.

## Composição (~69) — os mais prováveis de serem FN
| motivo | n | pergunta ao médico |
|---|---|---|
| Linfonodo excluído como **reacional** | 28 | algum é na verdade **suspeito** (deveria captar)? |
| **LLM** classificou negativo (borderline) | 20 | o LLM rejeitou certo? |
| Nódulo/cisto **0,8–0,99 cm** (borderline <1cm) | 18 | algum arredonda p/ ≥1cm / deveria contar? |
| **VET**: achado leve em exame "normal" | 3 | o exame era mesmo normal? |

> Foco: **linfonodo reacional (28)** — é o espelho da decisão dos relevantes; e **nódulo 0,8–0,99cm**
> (limítrofe do corte de 1cm). Se o médico marcar algum como `sim`, é um FN real a recuperar.
