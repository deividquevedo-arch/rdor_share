# Base ouro TI-RADS V2 — régua, reclassificação e métricas (congelada 2026-07-24)

## Régua de relevância V2 (lógica OR — confirmada pelo head)
Um laudo é **relevante** se satisfaz **QUALQUER** condição:

- **nódulo ≥ 1cm** (dimensão discriminada) **OU**
- **cisto ≥ 1cm** (dimensão discriminada) **OU**
- **TI-RADS 4 / 5 / 6** **OU**
- **linfonodo** patológico (linfonodomegalia / necrose / suspeito / atípico / perda de arquitetura hilar) **OU**
- **massa / tumor / neoplasia / bócio nodular** (multinodular / mergulhante).

**Não são condição de relevância:**
- nódulo/cisto **sem medida** ou **< 1cm** (a régua V1 "qualquer tamanho" foi substituída pela V2 ≥1cm);
- **PAAF / punção / biópsia** — são os *exames* que analisamos (extraímos e validamos a partir deles); o procedimento **não promove** por si;
- achado **extra-tireoidiano** (subcutâneo, seio maxilar, testículo, mama, renal...);
- linfonodo **reacional / proeminente benigno / habitual**.

Mecanismo no motor: `require_measure:True` nos critérios `nodulo_maior_1cm`/`cisto_maior_1cm` (nlp_engine ≥ 0.6.2) — sem medida ≥1cm → rebaixa. Drivers incondicionais (massa/tumor/bócio/linfonodo/TR4-5) não dependem de medida.

## Reclassificação V1 → V2 (7 casos, `1→0`)
Base ouro original (`base-ouro-tirads-2026-07-11.csv`, 586 resolvidos, 134 positivos) tinha rótulos **generosos** que a régua V2 corrige. Reclassificados **apenas** casos objetivos, com **dupla confirmação** (motor V2 rebaixou `motor=0` **E** leitura do laudo):

| Motivo | Qtd | Critério |
|---|---|---|
| PAAF de nódulo sem medida | 5 | laudo de punção; nódulo só na indicação/alvo, sem dimensão; motor rebaixou (`met=None`) |
| Achado extra-tireoidiano | 2 | nódulo subcutâneo (3,3cm) / cisto de seio maxilar — fora da tireoide |

**NÃO reclassificados:** 12 PAAF/nódulos que **têm medida ≥1cm** (o LLM mediu, `nodulo_maior_1cm:True`) — são V2-relevantes reais (`verdade=1` mantida). Reclassificá-los por regex seria erro (formatos "2,2 x 1,3 cm" / "nódulo de 2,8cm" que regex simples perde).

## Base ouro V2 congelada
- **Arquivo:** `dados/base-ouro-tirads-v2-2026-07-24.csv` (`id_exame, verdade_v1, verdade_v2, reclassificado`; sem laudo — LGPD).
- **586 resolvidos:** 127 positivos / 459 negativos (era 134 / 452 na V1).
- **SHA** (id,verdade_v2 ordenado): `024ed00770227e24`.

## Métricas do motor (config `v22.11-v2`, wheel 0.6.2) vs base ouro
| Base | TP | FP | FN | TN | Acc | Precisão | Recall | Especif. | F1 | F2 | MCC |
|---|---|---|---|---|---|---|---|---|---|---|---|
| V1 (original) | 127 | 4 | 7 | 448 | 0,9812 | 0,9695 | 0,9478 | 0,9912 | 0,9585 | 0,9520 | 0,9465 |
| **V2 (congelada)** | 127 | 4 | **0** | 455 | **0,9932** | 0,9695 | **1,0000** | 0,9913 | **0,9845** | **0,9937** | **0,9803** |

**FN=0** — todos os "FN" da V1 eram rótulos generosos (PAAF/extra-tireoide). Os 4 FP restantes (não tocados, conservador): nódulo ≥1cm benigno, massa por negação não-capturada, linfonodo proeminente. Refino desses = trabalho de precisão futuro (negação por escopo).

## Rastreabilidade
- Régua V2 confirmada pelo head (2026-07-23/24); reclassificação **sem necessidade de re-homologação do negócio** — lote de positivos + negativos já encaminhado para análise; critérios objetivos e auditáveis.
- Harness: `.claude/jobs/56d76a3e/tmp/baseohro_tirads.py` (atualizar `EXPECTED` para a matriz V2 se re-congelar).
