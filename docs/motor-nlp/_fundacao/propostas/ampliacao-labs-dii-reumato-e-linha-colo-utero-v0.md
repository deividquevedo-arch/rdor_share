---
tags: [captacao, dii, reumatologia, colo-do-utero, viabilidade]
---

# Ampliação da captação — laboratório em DII e Reumatologia, e linha nova de colo do útero

> v0 — 23/10/2026. Levantamento de viabilidade só com consultas agregadas (sem laudo nem dado de paciente).
> Responde ao pedido de: (1) incluir calprotectina fecal (DII) e FAN (Reumatologia) na identificação; (2) criar a
> identificação de NIC e ASC-US em citologia e anatomia patológica do colo do útero.

## Hoje

| linha | o que lê | laboratório? |
|---|---|---|
| DII | laudos de imagem da gold (`tb_gold_mov_exame`, filtro pelo nome do exame): TC/RM de abdome, pelve, canal anal + colonoscopia | não |
| Reumatologia | TC/US/RM de regiões articulares e ósseas | não |
| Colo do útero | **linha não existe** | — |

## Frente A — calprotectina (DII) e FAN (Reumatologia)

**Viável.** Os dois exames estão na mesma gold que as linhas usam (`tp_procedimento = 'LAB'`), com a mesma chave
de paciente, e o resultado vem legível (texto limpo em `laudo_transformado`) em mais de 90% dos casos.

| | calprotectina | FAN |
|---|---|---|
| exames/mês (set/2026, solicitações) | 589 | 3.310 |
| exames/mês com resultado (abr–set) | ~340 | ~2.800 |
| alterados/mês | >50 µg/g: ~142 · >200 µg/g: ~67 | reagente ≥ 1:160: ~319 |
| já marcados pela linha | 7–9% | <1% |
| **incremental/mês** | **+60 a +130 (+9% a +20% da DII)** | **+~315 (~2,8× a Reumatologia atual)** |

- Regra numérica/categórica sobre o resultado — **sem NLP de texto**.
- Volume de solicitações (589) maior que o de resultados (~340) na calprotectina: parte dos exames não tem item de
  resultado na gold (no FHIR, ~18% do resultado vem vazio).
- **Riscos:** FAN tem baixa especificidade (título positivo em população sem doença autoimune) — o corte precisa vir
  do negócio; positividade do FAN caiu de ~924 pacientes em abril para ~620/mês desde julho, sem explicação;
  sistema SMART parou de alimentar a gold em jul/2026; laboratório de apoio só aparece quando integrado; entrada de
  laboratório no pipeline é **fonte e contrato novos** — exige alinhamento com o Ops.

## Frente B — linha nova de colo do útero (NIC / ASC-US)

**Viável, com dependências.**

| | citologia cervical | anatomopatológico do colo |
|---|---|---|
| como se identifica | nome do exame (colpocitologia, Papanicolau) **e** texto do "exame citopatológico" genérico | **só pelo texto**, dentro do "exame anatomopatológico" genérico (~37 mil/mês, todos os órgãos) |
| laudos legíveis/mês | ~2.900 | ~1.350 |
| pacientes com alteração/mês | ~119 (ASC-US ~57, baixo grau ~44, alto grau ~14, ASC-H ~7, AGC ~6) | ~166 (NIC ~113; carcinoma/in situ ~48) |

- **Total estimado: ~270–285 pacientes/mês.** O volume da citologia pelo nome do exame foi reproduzido
  (1.662 em set/2026); a maior parte dos achados vem do citopatológico genérico, identificado pelo texto.
- **Régua:** parece config pura (padrão PI-RADS): ~20 termos, negações (NILM domina: 74% da citologia),
  NIC como sistema ordinal; Bethesda tem dois eixos (escamoso e glandular). Estimativa de 1–2 sprints com bancada.
- **Dependências:** definição clínica do que entra (ASC-US isolado? NIC 1? paciente já tratada?); pré-filtro por
  texto para não mandar ~37 mil anatomopatológicos/mês ao motor; laudo da citologia líquida do TASY (só 28% com
  texto — provavelmente em outro sistema).
- **Riscos:** seção de informação clínica do anatomopatológico cita citologia prévia (ASC-US como indicação);
  modelos com legenda Bethesda; "carcinoma" negado; dado sensível de saúde da mulher.

## Limites

Volumetria, não acerto: nenhuma regra foi validada por leitura de casos (exige ambiente governado e adjudicação).
Sobreposição com as linhas medida em ~7 semanas (a saída das linhas não tem histórico maior).
