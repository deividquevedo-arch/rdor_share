---
tags: [randomizacao, uplift, poder-estatistico]
---

# Poder estatístico — 5% × 10% de controle, com parâmetros medidos

> v1 — 22/10/2026. Substitui as premissas do [[horizonte-uplift-por-linha-v0]] (CV único de 3,5 e janela só de 30
> dias) por parâmetros **medidos em coortes reais**, por linha e por janela, e pelo volume observado da Onda 1.
> Pergunta: *qual a menor diferença que conseguimos enxergar com 5% e com 10%, e em quanto tempo?*

## Resposta curta

1. **10% chega a qualquer resultado em ~53% do tempo de 5%** (razão de 1,89, igual para todas as métricas).
2. **Retorno é decidível em meses.** Na janela de 90 dias, +5 p.p. aparece com **12,5 meses de acúmulo com 5%** e
   **6,6 com 10%**; +8 p.p. com 4,9 e 2,6.
3. **Consumo médio (incluindo quem tem consumo zero) é lento com qualquer fração.** Na Onda 1, com 18 meses de
   acúmulo e janela de 90 dias, o menor ganho detectável é **41% com 5%** e **29% com 10%** (33% e 24% com o ajuste
   pelo consumo anterior). Ganhos de 20% levam de 2 a 6 anos.
4. **As ondas 2 e 3 mudam a conta:** somando linhas, 5% sobre todas elas acumula controle em ritmo parecido com 10%
   só na Onda 1 — se o consumo delas variar de forma parecida (a medir).

## Parâmetros medidos

Coortes: pacientes com laudo relevante — Reumatologia (legado, exames de jan–ago/2026, CPF decifrado) e Tireoide
(plataforma hml, exames desde 16/07/2026). Consumo na `vw_mov_fato_consumo` (sem destino `H`), depois do exame.
Valor com **teto no percentil 99** (tratamento de outlier a pré-registrar). O CPF casou em ~100% das coortes (o
próprio exame é faturado); a conta usa 95%, conservador.

| linha | janela | n | retorno | média (com teto) | CV | correlação com consumo dos 180 dias anteriores |
|---|---|---|---|---|---|---|
| Reumatologia | 30 | 1.887 | 64% | R$ 11.738 | 3,25 | 0,50 |
| Reumatologia | 60 | 1.622 | 74% | R$ 17.147 | 3,25 | 0,55 |
| Reumatologia | 90 | 1.342 | 79% | R$ 21.650 | 3,15 | 0,61 |
| Reumatologia | 180 | 434 | 85% | R$ 24.732 | 2,83 | 0,64 |
| Tireoide | 30 | 5.723 | 60% | R$ 2.249 | 3,71 | 0,37 |
| Tireoide | 60 | 2.536 | 71% | R$ 3.550 | 2,93 | 0,37 |
| Tireoide | 90 · 180 | — | 76% · 81% | R$ 4.482 · R$ 5.120 | 2,84 · 2,55 | 0,37 |

⚠️ **Tireoide 90/180 é extrapolada** da janela de 60 pela evolução observada na Reumatologia: a coorte da plataforma
começa em julho e a base de consumo vai só até 29/09.
⚠️ **Sem o teto no p99 a variação dobra na Tireoide** (CV 6,4 em 30 dias): a regra de outlier é decisão que pesa.

**Volume (prd, 14–22/10):** Tireoide **789** e Reumatologia **204** pacientes sorteados por mês — **993** na Onda 1.
Com 5%, ~50 pacientes de controle por mês; com 10%, ~100.

## Menor ganho detectável — Onda 1, janela de 90 dias

95% de confiança, 80% de poder, estimativa estratificada por linha.

| meses de acúmulo | retorno 5% | retorno 10% | consumo 5% | consumo 10% | consumo com ajuste 5% | consumo com ajuste 10% |
|---|---|---|---|---|---|---|
| 6 | 7,2 p.p. | 5,3 p.p. | 70% | 51% | 57% | 41% |
| 12 | 5,1 p.p. | 3,7 p.p. | 50% | 36% | 40% | 29% |
| 18 | 4,2 p.p. | 3,0 p.p. | 41% | 29% | 33% | 24% |
| 24 | 3,6 p.p. | 2,6 p.p. | 35% | 26% | 29% | 21% |

Janelas de 30 e 60 dias dão números um pouco piores; a de 180 um pouco melhores, mas soma 6 meses de espera
(tabelas completas reproduzíveis pelo script de cálculo).

## Tempo até o primeiro resultado

Meses de **acúmulo** + **janela** + **atraso da base de consumo**. Em 22/10 a base ia só até 29/09 (23 dias), com
setembro incompleto — a carga parece mensal: contar **~1 mês** de atraso, não 2 semanas.
Ex.: retorno +5 p.p., janela 90, 10% → 6,6 + 3 + 1 ≈ **10,5 meses**; com 5% → 12,5 + 3 + 1 ≈ **16,5 meses**.

## Leituras interinas

Possíveis e recomendáveis (6 e 12 meses, por exemplo), desde que datas e critério de parada sejam combinados
antes (regra de gasto de alfa, tipo O'Brien-Fleming). O custo é ~3% a mais de amostra na leitura final. Olhar sem
regra e parar quando der "significativo" infla o falso positivo.

## O que muda a decisão

| opção | custo operacional | o que entrega |
|---|---|---|
| **10% na Onda 1** | ~50 pacientes a mais por mês sem navegação ativa | retorno em ~6–7 meses para +5 p.p.; consumo só para ganhos ≥ ~30% em 1,5 ano |
| **5% na Onda 1** | o menor | retorno em ~12–13 meses para +5 p.p.; consumo só para ganhos ≥ ~40% em 1,5 ano |
| **5% em todas as ondas** | o menor | controle acumulado parecido com 10% só na Onda 1, **se** a variação do consumo das novas linhas for parecida — falta medir |

## Limites

- Reumatologia 180 dias com n = 434; Tireoide 90/180 extrapolada.
- Coorte histórica de pacientes relevantes, não a população sorteada — o volume é o observado, o comportamento
  do consumo é o histórico.
- Ondas 2 e 3 sem parâmetros medidos; biliares, neuro e próstata sem volume no relatório de execução.
