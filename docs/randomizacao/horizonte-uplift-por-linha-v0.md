---
tags: [randomizacao, uplift, poder-estatistico]
---

# Quando teremos o resultado de uplift — por linha e por fração de controle

> v0 — 19/10/2026. Responde à pergunta: *quantos meses a linha precisa rodar para um resultado
> confiável com 5% e com 10% de controle?* Ligação: [[metodo-avaliacao-por-onda-v0]] §7 (uplift).

## Resposta curta

| o que se quer detectar | Onda 1 (5% / 10%) | Tireoide (5% / 10%) | Reumatologia (5% / 10%) |
|---|---|---|---|
| **conversão +10 p.p.** (voltou a consumir) | 4 / **2** meses | 4 / 2 | 26 / 14 |
| **conversão +8 p.p.** | 6 / **3** meses | 7 / 4 | 40 / 21 |
| **conversão +5 p.p.** | 15 / **8** meses | 18 / 9 | 102 / 54 |
| **consumo em R$ +30%** | 21 / **11** meses | 25 / 13 | 143 / 75 |
| **consumo em R$ +20%** | 48 / **25** meses | 56 / 29 | 321 / 170 |

**Somar a esse prazo:** a janela de observação (30 ou 90 dias depois do último sorteado) e o
atraso da base de consumo (~2 semanas). Ex.: conversão +8 p.p. com 10% na onda → 3 meses de
acúmulo + 90 dias de janela + 2 semanas ≈ **6,5 meses** até o primeiro resultado confiável.

## Leitura

1. **10% chega ao resultado na metade do tempo de 5%.** É estrutural: o controle cresce no dobro
   da velocidade.
2. **Por linha isolada, só a tireoide tem volume.** A reumatologia sorteia ~158 pacientes/mês
   desde que o escopo de unidades passou a valer (14/10) — qualquer leitura dela sozinha leva anos.
   **A leitura é por onda**, e as ondas 2 e 3 encurtam o prazo ao somar pacientes.
3. **Medir consumo em R$ é lento; medir conversão é rápido.** O consumo por paciente tem cauda
   muito longa (mediana ~R$ 130, média ~R$ 1.900–3.500 em 30 dias): o ruído esconde ganhos de 20%
   por anos. A conversão varia pouco e responde em meses.
   **Recomendação:** indicador principal = **conversão**; consumo em R$ como secundário, com ajuste
   pelo consumo anterior ao sorteio (reduz o ruído) e leitura em horizonte mais longo.

## Como foi calculado

- **Volume:** pacientes sorteados por dia útil em PRD desde 14/10 (escopo atual de unidades) × 21:
  tireoide **908/mês**, reumatologia **158/mês**, onda **1.066/mês**.
- **Variabilidade do desfecho, medida (não suposta):** pacientes com achado relevante e exame até
  30/08, consumo nos 30 dias seguintes ao exame, na base de consumo (CPF decifrado, 107 de 109
  casam):

| amostra | pacientes | % que consumiu | mediana | média | CV | CV sem o 1% mais alto |
|---|---|---|---|---|---|---|
| Reumatologia (prd) | 97 | 56,7% | R$ 135 | R$ 3.562 | 3,51 | 3,38 |
| Tireoide (prd) | 904 | 57,1% | R$ 116 | R$ 1.867 | 4,87 | 3,53 |
| Tireoide (hml, mais histórico) | 5.713 | 60,1% | R$ 136 | R$ 3.084 | 6,42 | 3,73 |

  Usado **CV = 3,5** (sem o 1% mais alto) e **conversão base = 58%**.
- **Critério:** intervalo de 95% e 80% de poder (z = 1,96 + 0,84), comparação de médias com grupos
  de tamanhos 5/95 ou 10/90.
- ⚠️ **A premissa do estudo de 23/09 (CV 2,05) subestimava a dispersão** — os prazos em R$ daqui são
  ~3× maiores que os que circularam ("detecta 14% em um ano" não se sustenta para consumo em R$).
- ⚠️ A janela medida é de **30 dias** (a de 90 ainda não fechou para nenhuma coorte). Em 90 dias a
  conversão sobe e o CV tende a cair um pouco; os prazos podem encurtar, não alongar muito.
- ⚠️ O ganho real é desconhecido: a tabela diz **quanto tempo para enxergar** cada tamanho de ganho,
  se ele existir.
