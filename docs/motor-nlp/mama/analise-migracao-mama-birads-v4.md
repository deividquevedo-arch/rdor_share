---
tags: [mama, birads, migracao-legado]
---

# BI-RADS (mama) — migração do legado para a plataforma NLP

> v4 · 04/11/2026 · fecha o estudo. Unifica a [[analise-migracao-mama-birads-v3|v3]] (29/10) com a
> [[avaliacao-fonte-pacs-rc-flowhub-v1|avaliação da fonte RC × PACS × FlowHub]] (03/11). Medições de 28/09 a
> 27/10/2026, só agregados, no código e nas tabelas que rodam em produção (`IAAzureDatabricksBiRads`, branch `hml`).
> Cards: `303791` — *Plano de Migração algoritmos final (randomização e central captação)* · `304135` —
> *[BI-RADS] Levantamento para Migração*.

## 1. Conclusão

- **A leitura do laudo está resolvida:** a régua migra por config (`ordinal_extraction`), sem código novo na lib.
- **A fonte PACS deixa de ser peça cara.** 89% do que o legado busca no PACS chega ao repositório clínico (RC) dias
  depois. A plataforma perde esses exames porque seleciona pela **data do exame de D-1**. Selecionar com
  **retroação de 7 dias** recupera **165 dos 192** BI-RADS ≥ 4 por mês, sem fonte nova.
- **Faltam duas peças de plataforma** — envio para a Mule e contrato de saída — e **quatro decisões do negócio**.

## 2. A linha em números (30 dias)

| | exames | BI-RADS ≥ 4 |
|---|---|---|
| RC (gold) | 26.966 | 1.731 |
| PACS, complemento do legado | 3.236 | 192 |
| **total** | **30.202** (~1.000/dia) | **1.923** (~64/dia) |

## 3. O que já está coberto

| | evidência |
|---|---|
| régua | config de `ordinal_extraction`, como o PI-RADS em produção; bancada: 99,9% de acerto da categoria (legado 99,6%) |
| filtro de entrada | 99,4% do filtro do legado (163 exames de diferença em 30 dias) |
| texto do laudo | 99,5% legível no RC |
| validação, notificação, dados do paciente | mecanismos existentes da plataforma |

## 4. O que falta

| | peça | o que é | alçada |
|---|---|---|---|
| **A** | envio para a Mule | o legado grava a saída no `hive_metastore` do workspace antigo e a Mule busca de lá; a plataforma não tem esse caminho. Ou a Mule lê do catálogo novo, ou a linha envia à Central de Captação | Plataforma + Mule |
| **B** | contrato de saída | o BI-RADS entrega a categoria de **todo** exame (−1 a 6), um registro por paciente por dia (a maior categoria), descartando registro sem campo obrigatório; a view atual entrega só relevantes | Plataforma |
| **C** | retroação na seleção | selecionar exames dos últimos 7 dias, com a deduplicação por `id_exame` que já existe, em vez de só D-1 | Plataforma |

### Por que C substitui a fonte PACS

| fonte | BI-RADS ≥ 4 do PACS recuperados | custo | campos do envio |
|---|---|---|---|
| **RC + retroação de 7 dias** (recomendada) | **165 de 192 (86%)** | leitura maior do RC, filtrada por mama | completos |
| RC + FlowHub como 2ª fonte | 192 de 192 | ~5× por leitura; runner com 2ª fonte, exclusão por pedido, identificador próprio | precisa do RC para convênio, CNPJ e regional |
| FlowHub como fonte única | 192 de 192 | idem | **inviável**: sem convênio, CNPJ e regional |

- **Atraso de chegada ao RC** dos 2.894 que chegam: 1.838 até D+1 · 1.010 entre 2 e 7 dias · 46 após 7 dias.
  Medido pela última atualização do registro, portanto **limite superior** — 7 dias é janela conservadora.
- **O que fica fora:** 342 exames/mês que nunca chegam ao RC, com **27 BI-RADS ≥ 4** (7 deles BI-RADS 6, câncer já
  diagnosticado) — ~1,4% dos casos ≥ 4 da linha.
- **O FlowHub contém o RC:** 25.278 de 25.287 mamografias do RC (99,96%) estão nele, e 100% do complemento PACS.
  A ressalva de que "não tem todos os dados do RC" vale para os **campos do envio**, não para os exames.
- **A retroação beneficia todas as linhas** com laudo tardio (1,7% dos exames de mama; outras linhas não medidas).

## 5. Itens menores

| item | alçada |
|---|---|
| e-mail de inconsistências por unidade (Excel via Logic App) | Negócio + Plataforma |
| schema `birads` em dev, hml e prd | Fábrica |
| desligar o legado — acesso ao orquestrador do workspace antigo | Plataforma |
| config `birads`, bancada na lib `0.12.3`, run em dev | DS |
| colunas de senha expostas em `especialistas.pacs_streaming.unidade_gold` | Governança |

## 6. Decisões do negócio

1. **Destino:** manter a Mule ou ir para a Central de Captação? → define A.
2. **Contrato:** categoria de todo exame ou só relevantes? → define B.
3. **Relevância:** BI-RADS ≥ 4, ou também achado clínico (~26% a mais)? → config nos dois casos.
4. **Os 27 BI-RADS ≥ 4/mês que nunca chegam ao RC:** aceitar a perda, ou manter uma 2ª fonte (FlowHub) só para eles?

## 7. Sequência

1. **Já:** config e bancada (DS) · schema (Fábrica) · retroação de 7 dias no runner, medida em dev (Plataforma).
2. **Após as decisões:** peças A e B.
3. **Paralelo em hml:** legado e plataforma na mesma janela, comparação por exame, sem envio duplo.
4. **Corte:** desligar o legado no orquestrador.

## 8. Não verificado

- Retorno da Mule (`logs.api_birads`): sem permissão de leitura.
- Atraso pela **primeira** chegada ao RC (não pela última atualização) — só reduziria a janela.
- Volume extra da retroação nas outras linhas.
