---
tags: [mama, birads, migracao-legado, spec]
---

# SPEC de migração — Mama (BI-RADS)

> v0 · 04/11/2026 · trabalho de DS da migração: régua, filtro de entrada e paridade. Base:
> [[analise-migracao-mama-birads-v4|estudo de migração v4]], bancada de junho
> ([[Relatorio-homologacao-birads-bancada-v1|relatório]], [[birads-diferencas-e-decisoes-resumo-v0|decisões]]) e a config
> `0.1.0-birads-rads-v5` (`fabrica-ia-plataforma`, `13a4efe`, 26/06). Cards: `304135` — *[BI-RADS] Levantamento para
> Migração* · `303791` — *Plano de Migração algoritmos final (randomização e central captação)*.

## 1. Escopo

| entra nesta SPEC (DS) | fica fora (plataforma / negócio) |
|---|---|
| config `birads` no formato da plataforma nova | envio para a Mule (peça A do estudo) |
| `gold_filter` medido nos dois sentidos | contrato de saída com a categoria de todo exame (peça B) |
| bancada na lib `0.12.3` sobre a gold atual | retroação de 7 dias na seleção do runner (peça C) |
| paridade por exame contra a saída gravada do legado | schema `birads` (time da Fábrica) |
| run em dev ponta a ponta | as quatro decisões do negócio (estudo v4, §6) |

A régua é desenhada para servir a qualquer decisão: categoria de todo exame **ou** só relevantes, BI-RADS ≥ 4 **ou**
achado clínico — tudo é config.

## 2. Universo de entrada (medido, 28/09–27/10/2026)

| | exames | BI-RADS ≥ 4 |
|---|---|---|
| gold — filtro do legado (4 colunas, IMG/IMA, sem biópsia) | 26.645 | — |
| gold — `gold_filter` sobre `proced_descricao` | 26.515 (**99,4%**) | 1.731 |
| texto legível | 99,5% | |
| laudo liberado depois de D+1 | 1,7% | |

## 3. A régua do legado e o que vira na plataforma

| o que o legado faz | na plataforma |
|---|---|
| regex de BI-RADS sobre o texto normalizado | `ordinal_extraction.systems.bi_rads.patterns` |
| aliases `BI-RADS`, `BIRADS`, `BI RADS`, `ACR BI-RADS`, `categoria` | `aliases` + `patterns` |
| categoria inteira 0–6 (não distingue 4A/4B/4C) | `categories` com **0–6 e 4A, 4B, 4C** — a saída pode achatar para o inteiro se o contrato pedir |
| remove a legenda por frases fixas do emissor | 🔴 `aggregation_legend_filter: {'enabled': True}` (genérico desde a `0.10.1`) |
| categoria = máximo do laudo | `aggregation_policy: 'max_category'` |
| relevante = categoria ≥ 4 | `relevance_mode: 'ordinal_only'` + `promote_categories: 4, 4A, 4B, 4C, 5, 6` |
| sem categoria → `-1` | ausência de categoria; não é anomalia |
| junta o PACS por pedido, depois CPF, depois nome | **não migra** — substituído pela retroação (estudo v4, §4) |

**Ganhos já medidos na bancada (junho):** categoria exata 99,9% (legado 99,6%); nos 14 desacordos, motor certo em 12,
legado em 0. O motor lê 4A/4B/4C, respeita negação ("BI-RADS 4 descartado") e não lê "5ª edição" como categoria 5.

**Limite conhecido, não corrigido:** `max_category` pega a categoria de **exame anterior citado** no laudo (2 casos na
bancada). Restringir à conclusão foi testado e piorou (96,06%) — fica como está, igual ao legado.

## 4. Config proposta

Partir da `0.1.0-birads-rads-v5`, renomear `rads_extraction` → `ordinal_extraction` (a lib aceita os dois; o canônico
é o novo) e alinhar ao formato das configs em produção (referência viva: `ntb_ia_tirads_config.py`).

```python
'segmentation': {'mode': 'full_doc'},
'ordinal_extraction': {
    'enabled': True,
    'aggregation_policy': 'max_category',
    'relevance_mode': 'ordinal_only',          # critério do legado: só a categoria decide
    'systems': {
        'bi_rads': {
            'aliases': ['BI-RADS', 'BIRADS', 'BI RADS', 'ACR BI-RADS', 'categoria'],
            'categories': ['0', '1', '2', '3', '4', '4A', '4B', '4C', '5', '6'],
            'patterns': [...],                  # o da v5, que já cobre US/MG/RM, zero à esquerda e romanos
            'normalization': {'roman_to_arabic': True},
            'aggregation_legend_filter': {'enabled': True},
            'relevance_policy': {'promote_categories': ['4', '4A', '4B', '4C', '5', '6']},
        }
    },
},
'embeddings': {'use_embeddings': False},
'llm_router': {'enabled': False},
```

- **Decisão 3 do negócio (achado clínico):** se entrar, troca `relevance_mode` e o léxico `findings` da v5 (preservado
  lá) passa a valer. Não muda o resto.
- **Sem semântica e sem juiz no primeiro corte:** a paridade se mede contra um legado que não os tem. `rule_only` é
  estágio de desenvolvimento; a escalada mede só o delta na mesma janela.
- **Sai da v5:** `catalog`, `data.legacy`, `runtime.profile` (bloco morto); `gold_filter.mode` (não é lido).

## 5. O que NÃO fazer

- 🔴 Não reproduzir a remoção de legenda por frase fixa — é o defeito, não a regra.
- 🔴 Não usar `max()` sobre a categoria em consulta de auditoria — em string é lexicográfico (`'4C' > '6'`). Agrupar.
- 🔴 Não declarar bloco morto (`catalog`, `monitoring`, `distribution`).
- 🔴 Não juntar o PACS por CPF e nome — o casamento é por paciente e descarta exame legítimo.
- ⚠️ `\b` perto de acento no `gold_filter` casa diferente no Spark 18 — usar fronteira explícita.

## 6. Critérios de aceite

1. **`gold_filter` medido nos dois sentidos**, com custo por termo, em pelo menos dois dias.
2. **Paridade por exame** contra a saída gravada do legado, três dias ou mais: categoria exata e corte ≥ 4, com as
   divergências classificadas em *legado errou* / *motor errou* / *fora de escopo*. Meta de referência: a bancada de
   junho (99,9% de categoria exata).
3. **Legenda provada:** laudos com a escala ACR embutida têm `legend_indices` > 0 e nenhum promove por ela.
4. **4A/4B/4C** contados à parte — o legado não os distingue; não contar como divergência.
5. **Run em dev ponta a ponta** (runner, `gold_filter`, `column_map`, view).
6. Config com **cabeçalho e changelog no arquivo**.

## 7. Passos

| # | passo | depende de |
|---|---|---|
| 1 | pedir o schema `birads` em dev, hml e prd | time da Fábrica |
| 2 | traduzir e medir o `gold_filter` | — |
| 3 | config `birads` a partir da v5, sem semântica e sem juiz | — |
| 4 | bancada local na lib `0.12.3` contra as amostras de junho e a gold atual | — |
| 5 | run em dev e paridade por exame | schema em dev |
| 6 | PR para `hml` | decisões 1 e 2 do negócio (destino e contrato) |
| 7 | depois: semântica e juiz, medindo o delta | homologação |

## 8. Dimensionamento

~1.000 exames/dia, ~58 BI-RADS ≥ 4/dia só pela gold (~64/dia com o complemento do PACS). Volume da ordem do TI-RADS
(~1.300/dia), bem abaixo da reumatologia (~3.400/dia) e das biliares (~3.300/dia). O esforço de DS está no filtro e na paridade; o caminho crítico está na
plataforma (peças A, B e C).
