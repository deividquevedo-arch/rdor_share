---
tags: [mama, birads, migracao-legado]
status: versao-anterior
---

# Migração da mama (BI-RADS) — análise

> v1 · 29/10/2026 · base: [[auditoria-legado-quatro-linhas-2026-10-14|auditoria]],
> [[inventario-legado-quatro-linhas-2026-10-14|inventário]] e [[mapa-migracao-legado-quatro-linhas-v0|mapa]] de 14/10,
> mais as medições de 29/10 abaixo (só agregados). Card do plano: `303791` — *Plano de Migração algoritmos final*;
> card da linha: `304135` — *[BI-RADS] Levantamento para Migração*.

## Em uma frase

**A régua migra por config, sem trabalho de lib; o esforço está fora do motor** — a fonte PACS, o contrato de saída
(categoria de todo exame) e o envio pela Mule, que a plataforma não tem.

## 1. A linha hoje, no legado

| | |
|---|---|
| job | `ia-birads-v2`, workspace antigo, Repo `IAAzureDatabricksBiRads@hml`, disparado por orquestrador sem acesso para o time |
| entrada | gold de exames (filtro `mam[oaá]` em 4 colunas, IMG/IMA, sem biópsia, sem RICHET) **+ PACS** (`hive_metastore.ia.vw_enave_df_historico_birads`) |
| régua | regex + remoção de legenda por frases fixas; categoria máxima do laudo |
| saída | categoria de **todo** exame, de −1 a 6 (`diamond_birads.birads`) |
| envio | GET que dispara a integração Mule (Portal de Acolhimento), espera o log até 4 h, notifica no Teams |
| retorno | `fl_integracao_concluida` na saída |
| extras | e-mail de inconsistências por unidade; correção de duplicidade por data de corte (JSONs em `/mnt`) |

**Volume medido (28/09 a 27/10/2026, entrada e saída do legado):**

| origem | exames | sem categoria (−1) | 0 a 3 | **4** | **5** | **6** | integração concluída |
|---|---|---|---|---|---|---|---|
| gold (RC) | 26.966 | 2.052 | 23.183 | 1.312 | 79 | 340 | 20.492 |
| **PACS** | **3.236** | 296 | 2.748 | 155 | 8 | 29 | 2.563 |
| **total** | **30.202** | 2.348 | 25.931 | **1.467** | **87** | **369** | 23.055 |

~1.000 exames por dia; **1.923 com BI-RADS ≥ 4 em 30 dias (~64/dia)**.

## 2. O que a plataforma já cobre

| componente | evidência |
|---|---|
| **régua** | config pura de `ordinal_extraction` (`systems.bi_rads` + `aggregation_legend_filter`), como a próstata. Bancada de 23/06: motor 99,9% × legado 99,6% na categoria exata, vence 12 de 14 desacordos — `rads/notas/Relatorio-homologacao-birads-bancada-v1.md` |
| **filtro de entrada** | medido em 29/10: o `gold_filter` sobre `proced_descricao` pega **26.515** contra **26.645** do filtro do legado em 4 colunas — **99,4%**; 163 exames de diferença em 30 dias |
| **texto do laudo** | 99,5% dos exames de mama com laudo legível na gold |
| validações (CNPJ, regional, tamanho do nome) | bloco `validacao` da navegação |
| notificação | `tools/data_exchange` |
| dados do paciente em claro | view de exportação |

## 3. O que falta — e de quem é

| # | lacuna | tamanho medido | alçada | caminho |
|---|---|---|---|---|
| 1 | **fonte PACS** | **+3.236 exames (+12%) e +192 com BI-RADS ≥ 4 (+11%) em 30 dias**; a view só existe no `hive_metastore` do workspace antigo | Plat + Neg | trazer a view para o catálogo novo e unir no runner, **ou** decidir aceitar a perda |
| 2 | **contrato de saída** | o legado entrega a categoria de **todo** exame (−1 a 6); a plataforma entrega só relevantes | Plat + Neg | a view passa a expor todas as categorias, **ou** o destino aceita só relevantes |
| 3 | **envio pela Mule** | GET de disparo + espera do log até 4 h; a plataforma só faz `POST /api/ingestao/inferencias` | Plat + Neg | adaptador novo **ou** convergir para a Central de Captação |
| 4 | **regra de relevância** | legado: BI-RADS ≥ 4; motor na bancada: achado clínico **ou** ≥ 4, ~26% a mais | Neg | decidir; régua é config nos dois casos |
| 5 | janela móvel de 30 dias reprocessada | a plataforma processa por dia e deduplica por `id_exame` | Neg | entender se a janela existe por laudo tardio ou pelo contrato da Mule |
| 6 | e-mail de inconsistências por unidade · correção de duplicidade | dois fluxos auxiliares com config em `/mnt` | Neg + Plat | manter, absorver no exchange ou desligar |
| 7 | **bancada desatualizada** | rodada na lib `0.1.1` (06/2026); produção está pinada em `0.12.3` | DS | reexecutar sobre a gold atual |
| 8 | schema `birads` nos três catálogos | — | Fábrica | pedir |
| 9 | **desligar o legado** | o orquestrador dispara mesmo com o job pausado | Plat | dono e acesso ao orquestrador |

ℹ️ **Origem do PACS:** pelo levantamento do card `304135` (encerrado), os dados vêm de um servidor on-premises
acessado por túnel, num processo do Azure Data Factory com orquestrador próprio — trazer o PACS é trabalho de
engenharia de dados, não de config.
⚠️ **Não verificado:** se os exames do PACS chegam à gold depois (laudo tardio). Se chegarem, a perda do item 1 é
de prazo, não de cobertura.

## 4. Sequência

1. **DS, sem dependência:** config `birads` + bancada reexecutada na `0.12.3` sobre a gold atual + run em dev.
2. **Decisões (negócio):** destino (Mule ou Central), contrato (todas as categorias ou só relevantes), regra de
   relevância, PACS.
3. **Plataforma:** schema; PACS no catálogo novo, se mantido; adaptador da Mule ou envio à Central.
4. **Corte:** paralelo em hml com comparação por `id_exame`, **sem envio duplo**; desligamento no orquestrador.

## 5. Decisões a levar — uma linha cada

1. **Destino:** manter a Mule (Portal de Acolhimento) ou convergir para a Central de Captação. — Neg
2. **Contrato:** entregar a categoria de todo exame ou só os relevantes. — Neg + Plat
3. **Relevância:** BI-RADS ≥ 4, ou achado clínico ou ≥ 4. — Neg
4. **PACS:** manter (+12% exames, +11% relevantes) com trabalho de plataforma, ou aceitar a perda. — Neg
5. **Orquestrador:** dono e acesso para o desligamento. — Plat
