---
tags: [mama, birads, migracao-legado]
status: versao-anterior
---

# BI-RADS (mama) — migração do legado para a plataforma NLP

> v2 · 29/10/2026 · documento de alinhamento com a plataforma: o que falta construir, de quem é cada item e o que o
> negócio precisa decidir antes.
> Fontes: código que roda em produção (`IAAzureDatabricksBiRads`, branch `hml`, `75a5898`), tabelas do legado no
> workspace antigo e da gold no workspace novo, medidas em 29/10/2026 — só agregados.
> Substitui a v1 (mesma pasta). Cards: `303791` — *Plano de Migração algoritmos final* · `304135` — *[BI-RADS]
> Levantamento para Migração*.

## 1. Resposta curta

**A leitura do laudo não é o problema — a régua migra por config, sem código novo na lib.** O que impede a migração
são **três capacidades que a plataforma não tem hoje**, todas fora do motor:

| # | capacidade que falta | por que importa |
|---|---|---|
| **A** | **saída e envio para a Mule** | a Mule (Portal de Acolhimento) lê uma tabela no `hive_metastore` do workspace antigo e grava o retorno lá; a plataforma só sabe fazer `POST` à Central de Captação |
| **B** | **contrato de saída do BI-RADS** | o legado entrega a categoria de **todo** exame (−1 a 6), um paciente por dia, com campos obrigatórios; a plataforma entrega só relevantes |
| **C** | **fonte PACS** | +3.236 exames (+12%) e **+192 com BI-RADS ≥ 4 (+11%) em 30 dias**; vem de servidor on-premises via Azure Data Factory |

O envio (A) é o bloqueio mais visível, mas **B e C também exigem construção** — não são config.

## 2. Volume da linha (legado, 28/09 a 27/10/2026)

| origem | exames | BI-RADS −1 | 0 a 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| gold (RC) | 26.966 | 2.052 | 23.183 | 1.312 | 79 | 340 |
| PACS | 3.236 | 296 | 2.748 | 155 | 8 | 29 |
| **total** | **30.202** | 2.348 | 25.931 | **1.467** | **87** | **369** |

~1.000 exames/dia · **1.923 com BI-RADS ≥ 4 em 30 dias (~64/dia)**.

## 3. Como o legado funciona hoje

Job `ia-birads-v2` no workspace antigo, disparado por um orquestrador sem acesso para o time. Oito tarefas:

1. **entrada** — gold de exames: `mam[oaá]` em 4 colunas, IMG/IMA, sem biópsia, sem unidade `RICHET%`, laudo com
   mais de 10 caracteres. Janela de 30 dias pela **data mais recente** entre emissão, pedido, liberação e entrega do
   laudo; **cada exame entra uma vez** (exclui o que já entrou em execução anterior).
2. **PACS** — `hive_metastore.ia.vw_enave_df_historico_birads`; entra o que **não** casou com a gold por pedido, CPF
   ou nome; laudo convertido de RTF/HTML; marcado `cod_origem = 'PACS'`.
3. **régua** — regex, remoção de legenda por frases fixas, categoria máxima do laudo.
4. **views** — `tb_diamond_mod_birads_complemento` e `vw_diamond_mod_birads_complemento` (catálogo
   `diamond_birads.birads`): **um registro por paciente por dia** (maior categoria), auditoria de inconsistências,
   descarta registro sem nascimento, sexo, convênio, nome, telefone, unidade, regional ou CNPJ.
5. **view legado** — copia para `hive_metastore.ia.tbl_gold_modelo_birads_complemento_v2_lake2` (447.949 linhas,
   último exame 26/10) com criptografia do `hive_metastore`, exposta em `ia.vw_gold_modelo_birads_complemento`.
   **É o que a Mule lê.**
6. **inconsistências** — planilha Excel por unidade, enviada por e-mail (Logic App + OneDrive).
7. **envio** — `GET` em `onco-navegadoras-exp-api(-prod).br-s1.cloudhub.io/api/navegadoras/inicial` com
   `dataInicial`, `dataFinal`, `flagIntegracao=false` e autenticação básica (secret do Databricks). O `GET` dispara a
   Mule, que **busca** os dados. O job espera até **4 h**, verificando a cada minuto, até a Mule gravar o retorno em
   `hive_metastore.logs.api_birads` (`status_integracao = '200'`); então marca `fl_integracao_concluida`.
8. **monitoramento** — estatística diária e alerta de volume; notificações no Teams.

Configurações fora do repositório (`/mnt/…/config/hml/`): notificações, e-mail de inconsistências, unidades das
inconsistências e a data de corte da regra de duplicidade.

## 4. O que a plataforma já cobre

| componente | evidência |
|---|---|
| **régua** | config de `ordinal_extraction` (`systems.bi_rads` + `aggregation_legend_filter`), como o PI-RADS em produção. Bancada (06/2026): categoria exata 99,9% × 99,6% do legado; o motor vence 12 de 14 desacordos (`rads/notas/Relatorio-homologacao-birads-bancada-v1.md`) |
| **filtro de entrada** | `gold_filter` sobre `proced_descricao`: **26.515** contra **26.645** do filtro do legado — **99,4%**, 163 exames de diferença em 30 dias |
| **texto do laudo** | 99,5% legível na gold |
| **laudo liberado com atraso** | 98,3% dos laudos de mama saem até o dia seguinte ao exame; **1,7%** (431 em set/2026) depois — a janela por `dt_dia_exame` da plataforma não os recupera |
| validação de campos, notificação, descriptografia na view | mecanismos existentes (`validacao` da navegação, `tools/data_exchange`, view de exportação) |

## 5. Complementos necessários

| # | item | o que construir | alçada |
|---|---|---|---|
| A1 | **destino da saída** | ou a Mule passa a ler uma view no catálogo novo (Unity Catalog do workspace da plataforma) e a gravar o retorno lá, ou a linha converge para a Central de Captação | Integração (Mule) + Plataforma + Negócio |
| A2 | **disparo e retorno** | `GET` de disparo + espera do log + atualização de `fl_integracao_concluida` — não existe no `data_exchange` | Plataforma |
| B1 | **todas as categorias na saída** | a view de exportação expor a categoria de todo exame, não só `fl_relevante = 1` | Plataforma + Negócio |
| B2 | **um paciente por dia** | deduplicação por paciente e dia, maior categoria (a da plataforma é por paciente + data + procedimento) | Plataforma |
| B3 | **campos obrigatórios e defaults** | mesmas regras de descarte e valores padrão da view do legado | Plataforma |
| C1 | **PACS** | trazer `vw_enave_df_historico_birads` para o catálogo novo (origem on-premises via ADF) e unir no runner com a regra "só o que não casou com a gold" | Engenharia de dados + Plataforma |
| D1 | laudo liberado com atraso | janela por data de liberação, ou reprocessar exames cujo laudo chegou depois (~1,7%) | Plataforma |
| D2 | e-mail de inconsistências por unidade | absorver no `data_exchange` ou desligar | Negócio + Plataforma |
| D3 | schema `birads` em dev, hml e prd | provisionar | Fábrica |
| D4 | **desligar o legado** | acesso e dono do orquestrador do workspace `bi-hml-02` | Plataforma |
| E1 | config `birads` + bancada na `0.12.3` + run em dev | régua, filtro, navegação, job | DS |

## 6. Decisões do negócio, antes de construir

1. **Destino:** manter a Mule (Portal de Acolhimento) ou convergir para a Central de Captação. Define A1 e A2.
2. **Contrato:** categoria de todo exame ou só relevantes. Define B1.
3. **Relevância:** legado = BI-RADS ≥ 4 (decidido no Portal); na bancada o motor também marcaria achado clínico,
   ~26% a mais. A régua é config nos dois casos.
4. **PACS:** manter (+11% dos casos ≥ 4, exige C1) ou aceitar a perda.

## 7. Sequência

1. **Agora, sem dependência:** E1 (DS) e D3 (Fábrica).
2. **Com as decisões 1 a 4:** A1/A2 ou Central; B1–B3; C1 se mantido.
3. **Paralelo em hml:** legado e plataforma na mesma janela, comparação por `id_exame`, **sem envio duplo**.
4. **Corte:** desligar no orquestrador (D4).

## 8. Não verificado

- `hive_metastore.logs.api_birads` — sem permissão de leitura; volume e taxa de sucesso do retorno não medidos.
- Se os exames do PACS chegam à gold depois (laudo tardio) — define se a perda de C1 é de prazo ou de cobertura.
- Quem consome `fl_integracao_concluida` além do próprio job.
