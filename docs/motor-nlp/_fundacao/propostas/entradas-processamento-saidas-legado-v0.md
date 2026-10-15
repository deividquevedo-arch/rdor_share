---
tags: [migracao, legado, contrato]
---

# Entradas, processamento e saídas — e o que a plataforma precisa evoluir

> v0 — 15/10/2026. Complemento do [[resumo-migracao-legado-quatro-linhas-v0|resumo]] e do
> [[inventario-legado-quatro-linhas-2026-10-14|inventário]]. Só o que o código em produção **usa**.

## 0. Como a `nlp_platform` processa hoje

| etapa | o que faz | limite para as linhas faltantes |
|---|---|---|
| `00_load_control` | registra execução e status | — |
| `01_config` | carrega a config da especialidade | — |
| `02_input` | lê `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, filtra por `gold_filter` sobre **uma coluna** (`proced_descricao`), deduplica por `id_exame`, monta `exm_laudo_texto` a partir do **`laudo_original`** | filtro de uma coluna; sem janela móvel; sem segunda fonte; sem dado de laboratório; sem idade/sexo para o motor |
| `03_persisters` | prepara tabelas de entrada e saída | — |
| `04_process` | roda o `nlp_engine` laudo a laudo; grava `fl_relevante`, `confidence_score`, `findings`, trilha | só processador NLP; saída é **um registro por laudo** |
| `05_monitoring` | taxa, volume | — |
| `06_view` | junta saída + `tb_gold_mov_exame` (`e`) + `tb_gold_mov_paciente` (`p`), descriptografa, filtra pela tabela de controle de envio | projeção pensada para relevantes |
| `07_randomization` | sorteio de controle | — |
| exchange | OneDrive/Logic App (planilha por unidade, lista suspensa, validação) e POST na Central | sem Mule, sem tabela lida por terceiro, sem retorno |

---

## 1. Mama (BI-RADS)

**Entrada**
- `tb_gold_mov_exame`: `id_exame`, `id_paciente`, `dt_exame`, `dt_liberacao_laudo`, `dt_pedido`,
  `tp_procedimento` (IMG/IMA), `dsc_procedimento_integracao`, `nme_codigo`, `dsc_codigo`, `nme_exame`
  (as quatro do filtro `mam[oaá]`), `proced_lista_exames.laudo_transformado`,
  `num_pedido_integracao`, `cod_origem`, `num_porta_entrada`, `num_motivo_entrada`, unidade
  (`id_unidade`, `cnpj_hospital`, `nme_hospital`, `nme_regional_hospital`), médico encaminhador
  (código, CRM, UF, nome, telefone), convênio (`cod_ans_convenio`, `nme_convenio`, plano).
- `tb_gold_mov_paciente`: `doc_cpf`, `cli_nome`, `cli_data_nascimento`, `cli_genero`, `cli_telefone_numero`.
- PACS: `hive_metastore.ia.vw_enave_df_historico_birads` (sobre `enave_df_historico_birads_v202310`).
- Janela: 30 dias até D-1, reprocessada todo dia; sem biópsia; sem unidades `RICHET%`.

**Processamento:** normaliza o laudo, remove a legenda do ACR, extrai a categoria BI-RADS e agrega
pelo máximo. Valida contrato da API (CNPJ, regional, nome da unidade, categoria).

**Saída:** `vw_diamond_mod_birads_complemento` — **todo exame**, com `vl_proced_birads` (−1 a 6),
CPF, nome e telefone do paciente, médico, convênio, unidade, laudo, flag de inconsistência.
Lida pela Mule depois de um GET de disparo.

## 2. Nódulo pulmonar

**Entrada**
- `tb_gold_mov_exame`: `id_exame`, `id_paciente`, `dt_dia_exame`, `dt_liberacao_laudo`,
  `tp_procedimento`, `tp_proced_classe_encontro`, `proced_descricao_ajustado`, `cod_procedimento`,
  `dsc_codigo` (as três do filtro TC/RM de tórax), `proced_lista_exames.laudo_transformado`,
  `nme_origem_exame`, `num_pedido_integracao`, unidade, regional, convênio.
- `tb_gold_mov_paciente`: `doc_cpf`, `cli_nome`, `cli_data_nascimento` (→ **idade**), `cli_genero`,
  DDD e telefone.
- Janela: 30 dias pela liberação do laudo.

**Processamento:** spaCy + `EntityRuler` com 13 dicionários; identifica cada nódulo e extrai
**tamanho (mm), localização, tipo, descrição, agravantes, classe**; avalia evolução contra exame
anterior e conduta recomendada; usa `xlm-roberta-large-xnli` (classificação) e
`paraphrase-multilingual-mpnet-base-v2` (similaridade).

**Saída:** `tb_diamond_mod_pulmao_saida_salesforce` — **um registro por exame com o nódulo
descrito** (37 colunas: paciente, unidade com id Salesforce, exame, laudo, local, tamanho, tipo,
descrição, agravantes, classe, descritores, justificativa e encaminhamento). Lida pelo Salesforce.

## 3. Nefro

**Entrada**
- `gold_corporativo.observation.tb_gold_mov_observation_exames`: TUSS `40301630`, componente `CRE`
  (`valueString` da creatinina), datas do exame (coalesce de 11 campos), paciente, unidade,
  encontro, médico solicitante. Janela: 5 dias.
- `gold_corporativo.patient.tb_gold_mov_patient`: `gender`, `birthDate` (→ **idade**), telefone.
- `practitioner` (CRM e UF), `encounter` e `account` (convênio, plano), `organization` (CNPJ, UF).
- **Sem laudo de texto.**

**Processamento:** limpa o valor de creatinina, calcula a TFG por CKD-EPI com idade e sexo.

**Saída:** `vw_gold_creatinina` — exame com creatinina e **clearance entre 0,1 e 60**, sem AMIL
(24 colunas: paciente, telefone, unidade, médico, convênio, TUSS, creatinina, clearance). Lida por
consumidor não identificado.

## 4. Hepato (ciclo)

**Entrada** — a mesma gold do motor da plataforma, mais `proced_modalidade`, `proced_origem` e o
**retorno do board**: planilhas preenchidas pelos médicos no OneDrive (complexidade, observações).

**Processamento:** (1) motor → (2) extração para o board com lista de complexidade → (3) captura
do retorno → (4) separação em **transplante** e **oncologia + baixa complexidade**.

**Saída:** planilha do board por unidade; `tb_diamond_mod_hepatologia_retorno_v2` (+ histórico);
duas planilhas de navegação, a partir de `vw_diamond_mod_hepatologia_analise_retorno_v2`
(35 colunas, com `complexidade` e `fl_transplante`, `fl_oncologia`, `fl_sem_doenca`, `fl_nao_navegavel`).

---

## 5. O que a plataforma precisa evoluir

| etapa | evolução | serve a |
|---|---|---|
| `02_input` | **filtro em várias colunas** + `tp_procedimento` (hoje só `proced_descricao`) | mama, nódulo |
| `02_input` | **texto do laudo por `laudo_transformado`**, com conversão de RTF como fallback (hoje `laudo_original`) | mama, nódulo, hepato |
| `02_input` | **janela móvel por data** (N dias até D-1) com reprocessamento, além da dedup por `id_exame` | mama, nódulo |
| `02_input` | **segunda fonte** unida à gold (PACS) | mama |
| `02_input` | **atributos do paciente** (idade, sexo) entregues ao processamento | nódulo, nefro |
| `02_input` | **entrada de laboratório** (observação FHIR por TUSS) | nefro |
| `04_process` | **processador não-NLP plugável** (regra/cálculo), no mesmo runner | nefro |
| `04_process` | **saída por lesão** (lista de nódulos com atributos) — exige lib | nódulo |
| `04_process` | **categoria ordinal como coluna** para todos os exames, não só na trilha | mama |
| `06_view` | **projeção sem filtro de relevância** e com colunas de contrato (CPF, telefone, ANS, porta/motivo de entrada, pedido de integração) | mama, nódulo, nefro |
| exchange | **disparo + espera de confirmação** (Mule) | mama |
| exchange | **tabela de entrega** com contrato fixo, lida por sistema externo | nódulo, nefro |
| exchange | **captura de retorno** de planilha preenchida → tabela com histórico | hepato |
| exchange | **navegação a partir do retorno**, roteada por campo (complexidade) | hepato |
| orquestração | **job de pós-processamento** com agenda própria (retorno dias úteis 07:00) | hepato |
| artefatos | modelos e dicionários em **Volume** de prd | nódulo |
