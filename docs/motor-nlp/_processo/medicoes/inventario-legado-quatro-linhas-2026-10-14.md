---
tags: [inventario, migracao, legado]
---

# Inventário do legado — Mama, Nódulo pulmonar, Nefro e Hepato

> **Levantamento, sem investigação.** 14/10/2026, workspace `workspace-analytics-bi-hml-02`
> (perfil `adb-2013197995950192`), em leitura. Nenhuma linha de paciente consultada: só estrutura,
> contagens e metadados.
> Complementa a [[auditoria-legado-quatro-linhas-2026-10-14|auditoria]] e o
> [[mapa-migracao-legado-quatro-linhas-v0|mapa de migração]].
>
> **Cópias fora do git** (contêm e-mail de destinatário e URL de Logic App/Teams):
> `Desktop/Rede D'Or/_legado-repos/_inventario-mnt/` (configs) e `_inventario-ddl/` (definição das views).

---

## 1. Jobs e execução

| | Mama | Nódulo | Nefro | Hepato — motor e board | Hepato — retorno e navegação |
|---|---|---|---|---|---|
| job | `ia-birads-v2` | `ia-pulmao` | `ia-rim` | `ia-hepatologia-envio-clone` | `ia-hepatologia-retorno-navegacao-clone` |
| disparo | orquestrador (`RUN_JOB_TASK`) | orquestrador; agenda própria pausada | orquestrador; agenda própria pausada | orquestrador | **agenda própria**, dias úteis 07:00 |
| Repo · branch · commit | `IAAzureDatabricksBiRads` · `hml` · `75a5898` | `IAAzureDatabricksPulmao` · `hml` · `c0ee4d3` | `IAAzureDatabricksRim` · `hml` · `df21725` | `IAAzureDatabricksHepatologia` · `hml` · `a6a33ec` | idem |
| runtime | `14.3.x-cpu-ml` | `14.3.x` | `14.3.x` | `14.3.x` | `14.3.x` |
| cluster | driver `E16as_v4`, 1–2 × `E4as_v4`, com policy | idem | idem | idem | idem |
| parâmetros | `catalog=diamond_birads`, `schema=birads`, `environment=hml`, `origem=api`, `work_schema=workarea` | `environment=hml` | nenhum | `catalog=diamond_hepatologia`, `schema=hepatologia`, `environment=hml`, `origem=batch` | idem + `navegacao=transplante\|oncologia` |
| notificação de falha | 4 e-mails | **nenhuma** | 4 e-mails | 4 e-mails | 7 e-mails |

**Tarefas:**
- **Mama:** `tabela_entrada` → `add_dados_pacs` → `algoritmo_birads` → `views` → `view_legado` →
  `analise_inconsistencias` → `executa_api_e_monitora` → `monitoramento`.
- **Nódulo:** `entrada` → `algoritmo` → `monitoramento`.
- **Nefro:** `modelo` → `monitoramento`.
- **Hepato (envio):** `setup` → `tabela_entrada` → `algoritmo` → `extracao` → `monitoramento`.
- **Hepato (retorno):** `Setup` → `Retorno` → `navegacao_transplante` e `navegacao_oncologia` → `monitoramento`.

⚠️ O orquestrador não é visível para o usuário que fez o levantamento; dono e definição pendentes.
⚠️ `environment=hml` em todos os jobs produtivos, e a configuração lida é a de `config/hml/`.

---

## 2. Entradas

| linha | fonte | existe no workspace novo? |
|---|---|---|
| Mama | `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `tb_gold_mov_paciente` | ✅ |
| Mama | `hive_metastore.ia.vw_enave_df_historico_birads` (PACS) | 🔴 **não** — só no workspace antigo |
| Nódulo | `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `tb_gold_mov_paciente` | ✅ |
| Nefro | `gold_corporativo.{observation.tb_gold_mov_observation_exames, patient.tb_gold_mov_patient, organization.tb_gold_mov_organization, practitioner.tb_gold_mov_practitioner, encounter.tb_gold_mov_encounter, account.tb_gold_mov_account}` | ✅ as seis |
| Hepato | `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `tb_gold_mov_paciente` | ✅ |

---

## 3. Configurações em `/mnt/trusted/datalake/ia/projetos/<projeto>/config/hml/`

| linha | arquivo | lido por | conteúdo |
|---|---|---|---|
| Mama | `birads/.../notificacoes.json` | `monitoring/ntb_ia_analise_logs_{erro,sucesso}` | títulos, URL de chat (Teams) e de e-mail (Logic App), 5 destinatários |
| Mama | `birads/.../confg_email_inconsistencias.json` | `monitoring/ntb_ia_analise_inconsistencias` | usuário OneDrive, URL de Logic App, pasta, assunto e corpo |
| Mama | `birads/.../config_unidades_inconsistencias.json` | idem | 1 grupo de unidades com destinatários |
| Mama | `api-birads/.../configCorrecaoDuplicidade.json` | `serving/ntb_ia_views` | uma data de corte |
| Mama | `monitoramento-enf-naveg/config/notificacoes*` | `monitoring/ntb_ia_analise_logs_notificacoes_diarias` | notificação diária |
| Nódulo | 10 × `Descritores-*.csv` + `classificacao-e-localizacao-ia-{riscopt,tipoPT}.csv` | `model/ntb_ia_predicao` | **dicionários da régua**: uma categoria por linha, termos com `%` como curinga |
| Nódulo | `arquivo_depara_unidades_salesforce.csv` (100 unidades) | idem | unidade do lake → unidade, URL e bucket do Salesforce |
| Nefro | — | — | **nenhuma configuração externa** |
| Hepato | `config_v2.json` | `serving/ntb_ia_hepatologia_extracao` | e-mail do board (Logic App, pasta, assunto) |
| Hepato | `unidades.json` (3 grupos) | `extracao` e `retorno` | destinatários do board por unidade |
| Hepato | `config_v2_2.json` | `serving/ntb_ia_hepatologia_navegacao` | e-mail da navegação |
| Hepato | `navegacao_transplante.json`, `navegacao_oncologia.json` (3 grupos cada) | idem | destinatários por fluxo |
| Hepato | `config.json` (JSON inválido), `navegacao.json`, `unidades_ftp.json` | **ninguém** | mortos — não migrar |

Os 13 dicionários do nódulo, por categoria (linhas = categorias):

| arquivo | categorias | exemplo de cabeçalho |
|---|---|---|
| `Descritores-ia-descritores-fora-pulmao` | 39 | `PAREDE` |
| `Descritores-ia-estruturais-sec` | 23 | `SOLIDO` |
| `classificacao-e-localizacao-ia-tipoPT` | 19 | `HIPODENSO` |
| `Descritores-qualitativos-m-ou-s` | 12 | `ONCOLOGICO` |
| `Descritores-ia-comparativos` | 6 | `COMPARATIVOS` |
| `Descritores-ia-qualitativo-benign` | 6 | `DESC. BENIGNA` |
| `Descritores-ia-qualitativo-de-con` | 6 | `SUGESTAO1` |
| `classificacao-e-localizacao-ia-riscopt` | 4 | `ESPICULADO` |
| `Descritores-ia-qualitativos-indet` | 3 | `INDETERMINADO` |
| `Descritores-ia-descritores-estruturais-prim2` | 2 | `NODULO` |
| `Descritores-ia-frasepadrao` | 1 | `FRASEPADRAO` |
| `Descritores-ia-procedimento` | 1 | `EXAME BIOPSIA` |
| `arquivo_depara_unidades_salesforce` | 100 unidades | — |

🔴 As configs de mama e hepato carregam **URL de Logic App e de Teams** — vão para secret scope.

---

## 4. Modelos e dependências

| linha | modelos | dependências instaladas no notebook |
|---|---|---|
| Mama | — | `openpyxl`, `xlsxwriter`, `striprtf==0.0.22`, `lxml==4.9.4`; `nltk.download('all')` |
| Nódulo | `xlm-roberta-large-xnli` e `paraphrase-multilingual-mpnet-base-v2` em **`/Volumes/diamond_fabrica_ia_dev/pulmao/huggingface/`** (catálogo de **dev**); spaCy `pt_core_news_lg`/`sm` 3.5.0 baixados do GitHub | `spacy==3.5.3`, `spacy-udpipe`, `transformers` (4.16.0 e 4.32.0 no mesmo código), `sentence-transformers==2.2.2`, `tensorflow==2.11.0`, `pytorch-lightning==1.4.9`, `huggingface_hub==0.16.4`, `protobuf==3.20.3`, `nltk==3.7`, `striprtf==0.0.20` |
| Nefro | — | nenhuma |
| Hepato | spaCy `pt_core_news_lg` | `openpyxl`, `xlsxwriter`, `unidecode`; `nltk` (`punkt`, `punkt_tab`, `stopwords`) |

---

## 5. Secrets, destinos externos e criptografia

| linha | secrets | hosts | criptografia | storage |
|---|---|---|---|---|
| Mama | `dbricks-keyvault-secrets/api-mule-birads-portalacolhimento-prd-secret` | Mule `onco-navegadoras-exp-api(-prod).br-s1.cloudhub.io`; Logic App `prod-111.westus`; SharePoint | `rdsl_decrypt`, `rdsl_encrypt`, `unencrypt` | `sardslusdevelopmenthml` |
| Nódulo | — | **nenhum** — o Salesforce lê a tabela | `rdsl_decrypt` | — |
| Nefro | — | **nenhum** — a view é lida de fora | `rdsl_decrypt`, `rdsl_encrypt`, `unencrypt` | — |
| Hepato | — (assinatura SAS **no código**) | Logic App `prod-05.eastus2` | `rdsl_decrypt` | `sardslusdevelopmenthml` |

---

## 6. Tabelas e views em uso

Só o que o código em produção grava ou o que as views de entrega leem. O resto dos schemas é
resíduo de teste e de versões anteriores (`dev_*`, `dev_mig_*`, `tbl_temp_*`, `*_AAAAMMDD`):
**366 objetos** no `hive_metastore.ia`, **38** em `diamond_hepatologia.workarea`, **16** em
`diamond_birads.workarea` — não migram.

| linha | objeto | tipo | linhas | papel |
|---|---|---|---|---|
| Mama | `diamond_birads.birads.tb_diamond_mod_birads_entrada` | tabela | 589.753 | entrada |
| Mama | `diamond_birads.birads.tb_diamond_mod_birads_saida` | tabela | 589.237 | categoria por exame |
| Mama | `diamond_birads.birads.tb_diamond_mod_birads_complemento` | tabela | 587.540 | saída enriquecida; `MERGE` em 14/10 |
| Mama | `diamond_birads.birads.tb_diamond_mod_birads_entrada_validacao` | tabela | 4.728 | validação de contrato |
| Mama | `diamond_birads.birads.tb_diamond_mod_monitoramento_enfermeira_navegadora` | tabela | 4.814 | monitoramento |
| Mama | `vw_diamond_mod_birads_complemento` | view | — | **entrega à Mule** (33 colunas) |
| Mama | `vw_diamond_mod_birads_inconsistencias`, `_inconsistencias_total` | view | — | relatório de inconsistências por e-mail |
| Mama | `hive_metastore.ia.vw_enave_df_historico_birads` → `ia.enave_df_historico_birads_v202310` (782.897) + `ia.vw_enave_df_unidades` | view + tabela | — | **PACS**; tabela gravada em 14/10 por identidade de serviço — carga externa ao job |
| Nódulo | `hive_metastore.ia.tb_diamond_mod_pulmao` | tabela | 334.395 | entrada |
| Nódulo | `hive_metastore.ia.tb_diamond_mod_pulmao_saida` | tabela | 24.562 | saída |
| Nódulo | `hive_metastore.ia.tb_diamond_mod_pulmao_saida_salesforce` | tabela | 8.553 | **entrega ao Salesforce** (37 colunas); recriada a cada run (`CREATE OR REPLACE`) |
| Nódulo | `tb_diamond_mod_pulmao_saida_descripto`, `tb_diamond_mod_pulmao_metricas` | tabela | —, 219 | saída descriptografada; métricas |
| Nefro | `hive_metastore.ia.tb_diamond_creatinina_wrk_01` a `_07` | tabela | 26.915 cada | intermediárias recriadas a cada run |
| Nefro | `hive_metastore.ia.tb_diamond_nefrologia_clearance_saida` | tabela | 2.828.544 | saída (`MERGE` por `idExame`) |
| Nefro | `hive_metastore.ia.vw_gold_creatinina` | view | — | **entrega** (24 colunas), consumidor desconhecido |
| Nefro | `diamond_nefrologia.nefrologia.*` | tabelas | 2.562.058 na saída | versão do `fabrica-ia-plataforma`, **parada em 13/05** — não migra |
| Hepato | `diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_entrada_v2` | tabela | 3.174.822 | entrada |
| Hepato | `…_saida` | tabela | 2.903.674 | saída do motor |
| Hepato | `…_retorno_v2` | tabela | 5.836 | **retorno do board** |
| Hepato | `…_retorno_historico_v2` | tabela | 40.219 | histórico do retorno |
| Hepato | `…_navegacao_v2` | tabela | 3.355 | navegação enviada |
| Hepato | `vw_diamond_mod_hepatologia` | view | — | extração para o board (25 colunas) |
| Hepato | `vw_diamond_mod_hepatologia_analise_retorno_v2` | view | — | **base da navegação por fluxo** (35 colunas) |
| todas | `hive_metastore.ia.tb_diamond_mod_monitoramento` | tabela | — | monitoramento diário por `nm_projeto` |

Versões anteriores ainda presentes, sem escrita pelo código atual: hepato `…_entrada`, `…_retorno`,
`…_retorno_historico`, `…_navegacao` (sem `_v2`) e `tbl_gold_modelo_hepatologia_*`; mama
`tbl_gold_modelo_birads_*`.

---

## 7. Contratos de entrega — colunas que o destino recebe

| entrega | colunas |
|---|---|
| **Mama → Mule** (`vw_diamond_mod_birads_complemento`, 33) | `id_predicao`, médico encaminhador (id, CRM, UF, nome, telefone), paciente (id, **CPF**, nome, nascimento, sexo, telefone), convênio (ANS, nome, plano), unidade (id, regional, CNPJ, nome), `num_porta_entrada`, `num_motivo_entrada`, `cod_origem`, `id_exame`, `num_pedido_integracao`, `proced_nome_exame`, **`proced_laudo_exame`**, **`vl_proced_birads`**, `dt_exame`, `fl_integracao_concluida`, `dt_execucao`, inconsistência (motivo, flag, total) |
| **Nódulo → Salesforce** (`tb_diamond_mod_pulmao_saida_salesforce`, 37) | `record_id`, paciente (id, nome, **CPF**, sexo, nascimento, idade, DDD, telefone), unidade (lake e Salesforce), regional, convênio, `num_pedido_integracao`, exame (data, modalidade, título, tipo, AN, status, laudo id, liberação, **texto do laudo**), `exm_frase_selec`, **nódulo** (local, local detalhado, tamanho mm, detalhe do tamanho, tipo, descrição), `exm_agravantes`, `exm_class`, `exm_descritores_laudo`, `exm_justificativa_nlp`, `exm_encaminhamento_nlp`, `dataExecucaoModelo` |
| **Nefro** (`vw_gold_creatinina`, 24) | `idPredicao`, `idPatient`, `idExame`, `idMedico`, nascimento, idade, sexo, **telefone**, unidade (id, CNPJ, nome, regional), CRM e UF do solicitante, convênio, plano, nome do solicitante, `dataExameRecente`, TUSS (código, descrição), `descricaoProcedimento`, `valorExameCreatinina`, **`valorClearance`**, `dt_execucao` |
| **Hepato → board** (`vw_diamond_mod_hepatologia`, 25) | execução, predição, exame, unidade, médico (id, UF CRM), paciente (id, nascimento, idade, sexo), exame (data, nome, código e tipo do procedimento), convênio, laudo (original e tratado), **plaquetas** (`dt_coleta_plt`, `vl_plt`, `num_dif_tempo_imagem_plt` — transportadas, não usadas pelo algoritmo), `fl_relevante` |
| **Hepato → navegação** (`vw_diamond_mod_hepatologia_analise_retorno_v2`, 35) | identificação do exame e da unidade, paciente (**CPF**, nome, idade, sexo, telefone), médico (nome, CRM, UF), exame, convênio, laudo, `fl_relevante`, origem, modalidade, **retorno** (`id_retorno`, `ts_retorno`, `ts_atualizacao`, `observacoes`, `complexidade`), flags de fluxo (`fl_entrada`, `fl_saida`, `fl_enviado`, `fl_retorno`, `fl_transplante`, `fl_oncologia`, `fl_sem_doenca`, `fl_nao_navegavel`) |

---

## 8. Pendências de inventário

| item | por que não fechou |
|---|---|
| definição e dono do orquestrador | sem acesso |
| processo que carrega `ia.enave_df_historico_birads_v202310` (PACS) | identidade de serviço; fora dos jobs visíveis |
| consumidor de `vw_gold_creatinina` e de `tb_diamond_mod_pulmao_saida_salesforce` | sem acesso a `system.query` |
| contrato do lado da Mule e do Salesforce | externo ao Databricks |
