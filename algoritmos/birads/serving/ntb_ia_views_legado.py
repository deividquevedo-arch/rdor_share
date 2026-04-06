# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #Parâmetros

# COMMAND ----------

# {
#   "catalog": "diamond_birads",
#   "data_execucao_modelo": "2025-06-23",
#   "environment": "dev",
#   "id_projeto": "birads",
#   "schema": "birads",
#   "work_catalog": "diamond_birads",
#   "work_schema": "workarea"
# }

# COMMAND ----------

# DBTITLE 1,ID Projeto
dbutils.widgets.text("id_projeto", "birads", "ID Projeto")
id_projeto = dbutils.widgets.get("id_projeto")
print("id_projeto:", id_projeto)

# COMMAND ----------

# DBTITLE 1,Ambiente
dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")
print("environment:", environment)

# COMMAND ----------

# DBTITLE 1,Prefixo Tabelas
environment_tbl = "" if environment in ["hml", "prd"] else f"{environment}_"
print("environment_tbl:", environment_tbl)

# COMMAND ----------

# DBTITLE 1,Catalog
dbutils.widgets.text("catalog", "diamond_birads", "Catalog")
catalog_name = dbutils.widgets.get("catalog")
print(f"catalog_name: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Schema
dbutils.widgets.text("schema", "birads", "Schema")
schema_name = dbutils.widgets.get("schema")
print(f"schema_name: {schema_name}")

# COMMAND ----------

# DBTITLE 1,Work Catalog
dbutils.widgets.text("work_catalog", "diamond_birads", "Work Catalog")
work_catalog_name = dbutils.widgets.get("work_catalog")
print(f"work_catalog_name: {work_catalog_name}")

# COMMAND ----------

# DBTITLE 1,Work Schema
dbutils.widgets.text("work_schema", "workarea", "Work Schema")
work_schema_name = dbutils.widgets.get("work_schema")
print(f"work_schema_name: {work_schema_name}")

# COMMAND ----------

# DBTITLE 1,Data Execução
dbutils.widgets.text("data_execucao_modelo", "", "Data Execução Modelo")
data_execucao_modelo = dbutils.widgets.get("data_execucao_modelo")
if data_execucao_modelo == "":
    data_execucao_modelo = datetime.now().strftime("%Y-%m-%d")
print(f"Data Referencia: {data_execucao_modelo}")

# COMMAND ----------

# DBTITLE 1,Main Catalog
if environment in ["hml", "prd"]:
    main_catalog = catalog_name + ("" if schema_name == "" else f".{schema_name}")
else:
    main_catalog = work_catalog_name + ("" if work_schema_name == "" else f".{work_schema_name}")

print(f"main_catalog: {main_catalog}")

# COMMAND ----------

# DBTITLE 1,Work Catalog
work_catalog = work_catalog_name + ("" if work_schema_name == "" else f".{work_schema_name}")
print(f"work_catalog: {work_catalog}")

# COMMAND ----------

# DBTITLE 1,Pasta raiz onde os dados devem ser salvos
if schema_name == "":
    root_folder = f"/mnt/trusted/datalake/{main_catalog}/data/{id_projeto}/diamond"
else:
    root_folder = f"abfss://artificial-intelligence@sardslusdevelopmenthml.dfs.core.windows.net/curated/ia/diamond/{id_projeto}/{environment}"

root_folder_legacy = f"/mnt/trusted/datalake/ia/data/{id_projeto}/gold"

print(root_folder)
print(root_folder_legacy)

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções Auxiliares

# COMMAND ----------

# DBTITLE 1,optimize_table
def optimize_table(table_id):
    spark.sql(f"VACUUM {table_id}")
    spark.sql(f"OPTIMIZE {table_id}")
    spark.sql(f"ANALYZE TABLE {table_id} COMPUTE STATISTICS")

# COMMAND ----------

# DBTITLE 1,table_location
table_location = lambda x: f"{root_folder}/{x.split('.')[-1]}"

# COMMAND ----------

# DBTITLE 1,table_location_legacy
table_location_legacy = lambda x: f"{root_folder_legacy}/{x.split('.')[-1]}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas

# COMMAND ----------

tbl_modelo_birads_complemento_v2_lake2 = f"ia.{environment_tbl}tbl_gold_modelo_birads_complemento_v2_lake2"
vw_gold_birads_legado = f"ia.{environment_tbl}vw_gold_modelo_birads_complemento"
vw_diamond_mod_birads_complemento = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_complemento"

# COMMAND ----------

print(f"{'tbl_modelo_birads_complemento_v2_lake2':45}: {tbl_modelo_birads_complemento_v2_lake2}")
print(f"{'vw_gold_birads_legado':45}: {vw_gold_birads_legado}")
print(f"{'vw_diamond_mod_birads_complemento':45}: {vw_diamond_mod_birads_complemento}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Queries

# COMMAND ----------

spark.sql(f"""
  create or replace table {tbl_modelo_birads_complemento_v2_lake2}
  location '{table_location_legacy(tbl_modelo_birads_complemento_v2_lake2)}'
  partitioned by (dataExecucaoModelo)
  as
  select
    id_predicao as idPredicao,
    id_medico_encaminhador as idMedico,
    hive_metastore.default.rdsl_encrypt(doc_crm_medico_encaminhador, 0) as numCrm,
    uf_crm_medico_encaminhador as ufCrm,
    hive_metastore.default.rdsl_encrypt(nome_medico_encaminhador) as medicoEncaminhador,
    hive_metastore.default.rdsl_encrypt(tel_medico_encaminhador) as telefoneMedicoEncaminhador,
    id_paciente as idPaciente,
    hive_metastore.default.rdsl_encrypt(num_cpf_paciente) as cpfPaciente,
    hive_metastore.default.rdsl_encrypt(nome_paciente) as nomePaciente,
    dt_nascimento_paciente as dataNascimentoPaciente,
    gen_sexo_paciente as sexoPaciente,
    hive_metastore.default.rdsl_encrypt(tel_contato_paciente) as telefoneContato,
    cod_ans_convenio as codigoAnsConvenio,
    nome_convenio as nomeConvenio,
    nome_plano as nomePlano,
    id_unidade as idHospital,
    emp_regional_unidade as regional,
    emp_cnpj_unidade as cnpjHospital,
    emp_nome_unidade as nomeHospital,
    num_porta_entrada as portaEntrada,
    num_motivo_entrada as motivoEntrada,
    cod_origem as origem,
    id_exame as idExame,
    num_pedido_integracao as idAtendimento,
    proced_nome_exame as tituloExame,
    proced_laudo_exame as observacoes,
    vl_proced_birads as birads,
    dt_exame as dataExame,
    fl_integracao_concluida as flgIntegracaoConcluida,
    dt_execucao as dataExecucaoModelo,
    audit_motivo_inconsistencia_negocio as motivoInconsistenciaNegocio,
    fl_inconsistencia_negocio as flgInconsistenciaNegocio,
    total_inconsistencia_negocio as qtdInconsistenciaNegocio

  from {vw_diamond_mod_birads_complemento}

""").display()

# COMMAND ----------

optimize_table(tbl_modelo_birads_complemento_v2_lake2)

# COMMAND ----------

spark.sql(f"""
  create or replace view {vw_gold_birads_legado}
  as
  select
    idPredicao,
    idMedico,
    hive_metastore.default.unencrypt(numCrm, 0) as numCrm,
    ufCrm,
    hive_metastore.default.unencrypt(medicoEncaminhador) as medicoEncaminhador,
    hive_metastore.default.unencrypt(telefoneMedicoEncaminhador) as telefoneMedicoEncaminhador,
    idPaciente,
    hive_metastore.default.unencrypt(cpfPaciente) as cpfPaciente,
    hive_metastore.default.unencrypt(nomePaciente) as nomePaciente,
    dataNascimentoPaciente,
    sexoPaciente,
    hive_metastore.default.unencrypt(telefoneContato) as telefoneContato,
    codigoAnsConvenio,
    nomeConvenio,
    nomePlano,
    idHospital,
    regional,
    cnpjHospital,
    nomeHospital,
    portaEntrada,
    motivoEntrada,
    origem,
    idExame,
    idAtendimento,
    tituloExame,
    observacoes,
    birads,
    dataExame,
    flgIntegracaoConcluida,
    dataExecucaoModelo,
    motivoInconsistenciaNegocio,
    flgInconsistenciaNegocio,
    qtdInconsistenciaNegocio

  from {tbl_modelo_birads_complemento_v2_lake2}

""").display()

# COMMAND ----------

dbutils.notebook.exit(0)

# COMMAND ----------


