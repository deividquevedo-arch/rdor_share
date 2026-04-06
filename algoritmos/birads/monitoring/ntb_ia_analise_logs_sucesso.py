# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Parâmetros

# COMMAND ----------

# dbutils.widgets.removeAll()

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

print(root_folder)

# COMMAND ----------

# DBTITLE 1,Tentativas
dbutils.widgets.text("tentativas", "1", "Tentativas")
tentativas = dbutils.widgets.get("tentativas")
print("tentativas:", tentativas)

# COMMAND ----------

# DBTITLE 1,Define o caminho principal da pasta remota
root_remote_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/"
print(root_remote_path)

# COMMAND ----------

# DBTITLE 1,Define o caminho principal da pasta remota de configs
root_remote_config_path = f"{root_remote_path}config/{environment}/"
print(root_remote_config_path)

# COMMAND ----------

# DBTITLE 1,Exibe variáveis
def print_params():
    print(f"{'id_projeto':25}: {id_projeto}")
    print(f"{'environment':25}: {environment}")
    print(f"{'environment_tbl':25}: {environment_tbl}")
    print(f"{'catalog_name':25}: {catalog_name}")
    print(f"{'schema_name':25}: {schema_name}")
    print(f"{'work_catalog_name':25}: {work_catalog_name}")
    print(f"{'work_schema_name':25}: {work_schema_name}")
    print(f"{'data_execucao_modelo':25}: {data_execucao_modelo}")
    print(f"{'main_catalog':25}: {main_catalog}")
    print(f"{'work_catalog':25}: {work_catalog}")
    print(f"{'root_folder':25}: {root_folder}")
    print(f"{'tentativas':25}: {tentativas}")
    print(f"{'root_remote_path':25}: {root_remote_path}")
    print(f"{'root_remote_config_path':25}: {root_remote_config_path}")

print_params()

# COMMAND ----------

# DBTITLE 1,Importa Funções de Notificação
# MAGIC %run ./ntb_ia_analise_logs_notificacoes

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega Configurações

# COMMAND ----------

notification_config = f"{root_remote_config_path}notificacoes.json"

df_config = spark.read.option("multiline","true").json(notification_config)
df_config.display()

# COMMAND ----------

config = df_config.collect()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega dados

# COMMAND ----------

df = get_data(data_execucao_modelo)
df.display()

# COMMAND ----------

df_errors = get_errors(data_execucao_modelo)
df_errors.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Envia notificações

# COMMAND ----------

icon = "https://freesvg.org/img/dholler-ok.png"
content = build_chat_message(
    environment=environment,
    data_execucao_modelo=data_execucao_modelo,
    tentativas=tentativas,
    df=df,
    df_errors=df_errors,
    config=config,
    icon=icon,
    success=True)
send_chat_message(config["chatUrl"], content)

# COMMAND ----------

