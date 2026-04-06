# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import requests
import time

from datetime import datetime
from requests.auth import HTTPBasicAuth

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

# DBTITLE 1,Origem
dbutils.widgets.text("origem", "api", "Origem")
origem = dbutils.widgets.get("origem")
print(f"Origem: {origem}")

# COMMAND ----------

params = {
    "id_projeto": id_projeto,
    "environment": environment,
    "catalog": catalog_name,
    "schema": schema_name,
    "work_catalog": work_catalog_name,
    "work_schema": work_schema_name,
    "data_execucao_modelo": data_execucao_modelo,
    "origem": origem,
}
params

# COMMAND ----------

# DBTITLE 1,Define Url da API
if environment in ["hml", "prd"]:
    url = f'https://onco-navegadoras-exp-api-prod.br-s1.cloudhub.io/api/navegadoras/inicial?dataInicial={data_execucao_modelo}&dataFinal={data_execucao_modelo}&flagIntegracao=false'
else:
    url = f'https://onco-navegadoras-exp-api.br-s1.cloudhub.io/api/navegadoras/inicial?dataInicial={data_execucao_modelo}&dataFinal={data_execucao_modelo}&flagIntegracao=false'

print(url)

# COMMAND ----------

# DBTITLE 1,send_data
def send_data(url, username, password):
    response = requests.get(url, auth=HTTPBasicAuth(username, password))

    print(f"Efetuando chamada para a url: {url}")

    if response.status_code >= 300:
        print(f"Falha na chamada para a API. Status code: {response.status_code}")
        return False

    print("Chamada para a API executada com sucesso!")
    return True

# COMMAND ----------

# DBTITLE 1,Aguarda envio dos dados
username = "a61cec08000742cfb96f2e1871927d55"
password = dbutils.secrets.get(scope="dbricks-keyvault-secrets", key="api-mule-birads-portalacolhimento-prd-secret")
status_api = False
status_log = False
tentativas = 0
start_time = time.time()
timeout = 4 * 60 * 60  # 4 hours in seconds
time_sleep = 60 # Segundos
notebook_timeout = 0 # (0 means no timeout) 

while (status_api == False or status_log == False) and (time.time() - start_time < timeout):
    tentativas += 1
    print("-"*80)
    print(f"Iniciando envio dos dados. Tentativa: {tentativas}")

    status_api = send_data(url, username, password)
    
    if status_api:
        try:
            dbutils.notebook.run("../monitoring/ntb_ia_analise_logs", notebook_timeout, params)
            status_log = True
        except Exception as e:
            print(f"Ocorreu um erro ao executar o notebook de análise de logs.")
            status_log = False
    else:
        print("Aguardando 1 minuto...")
        time.sleep(time_sleep)

if time.time() - start_time >= timeout and (status_api == False or status_log == False):
    print("Timeout de 4 horas atingido. Encerrando o loop.")

# COMMAND ----------

print(f"status_api: {status_api}")
print(f"status_log: {status_log}")

# COMMAND ----------

# DBTITLE 1,Envia notificações
if status_api and status_log:
    ntb = "../monitoring/ntb_ia_analise_logs_sucesso"
else:
    ntb = "../monitoring/ntb_ia_analise_logs_erro"

dbutils.notebook.run(ntb, notebook_timeout, params)

# COMMAND ----------

# DBTITLE 1,Executa notebooks de monitoramento
if status_api and status_log:
    dbutils.notebook.run("../monitoring/ntb_ia_birads_monit_diario", notebook_timeout, params)
    dbutils.notebook.run("../monitoring/ntb_ia_analise_logs_notificacoes_diarias", notebook_timeout, params)

# COMMAND ----------

dbutils.notebook.exit("Fim do processamento!")

# COMMAND ----------


