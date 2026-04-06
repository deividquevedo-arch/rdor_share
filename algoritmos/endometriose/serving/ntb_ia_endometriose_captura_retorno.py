# Databricks notebook source
# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

import requests
import json
import time

from dataclasses import dataclass, asdict

# COMMAND ----------

# MAGIC %md
# MAGIC # Parâmetros do notebook

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Ambiente
dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")
print("Environment:", environment)

# COMMAND ----------

# DBTITLE 1,ID Projeto
dbutils.widgets.text("id_projeto", "endometriose", "ID Projeto")
id_projeto = dbutils.widgets.get("id_projeto")
print("id_projeto:", id_projeto)

# COMMAND ----------

if id_projeto == "":
  raise Exception("ID Projeto não informado.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataclass Info

# COMMAND ----------

@dataclass
class Info:
    environment: str = ""
    unidade: str = ""
    emailTo: str = ""
    emailCc: str = ""
    emailBcc: str = ""
    unidades: str = ""

    onedriveUser: str = ""
    logicAppUrl: str = ""
    waitTimeInSeconds: int = 60

    sourcePath: str = ""
    targetPath: str = ""

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

def load_unidades(file_path):
    info_list = []

    df = spark.read.option("multiline", "true").json(file_path)

    for row in df.collect():
        info_list.append(
            Info(
                environment=environment,
                unidade=row["unidade"],
                emailTo=row["emailTo"],
                emailCc=row["emailCc"],
                emailBcc=row["emailBcc"],
                unidades=row["unidades"],
            )
        )

    return info_list

# COMMAND ----------

# DBTITLE 1,load_config
def load_config(file_path, info_list):
    df = spark.read.option("multiline", "true").json(file_path)

    for row in df.collect():
        for info in info_list:
            info.onedriveUser = row["onedriveUser"]
            info.logicAppUrl = row["logicAppUrl"]
            info.waitTimeInSeconds = row["waitTimeInSeconds"]
            info.sourcePath = f"/Projetos/{id_projeto}/Shared/Envio/"
            info.targetPath = f"trusted/datalake/ia/projetos/{id_projeto}/data/{environment}/retorno/"
        break

    return info_list

# COMMAND ----------

# DBTITLE 1,send
def get_files(info_list):

    for info in info_list:
        print("-" * 120)
        print(f"Capturando arquivos: {info.unidade}")
        payload = json.dumps(asdict(info))

        response = requests.post(
            info.logicAppUrl, data=payload, headers={"Content-Type": "application/json"}
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga das configurações

# COMMAND ----------

# DBTITLE 1,Carrega configurações das Unidades
unidades_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/unidades.json"
info_list = load_unidades(unidades_path)
print(info_list)

# COMMAND ----------

# DBTITLE 1,Carrega as configurações de Captura
config_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/config_captura.json"
info_list = load_config(config_path, info_list)
print(info_list)

# COMMAND ----------

# MAGIC %md
# MAGIC # Captura arquivos

# COMMAND ----------

# DBTITLE 1,Captura os arquivos
get_files(info_list)

# COMMAND ----------

# DBTITLE 1,Aguarda a carga dos arquivos, de acordo com o tempo configurado
if len(info_list):
  info = info_list[0]
  print(f"Aguardando carga dos arquivos por {info.waitTimeInSeconds} segundos")
  time.sleep(info.waitTimeInSeconds)

# COMMAND ----------


