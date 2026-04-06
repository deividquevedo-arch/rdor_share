# Databricks notebook source
# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

import requests
import json

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

from dataclasses import dataclass, asdict

@dataclass
class Info:
    fileName: str = ""
    sourcePath: str = ""
    targetPath: str = ""
    environment: str = ""
    unidade: str = ""
    onedriveUser: str = ""
    logicAppUrl: str = ""
    emailToDefault: str = ""
    emailTo: str = ""
    emailCc: str = ""
    emailBcc: str = ""
    emailSubject: str = ""
    emailBody: str = ""
    registros: str = ""
    dataProcessamento: str = ""
    dataProcessamentoFormatada: str = ""

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# DBTITLE 1,load_metadata
def load_metadata(file_path):
    info_list = []

    df = spark.read.option("multiline", "true").json(file_path)

    for row in df.collect():
        info_list.append(
            Info(
                environment=environment,
                fileName=row["nomeArquivo"],
                sourcePath=row["caminhoRemoto"],
                unidade=row["unidade"],
                registros=row["registros"],
                dataProcessamento=row["dataProcessamento"],
                dataProcessamentoFormatada=row["dataProcessamentoFormatada"],
            )
        )

    return info_list

# COMMAND ----------

# DBTITLE 1,load_config
def load_config(file_path, info_list):
    df = spark.read.option("multiline", "true").json(file_path)
    df.display()

    for row in df.collect():
        for info in info_list:
            info.targetPath = row["targetPath"]
            info.onedriveUser = row["onedriveUser"]
            info.logicAppUrl = row["logicAppUrl"]
            info.emailToDefault = row["emailToDefault"]

            _subject = "emailSubject" if info.registros > 0 else "emailSubjectNoRecords"
            _body = "emailBody" if info.registros > 0 else "emailBodyNoRecords"

            info.emailSubject = f"{'[' + environment.upper() + ']' if environment != 'prd' else ''}{row[_subject]}"
            info.emailBody = row[_body]

        break

    return info_list

# COMMAND ----------

# DBTITLE 1,load_unidades
def load_unidades(file_path, info_list):
    df = spark.read.option("multiline","true").json(file_path)

    for info in info_list:
        df_filtered = df.filter(df.unidade == info.unidade)

        if not df_filtered.rdd.isEmpty():
            row = df_filtered.collect()[0]

            info.emailTo = row['emailTo']
            info.emailCc = row['emailCc']
            info.emailBcc = row['emailBcc']
        else:
            info.emailTo = info.emailToDefault

    return info_list

# COMMAND ----------

# DBTITLE 1,send
def send(info_list):

    for info in info_list:
        print("-" * 120)
        print(f"Enviando: {info.unidade} - Registros: {info.registros}")
        payload = json.dumps(asdict(info))

        response = requests.post(
            info.logicAppUrl, data=payload, headers={"Content-Type": "application/json"}
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga das configurações

# COMMAND ----------

# DBTITLE 1,Carrega os metadados
file_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/data/{environment}/navegacao/{id_projeto}_metadados_navegacao.json"
info_list = load_metadata(file_path)
print(info_list)

# COMMAND ----------

# DBTITLE 1,Carrega as configurações de envio
config_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/config_envio.json"
info_list = load_config(config_path, info_list)
print(info_list)

# COMMAND ----------

# DBTITLE 1,Carrega configurações das Unidades
unidades_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/navegacao.json"
info_list = load_unidades(unidades_path, info_list)
print(info_list)

# COMMAND ----------

# MAGIC %md
# MAGIC # Envia arquivos

# COMMAND ----------

send(info_list)

# COMMAND ----------


