# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import json
import requests
import pyspark
from datetime import datetime, timedelta
from pyspark.sql import functions as F

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

# MAGIC %md
# MAGIC # Parâmetros

# COMMAND ----------

# DBTITLE 1,Ambiente
# dbutils.widgets.text("environment", "dev", "Environment")
# environment = dbutils.widgets.get("environment")
# print("Environment:", environment)

# COMMAND ----------

# DBTITLE 1,Prefixo Tabelas
# environment_tbl = "" if environment in ["hml", "prd"] else f"{environment}_"
# print("environment_tbl:", environment_tbl)

# COMMAND ----------

# DBTITLE 1,Catálogo
# dbutils.widgets.text("catalog", "ia", "Catalogo")
# catalog_name = dbutils.widgets.get("catalog")
# print("catalog_name:", catalog_name)

# COMMAND ----------

# DBTITLE 1,Data Execução Modelo
# dbutils.widgets.text("data_execucao_modelo", "", "Data Execução Modelo")
# data_execucao_modelo = dbutils.widgets.get("data_execucao_modelo")
# if data_execucao_modelo == "":
#     data_execucao_modelo = datetime.now().strftime("%Y-%m-%d")
# print(f"Data Execução Modelo: {data_execucao_modelo}")

# COMMAND ----------

# DBTITLE 1,Origem
dbutils.widgets.text("origem", "", "Origem")
origem = dbutils.widgets.get("origem")
print(f"Origem: {origem}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas

# COMMAND ----------

tb_diamond_mod_monitoramento = f"{main_catalog}.{environment_tbl}tb_diamond_mod_monitoramento_enfermeira_navegadora"

# COMMAND ----------

print(f"{'tb_diamond_mod_monitoramento':45}: {tb_diamond_mod_monitoramento}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# DBTITLE 1,get_data
def get_data(data_execucao: str):
    df = spark.sql(f"""
        SELECT *
        FROM {tb_diamond_mod_monitoramento}
        WHERE dt_apontamento = '{data_execucao}'
    """)
 
    return df

# COMMAND ----------

# DBTITLE 1,build_error_message
def build_error_message(
    df: pyspark.sql.dataframe.DataFrame,
    config: dict,
    content: dict
):
    facts = []

    body = content["body"]
    error_header = "Falhas ocorridas:"

    if df.rdd.isEmpty():
        error_header = "Não ocorreram falhas durante o envio dos dados"

    body.append({
            "type": "TextBlock",
            "weight": "Bolder",
            "text": error_header,
            "wrap": True,
            "style": "heading",
    })

    for row in df.collect():
        facts.append({"title": "Data e Hora", "value": row["data_hora"]})
        facts.append({"title": "Título", "value": row["idExame"]})
        facts.append({"title": "Código do Erro", "value": row["status_integracao"]})
        facts.append({"title": "Descrição do Erro", "value": row["mensagem"]})


    if len(facts):
        body.append({
            "type": "Container",
            "spacing": "Padding",
            "items": [
                {
                    "type": "FactSet",
                    "spacing": "Large",
                    "facts": facts
                }
            ],
        })

    content["body"] = body

    return content

# COMMAND ----------

def build_chat_message(
    *,
    data_execucao_modelo: str,
    df: pyspark.sql.dataframe.DataFrame,
    config: dict,
):
    facts = []

    for row in df.collect():
        facts.append({"title": "Unidade Referência", "value": row["emp_nome_unidade"]})
        facts.append({"title": "Data Referência", "value": row["dt_apontamento"].strftime("%Y-%m-%d")})
        facts.append({"title": "Bi-rads Referência", "value": row["vl_proced_birads"]})
        facts.append({"title": "Envios", "value": row["cnt_envio_diario"]})
        facts.append({"title": "Envios médio", "value": row["num_media_acumulada"]})
        facts.append({"title": "Envios fora da média", "value": row["num_variacao_absoluta"]})
        facts.append({"title": "Variação percentual", "value": row["num_variacao_percentual"]})
        facts.append({"title": "Sinalização", "value": row["cod_sinalizacao"]})
        facts.append({"title": "Origem", "value": row["cod_origem"]})
        facts.append({"title": "===============", "value": "==============="})

    content = {
        "type": "AdaptiveCard",
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {
                        "type": "Column",
                        "items": [
                            {
                                "type": "TextBlock",
                                "weight": "Bolder",
                                "text": f"Envio monitoramento Birads ({config['environment']})",
                                "wrap": True,
                            },
                            {
                                "type": "TextBlock",
                                "spacing": "None",
                                "text": f"Envios {row['cod_origem'].upper()}",
                                "isSubtle": True,
                                "wrap": True,
                            },
                        ],
                        "width": "stretch",
                    }
                ],
            },
            {"type": "TextBlock", "text": f"Data de Execução: {data_execucao_modelo}", "wrap": True},
            {"type": "FactSet", "facts": facts},
        ],
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
    }

    return content

# COMMAND ----------

# DBTITLE 1,send_chat_message
def send_chat_message(url: str, content: dict):
    message = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": content
        }],
    }

    response = requests.post(url, data=json.dumps(message), headers={"Content-Type": "application/json"})

    if response.status_code >= 300:
        print(f"Failed to send message. Status code: {response.status_code}")
        return False

    print("Message sent successfully!")
    return True

# COMMAND ----------

df = get_data(data_execucao_modelo)

df = df.withColumn(
    "priority",
    F.when(F.col("emp_nome_unidade") == "EMAIL", 0).otherwise(1)
)

df = df.withColumn(
    "order_custom",
    F.when(F.col("cod_sinalizacao") == "Aumento", 1)
     .when(F.col("cod_sinalizacao") == "Queda", 2)
     .when(F.col("cod_sinalizacao") == "Inatividade", 3)
     .otherwise(4)  # Caso tenha outras categorias
)

df = df.orderBy("priority", "order_custom").drop("priority", "order_custom")

df.display()

# COMMAND ----------

notification_config = f"/mnt/trusted/datalake/ia/projetos/monitoramento-enf-naveg/config/notificacoes.json"
 
df_config = spark.read.option("multiline","true").json(notification_config).where(f"environment == '{environment}'")
df_config.display()
 
config = df_config.collect()[0]

# COMMAND ----------

icon = "https://freesvg.org/img/dholler-ok.png"
content = build_chat_message(
    data_execucao_modelo=data_execucao_modelo,
    df=df,
    config=config)
send_chat_message(config["chatUrl"], content)

# COMMAND ----------

