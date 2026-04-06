# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import json
import requests
import pyspark

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

# MAGIC %md
# MAGIC # Parâmetros do notebook

# COMMAND ----------

# DBTITLE 1,Ambiente
# dbutils.widgets.text("environment", "dev", "Environment")
# environment = dbutils.widgets.get("environment")
# print(f"environment: {environment}")

# COMMAND ----------

# DBTITLE 1,Prefixo Tabelas
# environment_tbl = "" if environment in ["hml", "prd"] else f"{environment}_"
# print("environment_tbl:", environment_tbl)

# COMMAND ----------

# DBTITLE 1,Catálogo
# dbutils.widgets.text("catalog", "ia", "Catalogo")
# catalog_name = dbutils.widgets.get("catalog")
# print(f"catalog_name: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Data Execução Modelo
# dbutils.widgets.text("data_execucao_modelo", "", "Data Execução Modelo")
# data_execucao_modelo = dbutils.widgets.get("data_execucao_modelo")
# if data_execucao_modelo == "":
#     data_execucao_modelo = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,ID Projeto
# dbutils.widgets.text("id_projeto", "api_birads", "ID Projeto")
# id_projeto = dbutils.widgets.get("id_projeto")
# print("id_projeto:", id_projeto)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas

# COMMAND ----------

vw_diamond_mod_birads_complemento = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_complemento"

# COMMAND ----------

print(f"{'vw_diamond_mod_birads_complemento':45}: {vw_diamond_mod_birads_complemento}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# DBTITLE 1,get_data
def get_data(data_execucao: str):
    df = spark.sql(f"""
        with cte_processado as (
            select
                dt_execucao,
                count(1) as total,
                count_if(vl_proced_birads = 4) as birads_4,
                count_if(vl_proced_birads = 5) as birads_5,
                0 as enviados
            from {vw_diamond_mod_birads_complemento}
            where dt_execucao = '{data_execucao}'
            group by all
        )
        ,cte_enviado as (
            select
                comp.dt_execucao,
                count(logs.idExame) as enviado,
                count(distinct logs.idExame) as distinto
            from {vw_diamond_mod_birads_complemento} as comp
            inner join logs.api_birads as logs
                on comp.id_exame = logs.idExame
            where comp.dt_execucao = '{data_execucao}'
            group by all
        )
        ,cte_union as (
            select
                dt_execucao,
                total,
                birads_4,
                birads_5,
                0 as enviado,
                0 as distinto
            from cte_processado

            union all
            
            select
                dt_execucao,
                0 as total,
                0 as birads_4,
                0 as birads_5,
                enviado,
                distinto
            from cte_enviado
        )
        select
            dt_execucao,
            sum(total) as total,
            sum(birads_4) as birads_4,
            sum(birads_5) as birads_5,
            sum(enviado) as enviado,
            sum(distinto) as distinto
        from cte_union
        group by all
    """)

    return df


# COMMAND ----------

# DBTITLE 1,get_errors
def get_errors(data_log: str):
    df = spark.sql(f"""
        select
            idExame,
            date_format(max(data_hora), 'dd/MM/yyyy HH:mm:ss') as data_hora,
            status_integracao,
            mensagem
        from logs.api_birads
        where true
        and date(data_hora) = '{data_log}'
          and status_integracao != '200'
        group by all
        order by data_hora desc
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

    # if df.rdd.isEmpty():
    if df.count() == 0:
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

# DBTITLE 1,build_chat_message
def build_chat_message(
    *,
    environment: str,
    data_execucao_modelo: str,
    tentativas: str,
    df: pyspark.sql.dataframe.DataFrame,
    df_errors: pyspark.sql.dataframe.DataFrame,
    config: dict,
    icon: str,
    success: bool
):
    facts = []

    if not environment.upper() in ["HML"]:
       facts.append({"title": "Ambiente", "value": environment.upper()})
    facts.append({"title": "Data Execução", "value": data_execucao_modelo})
    facts.append({"title": "Tentativas de envio", "value": tentativas})

    for row in df.collect():
        facts.append({"title": "Laudos Processados", "value": row["total"]})
        facts.append({"title": "Laudos Enviados", "value": row["enviado"]})
        facts.append({"title": "Laudos Distintos", "value": row["distinto"]})
        facts.append({"title": "Bi-rads 4", "value": row["birads_4"]})
        facts.append({"title": "Bi-rads 5", "value": row["birads_5"]})

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
                                "type": "Image",
                                "url": icon,
                                "size": "Small",
                            }
                        ],
                        "width": "auto",
                    },
                    {
                        "type": "Column",
                        "items": [
                            {
                                "type": "TextBlock",
                                "weight": "Bolder",
                                "text": config["successTitle"] if success else config["errorTitle"],
                                "wrap": True,
                                "style": "heading",
                            }
                        ],
                        "width": "stretch",
                        "verticalContentAlignment": "Center",
                    },
                ],
            },
            {
                "type": "Container",
                "spacing": "Padding",
                "items": [
                    {
                        "type": "FactSet",
                        "spacing": "Large",
                        "facts": facts
                    }
                ],
            },
        ],
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.6",
    }

    content = build_error_message(df_errors, config, content)

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

# DBTITLE 1,build_email
def build_email(
    environment: str,
    data_execucao_modelo: str,
    tentativas: str,
    df: pyspark.sql.dataframe.DataFrame,
    df_errors: pyspark.sql.dataframe.DataFrame,
    config: dict
):
    facts = []

    email_body = ""
    if not environment.upper() in ["HML"]:
        email_body = f"<p><b>Ambiente:</b> {environment}</p>"
    email_body += f"<p><b>Data Execução:</b> {data_execucao_modelo}</p>"
    email_body += f"<p><b>Tentativas de Envio:</b> {tentativas}</p>"

    for row in df.collect():
        email_body +=  f"<p><b>Laudos Processados</b>: {row['total']}"
        email_body +=  f"<p><b>Laudos Enviados</b>: {row['enviado']}"
        email_body +=  f"<p><b>Laudos Distintos</b>: {row['distinto']}"
        email_body +=  f"<p><b>Bi-rads 4</b>: {row['birads_4']}"
        email_body +=  f"<p><b>Bi-rads 5</b>: {row['birads_5']}"

    for row in df_errors.collect():
        email_body += f"<hr>"
        email_body += f"<p><b>Data e Hora:</b> {row['data_hora']}</p>"
        email_body += f"<p><b>Título:</b> {row['idExame']}</p>"
        email_body += f"<p><b>Código do Erro:</b> {row['status_integracao']}</p>" 
        email_body += f"<p><b>Descrição do Erro:</b> {row['mensagem']}</p>"

    payload = {
        "emailTo": config["emailRecipients"]["emailTo"],
        "emailCc": config["emailRecipients"]["emailCc"],
        "emailBcc": config["emailRecipients"]["emailBcc"],
        "emailSubject": f"[{environment.upper()}] {config['errorTitle']}",
        "emailBody": email_body
    }

    return payload

# COMMAND ----------

# DBTITLE 1,send_email
def send_email(url: str, payload: dict):
    response = requests.post(url, data=json.dumps(payload), headers={"Content-Type": "application/json"})

    if response.status_code >= 300:
        print(f"Failed to send email. Status code: {response.status_code}")
        return False

    print("Email sent successfully!")
    return True
