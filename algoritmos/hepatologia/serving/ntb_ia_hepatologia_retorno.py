# Databricks notebook source
# MAGIC %md
# MAGIC # Instalação de libs

# COMMAND ----------

!pip install -q openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import json
import pandas as pd
import numpy as np
import requests
import time
import warnings

from datetime import date, datetime
from pyspark import pandas as ps
from pyspark.sql.functions import current_timestamp, lit, expr, col, when

# COMMAND ----------

warnings.simplefilter(action='ignore', category=FutureWarning)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parâmetros

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,ID Projeto
dbutils.widgets.text("id_projeto", "hepatologia", "ID Projeto")
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
dbutils.widgets.text("catalog", "diamond_hepatologia", "Catalog")
catalog_name = dbutils.widgets.get("catalog")
print(f"catalog_name: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Schema
dbutils.widgets.text("schema", "hepatologia", "Schema")
schema_name = dbutils.widgets.get("schema")
print(f"schema_name: {schema_name}")

# COMMAND ----------

# DBTITLE 1,Work Catalog
dbutils.widgets.text("work_catalog", "diamond_hepatologia", "Work Catalog")
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

# DBTITLE 1,root_path
# Não alterar
root_path = f"trusted/datalake/ia/projetos/{id_projeto}/data/"
print(root_path)

# COMMAND ----------

# DBTITLE 1,path_origem
path_origem = "/Projetos/Hepatologia/Shared/Envio/"

# COMMAND ----------

# DBTITLE 1,path_destino
path_destino = f"{root_path}{environment}/retorno/"
print(path_destino)

# COMMAND ----------

# DBTITLE 1,path
path = f"/mnt/{path_destino}"
print(path)

# COMMAND ----------

sleep_time_in_seconds = 300 if environment in ["hml", "prd"] else 30
print(f"sleep_time_in_seconds: {sleep_time_in_seconds}")

# COMMAND ----------

# DBTITLE 1,TODO: Define URL da LogicApp
if environment not in ["prd"]:
    logic_app_url = 'https://prod-05.eastus2.logic.azure.com:443/workflows/bcf62b0f15a7487893ecfa28ce116fde/triggers/Vari%C3%A1veis/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FVari%C3%A1veis%2Frun&sv=1.0&sig=KKipz3uYw_RtV7TgbEd4JcIv9BrMpISFhvmm6K2Pelg'
else:
    # TODO: Definir URL de PRD
    logic_app_url = ""

print(logic_app_url)

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

# MAGIC %md
# MAGIC # Variáveis com os nomes das tabelas

# COMMAND ----------

# DBTITLE 1,Define variáveis com nomes das tabelas
tbl_work = f"{work_catalog}.{environment_tbl}tb_diamond_wrk_hepatologia_retorno"
tbl_retorno = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_retorno"
tbl_retorno_hist = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_retorno_historico"

# COMMAND ----------

# DBTITLE 1,Exibe valores das variáveis com nomes das tabelas
print(f"{'tbl_work':20}: {tbl_work}")
print(f"{'tbl_retorno':20}: {tbl_retorno}")
print(f"{'tbl_retorno_hist':20}: {tbl_retorno_hist}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Importa arquivos do OneDrive

# COMMAND ----------

# DBTITLE 1,get_files
def get_files(df_unidades, url, env, path_origem, path_destino):
    for unidade in df_unidades.collect():
        _unid = unidade["unidade"]
        
        print("-" * 120)
        print(f"Recuperando arquivos da unidade: {_unid}")

        payload = json.dumps({
            "sourcePath": path_origem,
            "targetPath": path_destino,
            "environment": env,
            "unidade": _unid
        })

        response = requests.post(
            url, data=payload, headers={"Content-Type": "application/json"}
        )

        time.sleep(15)

# COMMAND ----------

# DBTITLE 1,Carrega configurações das Unidades
unidades_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/unidades.json"
df_unidades = spark.read.option("multiline", "true").json(unidades_path)
df_unidades.display()

# COMMAND ----------

print(path_origem)
print(path_destino)
print(sleep_time_in_seconds)

# COMMAND ----------

get_files(df_unidades, logic_app_url, environment, path_origem, path_destino)

print("-" * 120)
print(f"Aguardando {sleep_time_in_seconds} segundos") 
time.sleep(sleep_time_in_seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega dados de retorno

# COMMAND ----------

de_para_cols = {
    'Data Referência': "dt_execucao",
    'idPredicao': "id_predicao",
    'idExame': "id_exame",
    'Nome Paciente': "nome_paciente",
    'Idade Paciente': "num_idade_paciente",
    'Nome Hospital': "emp_nome_unidade",
    'UF Hospital': "emp_regional_unidade",
    'Médico Solicitante': "nome_medico",
    'CRM': "doc_crm_medico",
    'UF CRM': "uf_crm_medico",
    'Data Exame': "dt_exame",
    'Tipo Exame': "proced_nome_exame",
    'Laudo': "proced_laudo_exame",
    'Valor PLT': "vl_plt",
    'Dif. Tempo Imagem PLT': "num_dif_tempo_imagem_plt",
    'Achado Relevante': "cod_achado_relevante",
    'Achado': "nome_achado",
    'Linha de Cuidado': "cod_linha_cuidado",
    'Destino': "cod_destino",
    'Prioridade': "cod_prioridade",
    'Observação':  "obs_achado",
}

# COMMAND ----------

# DBTITLE 1,Define paths de sucesso e erro
today = date.today().strftime("%Y-%m-%d")

path_success = path.replace("retorno", "sucesso")
path_error = path.replace("retorno", "erro") + f"{today}/"

# COMMAND ----------

print(f"path        : {path}")
print(f"path_success: {path_success}")
print(f"path_error  : {path_error}")

# COMMAND ----------

# DBTITLE 1,Cria uma lista com os arquivo que serão carregados
files = [file.path for file in dbutils.fs.ls(path)]
files_success = []
files_error = []

for f in files:
    print(f)

# COMMAND ----------

# DBTITLE 1,Exclui a tabela work
spark.sql(f"drop table if exists {tbl_work}").display()

# COMMAND ----------

# DBTITLE 1,Verifica se existem arquivos para serem carregados
if len(files) == 0:
    dbutils.notebook.exit("Não há arquivos para serem processados!")

# COMMAND ----------

# DBTITLE 1,Faz um loop pelos arquivos e vai inserindo na tabela work
for file in files:
    try:
        print("-"*120)
        print(f"Carregando arquivo: {file}\n")

        df_excel = ps.read_excel(file)
        cols = [de_para_cols.get(col) for col in df_excel.columns]
        df_excel.columns = cols

        # Adiciona colunas que não existem no df_excel
        selection_cols = []
        for v in de_para_cols.values():
            selection_cols.append(v)
            if v not in cols: 
                df_excel[v] = None

        # Seleciona as colunas na ordem definida na várivel de_para_cols
        df_excel = df_excel[selection_cols]
        df_excel = df_excel.astype(str)

        df = df_excel.to_spark()

        df = (
            df
            .withColumn("id_retorno", expr("uuid()"))
            .withColumn("ts_retorno", current_timestamp())
            .withColumn("ts_atualizacao", current_timestamp())
            .withColumn("cod_arquivo_retorno", lit(file.split("/")[-1]))
        )

        df = (df
              .select([when(col(c) == "nan", None).otherwise(col(c)).alias(c) for c in df.columns])
              .select([when(col(c) == "None", None).otherwise(col(c)).alias(c) for c in df.columns])
        )

        df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{tbl_work}")

        print(f"Arquivo: {file}")
        print(f"Carregado para a tabela: ia.{tbl_work}")            

        files_success.append(file)

    except Exception as error:
        print(f"ERRO ao carregar arquivo: {file}")
        print(error)
        files_error.append(file)

# COMMAND ----------

print(f"Quantidade de arquivos carregados com sucesso: {len(files_success)}")
print(f"Quantidade de arquivos carregados com erro: {len(files_error)}")

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Lê os registros da tabela work e insere na tabela de retorno final
spark.sql(f"""
    insert into {tbl_retorno}
    (
        id_retorno,
        dt_execucao,
        id_predicao,
        id_exame,
        ts_retorno,
        ts_atualizacao,
        cod_arquivo_retorno,
        cod_achado_relevante,
        nome_achado,
        cod_linha_cuidado,
        obs_achado,
        cod_destino,
        cod_prioridade
    )
    select
        id_retorno,
        date(dt_execucao) as dt_execucao,
        id_predicao,
        id_exame,
        ts_retorno,
        ts_atualizacao,
        cod_arquivo_retorno,
        cod_achado_relevante,
        nome_achado,
        cod_linha_cuidado,
        obs_achado,
        cod_destino,
        cod_prioridade
    from {tbl_work}

    where (cod_achado_relevante is not null
         or nome_achado is not null
         or cod_linha_cuidado is not null
         or obs_achado is not null
         or cod_destino is not null
         or cod_prioridade is not null
    )
""").display()


# COMMAND ----------

# DBTITLE 1,Otimiza a tabela de retorno
optimize_table(tbl_retorno)

# COMMAND ----------

# DBTITLE 1,Exclui a tabela work
spark.sql(f"drop table if exists {tbl_work}").display()

# COMMAND ----------

# DBTITLE 1,Grava os registros duplicados em uma tabela histórica
df_hist = spark.sql(f"""
    select * from {tbl_retorno}
    qualify row_number() over (partition by id_predicao order by ts_retorno desc) > 1
""")

(
    df_hist
    .write
    .mode("append")
    .option("path", table_location(tbl_retorno_hist))
    .option("mergeSchema", "true")
    .saveAsTable(tbl_retorno_hist)
)

# COMMAND ----------

# DBTITLE 1,Otimiza a tabela de retorno histórica
optimize_table(tbl_retorno_hist)

# COMMAND ----------

# DBTITLE 1,Remove da tabela de retorno os registros duplicados e mantém somente a última versão
query = f"""
    select
        id_predicao,
        ts_retorno
    from {tbl_retorno} as hist
    qualify row_number() over (partition by id_predicao order by ts_retorno desc) > 1
"""

merge_query = f"""
    MERGE INTO {tbl_retorno} AS t
    USING ({query}) AS s
        ON  t.id_predicao = s.id_predicao
        AND t.ts_retorno = s.ts_retorno
    WHEN MATCHED THEN 
        DELETE
"""

spark.sql(merge_query).display()

# COMMAND ----------

# DBTITLE 1,Função move_files
def move_files(files, target_path):
    for file in files:
        name = file.split("/")[-1]
        target_file = target_path + name

        print("-"*120)
        print(f"Movendo arquivo: {file}")
        print(f"Destino: {target_file}")

        dbutils.fs.mv(file, target_file)

# COMMAND ----------

# DBTITLE 1,Move arquios carregados com sucesso
move_files(files_success, path_success)

# COMMAND ----------

# DBTITLE 1,Move arquivos que deram erro
move_files(files_error, path_error)

# COMMAND ----------

dbutils.notebook.exit("Fim da execução!!!")

# COMMAND ----------

spark.sql(f"select * from {tbl_retorno} where date(ts_retorno) = current_date()").display()

# COMMAND ----------

spark.sql(f"select * from  {tbl_retorno_hist}").display()

# COMMAND ----------


