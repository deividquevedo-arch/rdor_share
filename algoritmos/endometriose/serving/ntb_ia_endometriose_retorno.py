# Databricks notebook source
# MAGIC %md
# MAGIC # Instalação de libs

# COMMAND ----------

!pip install -q openpyxl

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Parâmetros do notebook

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Ambiente
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.dropdown("send_message", "True", ["True", "False"], "Send Message")

environment = dbutils.widgets.get("environment")
send_message = dbutils.widgets.get("send_message")
send_message = True if send_message == "True" else False


print("Environment:", environment)
print("Send Message: ", send_message)

# COMMAND ----------

# DBTITLE 1,ID Projeto
dbutils.widgets.text("id_projeto", "endometriose", "ID Projeto")
id_projeto = dbutils.widgets.get("id_projeto")
print("id_projeto:", id_projeto)

# COMMAND ----------

# DBTITLE 1,Root path
# Não alterar
root_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/data/{environment}/"
root_path

# COMMAND ----------

# DBTITLE 1,Path retorno
path = f"{root_path}retorno/"
path

# COMMAND ----------

# DBTITLE 1,Define variável que será injetada no nome das tabelas
if environment in ["hml", "prd"]:
    environment_tbl = ""
else:
    environment_tbl = f"{environment}_"

print(f"environment_tbl: {environment_tbl}")

# COMMAND ----------

# DBTITLE 1,Define variáveis com nomes das tabelas
tbl_work = f"{environment_tbl}tbl_work_modelo_{id_projeto}_retorno"
tbl_retorno = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_retorno"
tbl_retorno_hist = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_retorno_historico"

# COMMAND ----------

print(f"tbl_work: {tbl_work}")
print(f"tbl_retorno: {tbl_retorno}")
print(f"tbl_retorno_hist: {tbl_retorno_hist}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

import numpy as np

from datetime import date

from pyspark import pandas as ps
from pyspark.sql.functions import current_timestamp, lit, expr

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega dados de retorno

# COMMAND ----------

# DBTITLE 1,Define tipos de dados das colunas que serão lidas dos arquivos de retorno
dtypes = {
    "dataExecucaoModelo": str,
    "idPredicao": str,
    "idExame": str,
    "nomePaciente": str,
    "cpfPaciente": str,
    "idadePaciente": str,
    "sexoPaciente": str,
    "telefoneContato": str,
    "unidade": str,
    "cnpjUnidade": str,
    "regionalUnidade": str,
    "crm": str,
    "ufCRM": str,
    "nomeMedico": str,
    "dataExame": str,
    "dataLiberacaoLaudo": str,
    "convenio": str,
    "codigoTUSS": str,
    "descricaoTUSS": str,
    "nomeProcedimento": str,
    "laudo": str,
    "classificacao": str,
    "achado": str,
    "observacoes": str,
    "destino": str
}

# COMMAND ----------

col_names = [col for col in dtypes.keys()]

# COMMAND ----------

# DBTITLE 1,Define paths de sucesso e erro
today = date.today().strftime("%Y-%m-%d")

path_success = path.replace("retorno", "retorno-sucesso")
path_error = path.replace("retorno", "retorno-erro") + f"{today}/"

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

tbl_work

# COMMAND ----------

# DBTITLE 1,Exclui a tabela work
spark.sql(f"drop table if exists ia.{tbl_work}").display()

# COMMAND ----------

# DBTITLE 1,Verifica se existem arquivos para serem carregados
if len(files) == 0:
    dbutils.notebook.exit("Não há arquivos para serem processados!")

# COMMAND ----------

# DBTITLE 1,Faz um loop pelos arquivos e vai inserindo na tabela work
for file in files:
    try:
        df_excel = ps.read_excel(file, skiprows=1, header=None, names=col_names, dtype=dtypes)
        df_excel = df_excel.astype(dtypes)

        df = df_excel.to_spark()
        df = (
            df
            .withColumn("idRetorno", expr("uuid()"))
            .withColumn("dataHoraRetorno", current_timestamp())
            .withColumn("dataHoraAtualizacao", current_timestamp())
            .withColumn("arquivoRetorno", lit(file.split("/")[-1]))
        )
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"ia.{tbl_work}")

        print("-"*120)
        print(f"Arquivo: {file}")
        print(f"Carregado para a tabela: ia.{tbl_work}")            

        files_success.append(file)

    except Exception as error:
        print("-"*120)
        print(f"ERRO ao carregar arquivo: {file}")
        print(error)
        files_error.append(file)

# COMMAND ----------

spark.sql(f"select * from ia.{tbl_work}").display()

# COMMAND ----------

tbl_retorno

# COMMAND ----------

# DBTITLE 1,Lê os registros da tabela work e insere na tabela de retorno final
spark.sql(f"""
    insert into ia.{tbl_retorno}
    (
        idRetorno,
        dataExecucaoModelo,
        idPredicao,
        idExame,
        dataHoraRetorno,
        dataHoraAtualizacao,
        arquivoRetorno,
        achado,
        observacoes,
        destino
    )
    select
        idRetorno,
        date(dataExecucaoModelo) as dataExecucaoModelo,
        idPredicao,
        idExame,
        dataHoraRetorno,
        dataHoraAtualizacao,
        arquivoRetorno,
        if(achado = 'None', null, achado) as achado,
        if(observacoes = 'None', null, observacoes) as observacoes,
        if(destino = 'None', null, destino) as destino
    from ia.{tbl_work}
    where (achado != 'None'
         or observacoes != 'None'
         or destino != 'None'
    )
""").display()

# COMMAND ----------

# DBTITLE 1,Otimiza a tabela de retorno
spark.sql(f"optimize ia.{tbl_retorno}").display()
spark.sql(f"analyze table ia.{tbl_retorno} compute statistics").display()

# COMMAND ----------

tbl_retorno

# COMMAND ----------

spark.sql(f"select * from ia.{tbl_retorno}").display()

# COMMAND ----------

# DBTITLE 1,Exclui a tabela work
spark.sql(f"drop table if exists ia.{tbl_work}").display()

# COMMAND ----------

tbl_retorno_hist

# COMMAND ----------

# DBTITLE 1,Grava os registros duplicados em uma tabela histórica
df_hist = spark.sql(f"""
    select * from ia.{tbl_retorno}
    qualify row_number() over (partition by idPredicao order by dataHoraRetorno desc) > 1
""")

(
    df_hist
    .write
    .mode("append")
    .option("path", f"/mnt/trusted/datalake/ia/data/{id_projeto}/gold/{tbl_retorno_hist}")
    .option("mergeSchema", "true")
    .saveAsTable(f"ia.{tbl_retorno_hist}")
)

# COMMAND ----------

# DBTITLE 1,Otimiza a tabela de retorno histórica
spark.sql(f"optimize ia.{tbl_retorno_hist}").display()
spark.sql(f"analyze table ia.{tbl_retorno_hist} compute statistics").display()

# COMMAND ----------

# DBTITLE 1,Remove da tabela de retorno os registros duplicados e mantém somente a última versão
query = f"""
    select idPredicao, dataHoraRetorno
    from ia.{tbl_retorno} as hist
    qualify row_number() over (partition by idPredicao order by dataHoraRetorno desc) > 1
"""

merge_query = f"""
    MERGE INTO ia.{tbl_retorno} AS t
    USING ({query}) AS s
        ON  t.idPredicao = s.idPredicao
        AND t.dataHoraRetorno = s.dataHoraRetorno
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

spark.sql(f"select * from  ia.{tbl_retorno}").display()

# COMMAND ----------

spark.sql(f"select * from  ia.{tbl_retorno_hist}").display()

# COMMAND ----------


