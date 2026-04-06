# Databricks notebook source
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
dbutils.widgets.text("id_projeto", "reumatologia", "ID Projeto")
id_projeto = dbutils.widgets.get("id_projeto")
print("id_projeto:", id_projeto)

# COMMAND ----------

# DBTITLE 1,Data Execução Modelo
from datetime import datetime

dbutils.widgets.text("data_execucao_modelo", "", "Data Execução Modelo")
data_execucao_modelo = dbutils.widgets.get("data_execucao_modelo")
if data_execucao_modelo == "":
    data_execucao_modelo = datetime.now().strftime("%Y-%m-%d")

print(f"Data Execução Modelo: {data_execucao_modelo}")

# COMMAND ----------

# DBTITLE 1,Define variável que será injetada no nome das tabelas
if environment in ["hml", "prd"]:
    environment_tbl = ""
else:
    environment_tbl = f"{environment}_"

print("environment_tbl:", environment_tbl)

# COMMAND ----------

# DBTITLE 1,Define variáveis com nomes das tabelas
vw_name_saida = f"{environment_tbl}vw_gold_modelo_{id_projeto}"
print(vw_name_saida)

# COMMAND ----------

# DBTITLE 1,Define o caminho temporário para extração
temp_remote_path = f"/mnt/temp/ia/{id_projeto}/temp_ftp/"
print(temp_remote_path)

# COMMAND ----------

# DBTITLE 1,Define o caminho principal da pasta remota
root_remote_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/"
print(root_remote_path)

# COMMAND ----------

# DBTITLE 1,Define o caminho principal da pasta remota de configs
root_remote_config_path = f"{root_remote_path}config/{environment}/"
print(root_remote_config_path)

# COMMAND ----------

# DBTITLE 1,Define o caminho principal da pasta remota dos dados
root_remote_data_path = f"{root_remote_path}data/{environment}/envio/"
print(root_remote_data_path)

# COMMAND ----------

# DBTITLE 1,Define o caminho mensal da pasta remota
year, month, day = data_execucao_modelo.split("-")
remote_path = f"{root_remote_data_path}{year}/{month}/"
print(remote_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

# DBTITLE 1,Import
import os
import pandas as pd
import copy
import json
import subprocess

from datetime import date
from pathlib import Path

# COMMAND ----------

# DBTITLE 1,Obtém o caminho da pasta atual
shell_command = "pwd"
output = subprocess.check_output(shell_command, shell=True)
current_folder = os.path.join(output.decode().strip(), id_projeto) + "/"

Path(current_folder).mkdir(parents=True, exist_ok=True)

print(current_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração de display

# COMMAND ----------

# DBTITLE 1,pd.set_option
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 230)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exporta os dados

# COMMAND ----------

vw_name_saida

# COMMAND ----------

# DBTITLE 1,Função get_data
def get_data(unidades):
  df = spark.sql(f"""
      select
         current_date() as dataExecucaoModelo
        , idPredicao
        , idExame

        , idUnidade
        , cnpjUnidade
        , nomeUnidade
        , regionalUnidade

        , numCrmSolicitante
        , ufCrmSolicitante
        , nomeMedicoSolicitante

        , numCpf as cpfPaciente
        , nomePaciente
        , idadePaciente
        , sexoPaciente
        , telefoneContato
        , nomeConvenio

        , dataExame
        , codigoTuss
        , descricaoTuss
        , descricaoProcedimento
        , replace( replace(laudoExame, '"', '') , '\n', '') as laudoExame
        
        , valorProbabilidade
        
      from ia.{vw_name_saida}
      
      where idUnidade in ({unidades})
  """)

  return df

# COMMAND ----------

# DBTITLE 1,Função move_parquet
def move_parquet(source, target):
    for file in dbutils.fs.ls(source):
        if file.name.endswith(".parquet"):
            dbutils.fs.mv(file.path, target)
            return True
    else:
        return False

# COMMAND ----------

def move_csv(source, target):
    for file in dbutils.fs.ls(source):
        if file.name.endswith(".csv"):
            dbutils.fs.mv(file.path, target)
            return True
    else:
        return False

# COMMAND ----------

# DBTITLE 1,Função
def download_files(source, target):
    target = f"file://{target}"
    
    print("Baixando arquivo")
    print(f"De  : {source}")
    print(f"Para: {target}")

    dbutils.fs.cp(source, target, recurse=True)

# COMMAND ----------

# DBTITLE 1,Função export_to_parquet
def export_to_parquet(df, unidade):
    file_name = f"{id_projeto.capitalize()}-{unidade}-{data_execucao_modelo}.parquet"
    local_file = f"{current_folder}{file_name}"
    remote_file = f"{remote_path}{file_name}"

    df.repartition(1).write.mode("overwrite").parquet(temp_remote_path)

    move_parquet(temp_remote_path, remote_file)

    download_files(remote_file, local_file)

    return {
        "unidade": unidade,
        "nomeArquivo": file_name,
        "arquivoLocal": local_file,
        "arquivoRemoto": remote_file,
        "caminhoRemoto": remote_path.replace("/mnt", ""),
        "registros": df.count(),
        "dataProcessamento": data_execucao_modelo,
        "dataProcessamentoFormatada": datetime.strptime(data_execucao_modelo, "%Y-%m-%d").strftime("%d/%m/%Y"),
    }

# COMMAND ----------

def export_to_csv(df, unidade):
    file_name = f"{id_projeto.capitalize()}-{unidade}-{data_execucao_modelo}.csv"
    local_file = f"{current_folder}{file_name}"
    remote_file = f"{remote_path}{file_name}"

    df.repartition(1).write.mode("overwrite").option("header", "true").option("quote", '"').csv(temp_remote_path)

    move_csv(temp_remote_path, remote_file)

    download_files(remote_file, local_file)

    return {
        "unidade": unidade,
        "nomeArquivo": file_name,
        "arquivoLocal": local_file,
        "arquivoRemoto": remote_file,
        "caminhoRemoto": remote_path.replace("/mnt", ""),
        "registros": df.count(),
        "dataProcessamento": data_execucao_modelo,
        "dataProcessamentoFormatada": datetime.strptime(data_execucao_modelo, "%Y-%m-%d").strftime("%d/%m/%Y"),
    }

# COMMAND ----------

# DBTITLE 1,Função copy_files
def copy_files(source, target):
    source = f"file://{source}"
    # print("-"*120)
    print("Copiando arquivo")
    print(f"De  : {source}")
    print(f"Para: {target}")

    dbutils.fs.cp(source, target, recurse=True)

# COMMAND ----------

# DBTITLE 1,Função extract_data
def extract_data(unidade, ids):
    print("-" * 120)
    print(f"Extraindo dados para a unidade: {unidade}")
    _df_export = get_data(ids)

    print(f"Registros encontrados: {_df_export.count()}")
    _info = export_to_parquet(_df_export, unidade)
    # _info = export_to_csv(_df_export, unidade)

    return _info

# COMMAND ----------

# DBTITLE 1,Função clean
def clean(folder):
    files = os.listdir(folder)
    for file in files:
        file_path = os.path.join(folder, file)        
        if os.path.isfile(file_path):
            print(f"Excluido arquivo: {file_path}")
            os.remove(file_path)

# COMMAND ----------

# DBTITLE 1,Função send_ftp
import paramiko, os
from io import BytesIO

def send_ftp(
    *,
    files: [str],
    host: str,
    port: int,
    username: str,
    password: str,
    remote_path: str,
):
    print(f"Conectando: {host}:{port}")
    print("-"*120)

    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception as error:
        print(f"Erro ao conectar: {error}")
        return False
    
    for _file in files:
        _remote_file = os.path.join(remote_path, os.path.basename(_file))

        try:
            print(f"Carregando arquivo: {_file}")
            buffer = BytesIO()
            with open(_file, 'rb') as f:
                buffer.write(f.read())
            buffer.seek(0)
        except Exception as error:
            print(f"Erro ao carregar arquivo: {error}")
            print("-"*120)
            continue

        try:
            print(f"Enviando para: {_remote_file}")
            with sftp.file(_remote_file, 'wb') as remote_file:
                remote_file.write(buffer.read())
            print(f"Arquivo enviado com sucesso!")
            print("-"*120)
        except Exception as error:
            print(f"Erro ao enviar arquivo: {error}")
            print("-"*120)
            continue

    print(f"Fechando conexão!")
    try:
        sftp.close()
        transport.close()
    except Exception as error:
        print(f"Erro ao fechar conexão: {error}")
    
    return True

# COMMAND ----------

# DBTITLE 1,Carrega as configurações
df = spark.read.option("multiline","true").json(f"{root_remote_config_path}unidades_ftp.json")
df.display()

# COMMAND ----------

# DBTITLE 1,Exporta os dados para cada unidade
files = []

for row in df.collect():
    if row.unidades is None:
        print("-" * 120)
        print(f"Unidade: {row.unidade} - Não configurada!")
        continue

    info = extract_data(row.unidade, row.unidades)
    files.append(info)

# COMMAND ----------

# DBTITLE 1,Salva metadados dos arquivos gerados
metadados = f"{current_folder}{id_projeto}_metadados_envio_ftp.json"

json_data = json.dumps(files)

with open(metadados, "w") as file:
    file.write(json_data)

# COMMAND ----------

# DBTITLE 1,Copia arquivo com metadados para o storage
copy_files(metadados, root_remote_data_path)

# COMMAND ----------

# DBTITLE 1,Envia arquivos para o FTP
host = "mcnplmk800q7glj-l0kvmhrnn9fm.ftp.marketingcloudops.com"
port = 22
username = "514007409_3"
password = dbutils.secrets.get(scope="dbricks-keyvault-secrets", key="ftp-sf-marketing-cloud")
remote_path = "/Export/IA/"
local_files = [file["arquivoLocal"] for file in files]

send_ftp(
    files=local_files,
    host=host,
    port=port,
    username=username,
    password=password,
    remote_path=remote_path,
)

# COMMAND ----------

# DBTITLE 1,Apaga arquivos locais (se existirem)
clean(current_folder)

# COMMAND ----------

# DBTITLE 1,Lista conteúdo da pasta local
!ls -lha {current_folder}

# COMMAND ----------

# DBTITLE 1,Remove pasta
!rm -rf {current_folder}

# COMMAND ----------

# DBTITLE 1,Fim do processamento!
dbutils.notebook.exit("Fim do processamento!")

# COMMAND ----------


