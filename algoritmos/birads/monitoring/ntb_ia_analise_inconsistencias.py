# Databricks notebook source
# MAGIC %md
# MAGIC # Instalação de libs

# COMMAND ----------

# DBTITLE 1,pip install
!pip install -q openpyxl xlsxwriter

# COMMAND ----------

# DBTITLE 1,restartPython
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import copy
import json
import openpyxl
import os
import pandas as pd
import pyspark
import requests
import subprocess

from dataclasses import dataclass, asdict
from datetime import datetime, date
from pathlib import Path
from pyspark.sql.functions import col, concat_ws

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
# MAGIC # Variávies de configurações

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

# DBTITLE 1,Obtém o caminho da pasta atual
current_folder = os.path.join("/tmp", id_projeto) + "/"
Path(current_folder).mkdir(parents=True, exist_ok=True)
print(current_folder)

# COMMAND ----------

# DBTITLE 1,Nomes das tabelas
vw_gold_modelo_birads_complemento = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_complemento"

# COMMAND ----------

# DBTITLE 1,Exibe variáveis
def print_params():
    print(f"{'environment':40}: {environment}")
    print(f"{'environment_tbl':40}: {environment_tbl}")
    print(f"{'catalog_name':40}: {catalog_name}")
    print(f"{'data_execucao_modelo':40}: {data_execucao_modelo}")
    print(f"{'id_projeto':40}: {id_projeto}")
    print(f"{'root_remote_path':40}: {root_remote_path}")
    print(f"{'root_remote_config_path':40}: {root_remote_config_path}")
    print(f"{'vw_gold_modelo_birads_complemento':40}: {vw_gold_modelo_birads_complemento}")

print_params()

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
# MAGIC # Exporta os dados para Excel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções

# COMMAND ----------

# DBTITLE 1,get_data
def get_data(data_execucao: str):
    """
    Recupera dados da tabela 'vw_gold_modelo_birads_complemento' filtrados por data de execução e critérios de inconsistência.

    Args:
        data_execucao (str): Data de execução do modelo no formato 'YYYY-MM-DD'.

    Returns:
        DataFrame: DataFrame contendo os dados filtrados e ordenados, com coluna adicional 'motivo' concatenando motivos de inconsistência.
    """
    df = spark.sql(f"""
        select * from {vw_gold_modelo_birads_complemento}
        where dt_execucao = '{data_execucao}'
        and fl_inconsistencia_negocio = 1
        and vl_proced_birads in (4, 5, 6)
        order by
            emp_nome_unidade,
            nome_paciente
    """)

    df = df.withColumn("motivo", concat_ws(", ", col("audit_motivo_inconsistencia_negocio")))

    return df

# COMMAND ----------

# DBTITLE 1,get_cols
def get_cols():
    """
    Dicionário de/para com os nomes das colunas de saída.

    Returns:
        dict: Dicionário onde as chaves são os nomes das colunas originais e os valores são os nomes das colunas de saída.
    """
    dic_col_names = {
        "dt_execucao": "dataExecucaoModelo",
        "id_exame": "idExame",
        "emp_nome_unidade": "Unidade",
        "emp_regional_unidade": "Regional",
        "nome_medico_encaminhador": "Médico Solicitante",
        "doc_crm_medico_encaminhador": "CRM",
        "uf_crm_medico_encaminhador": "UF CRM",
        "nome_paciente": "Paciente",
        "num_cpf_paciente": "CPF",
        "proced_nome_exame": "Exame",
        "vl_proced_birads": "Bi-rads",
        "cod_origem": "Origem",
        "audit_motivo_inconsistencia_negocio": "Motivo"
    }

    return dic_col_names

# COMMAND ----------

# DBTITLE 1,export_to_excel
def export_to_excel(df, dic_col_names, unidade):
    """
    Exporta um DataFrame para um arquivo Excel, aplicando nomes de colunas personalizados e salvando o arquivo localmente e remotamente.

    Args:
        df (DataFrame): DataFrame contendo os dados a serem exportados.
        dic_col_names (dict): Dicionário onde as chaves são os nomes das colunas originais e os valores são os nomes das colunas de saída.
        unidade (str): Nome da unidade para identificação no retorno.

    Returns:
        dict: Dicionário contendo informações sobre o arquivo exportado, incluindo nome, caminhos local e remoto, e detalhes de processamento.
    """
    file_name = f"{id_projeto}_inconsistencias_{unidade.lower()}_{data_execucao_modelo}.xlsx"
    local_file = f"{current_folder}{file_name}"
    remote_file = f"{remote_path}{file_name}"

    cols = [col for col in dic_col_names.keys()]
    df[cols].to_excel(local_file, index=False, freeze_panes=(1, 0), engine='xlsxwriter')

    return {
        "unidade": unidade,
        "nomeArquivo": file_name,
        "arquivoLocal": local_file,
        "arquivoRemoto": remote_file,
        "caminhoRemoto": remote_path.replace("/mnt", ""),
        "registros": len(df.index),
        "dataProcessamento": data_execucao_modelo,
        "dataProcessamentoFormatada": datetime.strptime(data_execucao_modelo, "%Y-%m-%d").strftime("%d/%m/%Y"),
    }

# COMMAND ----------

# DBTITLE 1,get_cols_config
def get_cols_config():
    """
    Define tamanho fixo para algumas colunas. ( -1 = Coluna oculta )

    Returns:
        dict: Dicionário onde as chaves são os nomes das colunas e os valores são os tamanhos fixos das colunas.
    """
    dic_col_fixed_width = {
        "dt_execucao": -1,
        "id_exame": -1,
    }
    return dic_col_fixed_width

# COMMAND ----------

# DBTITLE 1,format_excel
def format_excel(full_file_name, dic_col_names, dic_col_fixed_width):
    """
    Formata um arquivo Excel, ajustando nomes de colunas, alinhamento e largura das colunas.

    Args:
        full_file_name (str): Caminho completo do arquivo Excel a ser formatado.
        dic_col_names (dict): Dicionário onde as chaves são os nomes das colunas originais e os valores são os nomes das colunas de saída.
        dic_col_fixed_width (dict): Dicionário onde as chaves são os nomes das colunas e os valores são os tamanhos fixos das colunas (-1 para ocultar).

    Returns:
        None
    """
    import math
    from openpyxl.styles import Alignment

    wb = openpyxl.load_workbook(full_file_name)
    sheet = wb.active

    dic_col_size = {}
    wrap_text = []
    h_align_center = [
        "Regional",
        "UF CRM",
        "Bi-rads"
    ]

    for cols in sheet.iter_cols():
        for cell in cols:
            # Renomeia as colunas
            if cell.row == 1:
                cell.value = dic_col_names.get(cell.value, cell.value)

            column_letter = cell.column_letter
            len_value = len(str(cell.value))
            
            if len_value > dic_col_size.get(column_letter, 0):
                dic_col_size[column_letter] = len_value

            alignment = copy.copy(cell.alignment)
            alignment.vertical = "center"

            if cell.row > 1 and cols[0].value in wrap_text:
                alignment.wrapText = True

            if cell.row > 1 and cols[0].value in h_align_center:
                alignment.horizontal = "center"
        
            cell.alignment = alignment

    # Ajusta a largura das colunas
    for k, v in dic_col_size.items():
        adjusted_width = dic_col_fixed_width.get(sheet[f"{k}1"].value)

        if adjusted_width is None:
            adjusted_width = int((v + 2))

        if adjusted_width == -1:
            sheet.column_dimensions[k].hidden = True
        else:
            sheet.column_dimensions[k].width = adjusted_width
                
    wb.save(full_file_name)
    wb.close()

# COMMAND ----------

# DBTITLE 1,copy_files
def copy_files(source, target):
    """
    Copia um arquivo de uma localização de origem para um destino no sistema de arquivos do Databricks.

    Args:
        source (str): Caminho completo do arquivo de origem.
        target (str): Caminho completo do arquivo de destino.

    Returns:
        None
    """
    source = f"file://{source}"
    # source = f"{source}"
    print("Copiando arquivo")
    print(f"De  : {source}")
    print(f"Para: {target}")

    dbutils.fs.cp(source, target, recurse=True)

# COMMAND ----------

# DBTITLE 1,extract_data
def extract_data(unidade, df_export: pd.DataFrame):
    """
    Extrai dados para uma unidade específica, exporta para Excel, formata o arquivo e copia para um destino remoto.

    Args:
        unidade (str): Nome da unidade para a qual os dados estão sendo extraídos.
        df_export (pd.DataFrame): DataFrame contendo os dados a serem exportados.

    Returns:
        dict: Informações sobre o arquivo exportado, incluindo caminhos locais e remotos.
    """
    print("-" * 120)
    print(f"Extraindo dados para a unidade: {unidade}")
    
    print(f"Registros encontrados: {len(df_export.index)}")

    _dic_col_names = get_cols()
    
    _info = export_to_excel(df_export, _dic_col_names, unidade)
    
    _dic_col_fixed_width = get_cols_config()
    
    format_excel(_info['arquivoLocal'], _dic_col_names, _dic_col_fixed_width)
    
    copy_files(_info['arquivoLocal'], _info['arquivoRemoto'])

    return _info

# COMMAND ----------

# DBTITLE 1,clean
def clean(folder):
    """
    Remove todos os arquivos de uma pasta especificada.

    Args:
        folder (str): Caminho da pasta de onde os arquivos serão removidos.

    Returns:
        None
    """
    files = os.listdir(folder)
    for file in files:
        file_path = os.path.join(folder, file)        
        if os.path.isfile(file_path):
            print(f"Excluido arquivo: {file_path}")
            os.remove(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração e exportação

# COMMAND ----------

# DBTITLE 1,Carrega os dados
df = get_data(data_execucao_modelo)

# COMMAND ----------

root_remote_config_path

# COMMAND ----------

# DBTITLE 1,Carrega as configurações
df_config = spark.read.option("multiline","true").json(f"{root_remote_config_path}config_unidades_inconsistencias.json")
df_config.display()

# COMMAND ----------

# DBTITLE 1,Exporta os dados para cada unidade
files = []

for row in df_config.collect():
    if row.unidades is None:
        print("-" * 120)
        print(f"Unidade: {row.unidade} - Não configurada!")
        continue

    info = extract_data(row.unidade, df.toPandas())
    files.append(info)


# COMMAND ----------

# DBTITLE 1,Salva metadados dos arquivos gerados
metadados = f"{current_folder}{id_projeto}_metadados_envio.json"

json_data = json.dumps(files)

with open(metadados, "w") as file:
    file.write(json_data)

# COMMAND ----------

# DBTITLE 1,Copia arquivo com metadados para o storage
copy_files(metadados, root_remote_data_path)

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

# MAGIC %md
# MAGIC # Envio de arquivos para o OneDrive

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataclass Info

# COMMAND ----------

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
# MAGIC ## Funções

# COMMAND ----------

# DBTITLE 1,load_metadata
def load_metadata(file_path):
    """
    Carrega metadados de um arquivo JSON e cria uma lista de objetos Info.

    Args:
        file_path (str): O caminho do arquivo JSON contendo os metadados.

    Returns:
        list: Uma lista de objetos Info preenchidos com os dados do arquivo JSON.
    """
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
    """
    Carrega a configuração de um arquivo JSON e atualiza a lista de objetos Info.

    Args:
        file_path (str): O caminho do arquivo JSON contendo a configuração.
        info_list (list): Uma lista de objetos Info a serem atualizados.

    Returns:
        list: A lista de objetos Info atualizada com os dados da configuração.
    """
    df = spark.read.option("multiline", "true").json(file_path)
    display(df)

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
    """
    Carrega informações de unidades de um arquivo JSON e atualiza a lista de objetos Info.

    Args:
        file_path (str): O caminho do arquivo JSON contendo as informações das unidades.
        info_list (list): Uma lista de objetos Info a serem atualizados.

    Returns:
        list: A lista de objetos Info atualizada com os dados das unidades.
    """
    df = spark.read.option("multiline", "true").json(file_path)

    for info in info_list:
        df_filtered = df.filter(df.unidade == info.unidade)

        if not df_filtered.rdd.isEmpty():
            row = df_filtered.collect()[0]

            info.emailTo = row["emailTo"]
            info.emailCc = row["emailCc"]
            info.emailBcc = row["emailBcc"]
        else:
            info.emailTo = info.emailToDefault

    return info_list

# COMMAND ----------

# DBTITLE 1,send_to_onedrive
def send_to_onedrive(info_list):
    """
    Envia informações para o OneDrive usando a URL da Logic App.

    Args:
        info_list (list): Uma lista de objetos Info contendo as informações a serem enviadas.

    Returns:
        None
    """
    for info in info_list:
        print("-" * 120)
        print(f"Enviando: {info.unidade} - Registros: {info.registros}")
        payload = json.dumps(asdict(info))

        response = requests.post(
            info.logicAppUrl, data=payload, headers={"Content-Type": "application/json"}
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga das configurações

# COMMAND ----------

# DBTITLE 1,Carrega os metadados
file_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/data/{environment}/envio/{id_projeto}_metadados_envio.json"
info_list = load_metadata(file_path)
print(info_list)

# COMMAND ----------

# DBTITLE 1,Carrega as configurações de envio
config_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/confg_email_inconsistencias.json"
info_list = load_config(config_path, info_list)
print(info_list)

# COMMAND ----------

# DBTITLE 1,Carrega configurações das Unidades
unidades_path = f"/mnt/trusted/datalake/ia/projetos/{id_projeto}/config/{environment}/config_unidades_inconsistencias.json"
info_list = load_unidades(unidades_path, info_list)
print(info_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envia arquivos

# COMMAND ----------

send_to_onedrive(info_list)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fim da execução

# COMMAND ----------

dbutils.notebook().exit("Fim da execução!")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Envio de Notificação no Teams (DESATIVADO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções

# COMMAND ----------

# DBTITLE 1,build_header
def build_header(data_execucao: str):
    header = [
        {
            "type": "TextBlock",
            "size": "ExtraLarge",
            "weight": "Bolder",
            "text": "Pacientes com dados inconsistentes",
            "horizontalAlignment": "Center",
            "color": "Default",
            "isSubtle": False,
            "wrap": True,
            "spacing": "None"
        },
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": "Data de Execução",
                            "isSubtle": False
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": data_execucao,
                            "wrap": True,
                            "horizontalAlignment": "Right",
                            "size": "Large",
                            "weight": "Bolder",
                            "color": "Good"
                        }
                    ]
                }
            ]
        },
    ]
    return header

# COMMAND ----------

# DBTITLE 1,build_inconsistencies
def build_inconsistencies(inconsistencies: list):
    _rows = []

    for _motivo in inconsistencies:
        _rows.append(
            {
                "type": "TableRow",
                "cells": [
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": f"- {_motivo}",
                                "wrap": True
                            }
                        ]
                    }
                ]
            }
        )
    
    return _rows

# COMMAND ----------

# DBTITLE 1,build_patient_body
def build_patient_body(
    nome_paciente: str,
    categoria_birads: str,
    unidade: str,
    regional: str,
    origem: str,
    inconsistencias: list
):
    _unidade = f"{unidade} - {regional}"
    _inconsistencias = build_inconsistencies(inconsistencias)

    body = [
        {
            "type": "TextBlock",
            "size": "Large",
            "weight": "Bolder",
            "text": nome_paciente,
            "separator": True,
            "color": "Accent",
            "wrap": True
        },
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "size": "Medium",
                            "weight": "Bolder",
                            "text": "Bi-rads",
                            "color": "Accent"
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": categoria_birads,
                            "wrap": True,
                            "horizontalAlignment": "Right",
                            "size": "Medium",
                            "weight": "Bolder",
                            "isSubtle": True
                        }
                    ]
                }
            ]
        },
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [
                        {
                            "type": "TextBlock",
                            "size": "Medium",
                            "weight": "Bolder",
                            "text": "Unidade",
                            "color": "Accent"
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": _unidade,
                            "wrap": True,
                            "horizontalAlignment": "Right",
                            "size": "Medium",
                            "weight": "Bolder",
                            "isSubtle": True
                        }
                    ]
                }
            ]
        },
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [
                        {
                            "type": "TextBlock",
                            "size": "Medium",
                            "weight": "Bolder",
                            "text": "Origem",
                            "color": "Accent"
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": origem,
                            "wrap": True,
                            "horizontalAlignment": "Right",
                            "size": "Medium",
                            "weight": "Bolder",
                            "isSubtle": True
                        }
                    ]
                }
            ]
        },
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": "Inconsistências:",
                            "color": "Attention"
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "stretch"
                }
            ]
        },
        {
            "type": "Table",
            "columns": [
                {
                    "width": 1
                }
            ],
            "rows": _inconsistencias,
            "firstRowAsHeaders": False,
            "showGridLines": False
        },
    ]

    return body

# COMMAND ----------

# DBTITLE 1,build_action_set
def build_action_set():
    action_set = [
        {
            "type": "ActionSet",
            "actions": [
                {
                    "type": "Action.OpenUrl",
                    # "title": "Visualizar todos os pacientes",
                    "title": "Clique aqui para visualizar todos os pacientes com inconsistências no dias 01/04/2025",
                    "url": "https://rededor-my.sharepoint.com/:x:/g/personal/ia-service-user_rededor_com_br/EQwFpXUjO1xEhD59GkvSeVgBL8dtuJPIDQ4UonsqCUvfNA?e=8IW8Cw",
                    "tooltip": "Visualizar todos os pacientes"
                }
            ]
        }
    ]
    return action_set

# COMMAND ----------

# DBTITLE 1,build_chat_message
def build_chat_message(
    *,
    environment: str,
    data_execucao_modelo: str,
    df: pyspark.sql.dataframe.DataFrame,
):
    _data_execucao = datetime.strptime(data_execucao_modelo, "%Y-%m-%d").strftime("%d/%m/%Y")

    df_birads = df.filter(df['birads'].isin([4, 5])).orderBy('nomePaciente')

    body = build_header(_data_execucao)

    
    for row in df_birads.collect():
        body += build_patient_body(
            nome_paciente = row['nomePaciente'],
            categoria_birads = row['birads'],
            unidade = row['nomeHospital'],
            regional = row['regional'],
            origem = row['origem'],
            inconsistencias = row['motivoInconsistenciaNegocio']
        )

    # body += build_patient_body()
    body += build_action_set()

    content = {
        "type": "AdaptiveCard",
        "body": body,
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

# MAGIC %md
# MAGIC ## Envio da notificação

# COMMAND ----------

# DBTITLE 1,Envia notificação
# DEV
url = "https://prod-111.westus.logic.azure.com:443/workflows/f8fc34ba7d3945feb9a5f368695a188b/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=el2Vjy30_8sbshzQ9MKRiYV4hricauyFJT5GvPqSvEQ"

df = get_data(data_execucao_modelo)
message = build_chat_message(environment=environment, data_execucao_modelo=data_execucao_modelo, df=df)
send_chat_message(url=url, content=message)
