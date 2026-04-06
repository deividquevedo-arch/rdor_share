# Databricks notebook source
!pip install -q xlsxwriter

# COMMAND ----------

!pip install -q openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")
print("Environment:", environment)

# COMMAND ----------

dbutils.widgets.text("project_id", "reumatologia", "Código do Projeto")

project_id = dbutils.widgets.get("project_id")
project_id = project_id.lower()

print("ID do Projeto:", project_id)

# COMMAND ----------

if environment in ["hml", "prd"]:
    environment_tbl = ""
else:
    environment_tbl = f"{environment}_"

print("environment_tbl:", environment_tbl)

# COMMAND ----------

if environment == "dev":
    adf_bucket = "/mnt/trusted/datalake/ia/projetos/reumatologia/data"
    logic_app_url = "https://prod-53.eastus2.logic.azure.com:443/workflows/ad36056e03004d2990ed2c96f2e9b21a/triggers/Vari%C3%A1veis/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FVari%C3%A1veis%2Frun&sv=1.0&sig=6nMiNbSBVeGf7x3rlCCPVQASxjuPwoJywvNm7_47rYo"

# COMMAND ----------

dbutils.widgets.text("file_name", "{project_id}_{formatted_date}.xlsx", "File Name")

file_name = dbutils.widgets.get("file_name")

print("File Name:", file_name)

# COMMAND ----------

view_name_saida = f"{environment_tbl}vw_gold_modelo_{project_id}"
print(view_name_saida)

# COMMAND ----------

import pandas as pd
import openpyxl
import copy
import requests
from datetime import date, datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração de display

# COMMAND ----------

current_date = date.today()
formatted_date = current_date.strftime('%Y%m%d')
file_name = eval(f'f"""{file_name}"""')
output_file = f"/databricks/driver/{file_name}"
print(output_file)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exporta os dados para um arquivo Excel

# COMMAND ----------



# COMMAND ----------

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 230)

# COMMAND ----------

view_name_saida = f"{environment_tbl}vw_gold_modelo_{project_id}"
print(view_name_saida)

# COMMAND ----------

df_export = spark.sql(f"select *  from ia.{view_name_saida}").toPandas()

# COMMAND ----------

display(df_export)

# COMMAND ----------

df_export.shape[0]

# COMMAND ----------

df_export.dtypes

# COMMAND ----------



# COMMAND ----------

fields = ["idPredicao",
"numCPF",
"nomePaciente",
"sexoPaciente",
"telefoneContato",
"nomeConvenio",
"nomeUnidade",
"regionalUnidade",
"numCrmSolicitante",
"ufCrmSolicitante",
"nomeMedicoSolicitante",
"dataExame",
"codigoTuss",
"descricaoTuss",
"descricaoProcedimento",
"laudoExame",
"valorProbabilidade"]

# COMMAND ----------

df_export = df_export[fields]

# COMMAND ----------

df_export.to_excel(output_file, index=False, freeze_panes=(1, 0), engine='xlsxwriter')

# COMMAND ----------

dic_col_names = {
    "ID": 30
    , "CPF": 30
    , "Nome do Paciente": 30
    , "Sexo do Paciente": 10
    , "Telefone do Paciente": 20
    , "Nome do Convenio": 30
    , "Unidade": 30
    , "Regional da Unidade": 30
    , "CRM": 10
    , "UF CRM": 10
    , "Médico Solicitante": 30
    , "Data Exame": 20
    , "Codigo TUSS": 30
    , "Descrição TUSS": 50
    , "Procedimento": 30
    , "Laudo": 80
    , "Probabilidade": 30
}

# COMMAND ----------

dic_col_names = {
    "idPredicao" : "ID",
    "numCPF" : "CPF",
    "nomePaciente" : "Nome do Paciente", 
    "sexoPaciente" : "Sexo do Paciente", 
    "telefoneContato" : "Telefone do Paciente",
    "nomeConvenio" : "Nome do Convenio", 
    "nomeUnidade" : "Unidade",
    "regionalUnidade" : "Regional da Unidade",
    "numCrmSolicitante" : "CRM",
    "ufCrmSolicitante" : "UF CRM", 
    "nomeMedicoSolicitante" : "Médico Solicitante",
    "dataExame" : "Data Exame", 
    "codigoTuss" : "Codigo TUSS",
    "descricaoTuss" : "Descrição TUSS",
    "descricaoProcedimento" : "Procedimento",
    "laudoExame" : "Laudo",
    "valorProbabilidade" : "Probabilidade"
}

# COMMAND ----------

# dic_col_fixed_width = {
#     "Laudo": 150,
#     "Achado": 80,
#     "Linha de Cuidado": 80,
#     "Observação": 80,
#     "Destino": 80,

# }

# COMMAND ----------

import math
from openpyxl.styles import Alignment

wb = openpyxl.load_workbook(output_file)
sheet = wb.active

dic_col_size = {}

for cols in sheet.iter_cols():
    for cell in cols:

        # print(dir(cell))
        # print(cell.row)

        # Renomeia as colunas
        if cell.row == 1:
            cell.value = dic_col_names.get(cell.value, cell.value)

        column_letter = cell.column_letter
        len_value = len(str(cell.value))
        
        if len_value > dic_col_size.get(column_letter, 0):
            dic_col_size[column_letter] = len_value

        alignment = copy.copy(cell.alignment)
        alignment.vertical="center"

        # if cols[0].value == "Laudo":    
        #     alignment.wrapText=True

        cell.alignment = alignment

    dic_col_fixed_width = {
        "ID": 40
        , "CPF": 25
        , "Nome do Paciente": 40
        , "Sexo do Paciente": 10
        , "Telefone do Paciente": 20
        , "Nome do Convenio": 30
        , "Unidade": 30
        , "Regional da Unidade": 30
        , "CRM": 10
        , "UF CRM": 10
        , "Médico Solicitante": 30
        , "Data Exame": 20
        , "Codigo TUSS": 30
        , "Descrição TUSS": 50
        , "Procedimento": 30
        , "Laudo": 80
        , "Probabilidade": 30
    }

    # Ajusta a largura das colunas
    for k, v in dic_col_size.items():
        adjusted_width = dic_col_fixed_width.get(sheet[f"{k}1"].value)

        if adjusted_width is None:
            adjusted_width = int((v + 2))

        if adjusted_width == -1:
            sheet.column_dimensions[k].hidden = True
        else:
            sheet.column_dimensions[k].width = adjusted_width

# # Ajusta a largura das colunas
# for k, v in dic_col_size.items():
#     adjusted_width = dic_col_fixed_width.get(sheet[f"{k}1"].value)

#     if adjusted_width is None:
#         adjusted_width = int((v + 2))

#     sheet.column_dimensions[k].width = adjusted_width

wb.save(output_file)
wb.close()


# COMMAND ----------

source_file = f"file://{output_file}"
target_file = f"/mnt/trusted/datalake/ia/projetos/{project_id}/data/{file_name}"

print("Copiando arquivo")
print(f"De: {source_file}")
print(f"Para: {target_file}")

dbutils.fs.cp(source_file, target_file)


# COMMAND ----------

emailTo = "dmarcello.sulamerica@rededor.com.br"
# emailTo = "famaral.fcct@rededor.com.br"
emailBcc = "dmarcello.sulamerica@rededor.com.br"
fileName = target_file.split('/')[-1]
sourcePath = target_file.replace(fileName, '')
sourcePath = sourcePath.replace('/mnt', '')
targetPath = "/Projetos/Reumatologia/Shared/Envio/"

body = f"""<h1>Identificação Reumatologia</h1><p>O processo de reumatologia foi executado e um arquivo contendo pacientes com probabilidade de ser reumatológico foi exportado.</p><h4>Link para o arquivo:"""
body = body + " <a href='{{fileUrl}}'>{{fileName}}</a><h4>"

payload = {
"fileName": fileName,
"sourcePath": sourcePath,
"targetPath": targetPath,
"environment": environment,
"unidade": "Todas",
"onedriveUser": "ia-service-user_rededor_com_br",
"emailTo": emailTo,
"emailCc": "",
"emailBcc": emailBcc,
"emailSubject": f"[{environment}][IA - Reumatologia] {datetime.now().strftime('%d/%m/%Y')}",
"emailBody": body,
"registros": df_export.shape[0],
"dataProcessamento": datetime.now().strftime("%Y-%m-%d"),
"dataProcessamentoFormatada": datetime.now().strftime("%d/%m/%Y")
}

req = requests.post(logic_app_url, json=payload)
print(req.status_code)

# COMMAND ----------

!rm -f {output_file}
