# Databricks notebook source
dbutils.widgets.text("project_id", "reumatologia", "Código do Projeto")

project_id = dbutils.widgets.get("project_id")
project_id = project_id.lower()

print("ID do Projeto:", project_id)

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")
print("Environment:", environment)

# COMMAND ----------

if environment in ["hml", "prd"]:
    environment_tbl = ""
else:
    environment_tbl = f"{environment}_"

print("environment_tbl:", environment_tbl)

# COMMAND ----------

table_name_entrada = f"{environment_tbl}tbl_gold_{project_id}_entrada"
print(table_name_entrada)

# COMMAND ----------

table_name_saida = f"{environment_tbl}tbl_gold_modelo_{project_id}_saida"
print(table_name_saida)

# COMMAND ----------

view_name_saida = f"{environment_tbl}vw_gold_modelo_{project_id}"
print(view_name_saida)

# COMMAND ----------

try:
    dbutils.fs.mkdirs(f"/mnt/trusted/datalake/ia/data/{project_id}/gold/{table_name_entrada}")
except Exception as e:
    print(e)

# COMMAND ----------

spark.sql(f"""
    create table if not exists  ia.{table_name_entrada} 
    (
         idPredicao STRING
        ,idPatient STRING
        ,idExame STRING
        ,idMedico STRING
        ,idUnidade STRING
        
        ,numCPF STRING
        ,nomePaciente STRING
        ,sexoPaciente STRING
        ,idadePaciente STRING
        ,telefoneContato STRING
        ,nomeConvenio STRING

        ,cnpjUnidade STRING
        ,nomeUnidade STRING
        ,regionalUnidade STRING

        ,numCrmSolicitante STRING
        ,ufCrmSolicitante STRING
        ,nomeMedicoSolicitante STRING
        ,dataExame DATE
        ,codigoTuss STRING
        ,descricaoTuss STRING
        ,descricaoProcedimento STRING
        ,laudoExame STRING
        ,datParticao DATE
    )
    partitioned by (datParticao)
    location '/mnt/trusted/datalake/ia/data/{project_id}/gold/{table_name_entrada}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Tabela de saida

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC  if not exists

# COMMAND ----------

spark.sql(f"""create table if not exists  ia.{table_name_saida}
    (
        idPredicao STRING,
        idPatient STRING,
        idExame STRING,
        tmpPredicao TIMESTAMP,
        valorProbabilidade FLOAT,
        dataExecucaoModelo DATE
    )
    partitioned by (dataExecucaoModelo)
    location '/mnt/trusted/datalake/ia/data/{project_id}/gold/{table_name_saida}'
""").display()

print("Tabela criada ", table_name_saida)

# COMMAND ----------



# COMMAND ----------

table_name_saida_wrk = f"{environment_tbl}tbl_wrk_gold_modelo_{project_id}_saida"
print(table_name_saida_wrk)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS  ia.{table_name_saida_wrk} (
  
  idPredicao string,
  idPatient string,
  idExame string,
  idMedico string,
  laudoExame string,
  dataProcessamento date,
  valorProbabilidade double,

  encontrado ARRAY<STRUCT<fraseAvaliada: STRING, fraseParte: STRING, fraseParteIgnorar: STRING, fraseProcurado: STRING, fraseProcuradoIgnorar: STRING, valorProbabilidade: DOUBLE, valorProbabilidadeCorrigido: DOUBLE, valorProbabilidadeIgnorar: DOUBLE>>
  )
location '/mnt/trusted/datalake/ia/data/{project_id}/gold/{table_name_saida_wrk}'
""").display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Preparacao da View de Saída

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,View criada para atender os padrões de export para FTP
spark.sql(f"""create or replace view   ia.{view_name_saida} as 
select distinct
  inp.idPredicao
  ,inp.idPatient 
  ,inp.idExame 
  ,inp.idMedico 
  ,inp.idUnidade 
  ,default.unencrypt(inp.numCPF , 0) as numCPF
  ,default.unencrypt(inp.nomePaciente, 0) as nomePaciente 
  ,inp.idadePaciente
  ,inp.sexoPaciente 
  ,inp.telefoneContato 
  ,inp.nomeConvenio 
  ,default.unencrypt(inp.cnpjUnidade , 0)  as cnpjUnidade
  ,inp.nomeUnidade
  ,default.unencrypt(inp.regionalUnidade , 0)  as regionalUnidade
  ,default.unencrypt(inp.numCrmSolicitante , 0)  as numCrmSolicitante
  ,inp.ufCrmSolicitante 
  ,default.unencrypt(inp.nomeMedicoSolicitante , 0 ) as nomeMedicoSolicitante
  ,inp.dataExame 
  ,inp.codigoTuss 
  ,inp.descricaoTuss 
  ,inp.descricaoProcedimento 
  ,inp.laudoExame 
  ,outp.valorProbabilidade
from        ia.{table_name_entrada} as inp
inner join  ia.{table_name_saida} as outp 
on 
  inp.idPredicao = outp.idPredicao

where outp.valorProbabilidade > 50.0
and inp.datParticao = current_date()
""").display()

# COMMAND ----------

view_name_saida
