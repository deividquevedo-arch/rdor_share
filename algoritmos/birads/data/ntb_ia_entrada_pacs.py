# Databricks notebook source
# MAGIC %md
# MAGIC # Bi-Rads
# MAGIC ### Carrega laudos do PACS que não foram encontrados no RC 

# COMMAND ----------

!pip install -q striprtf==0.0.22 lxml==4.9.4

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from striprtf.striprtf import rtf_to_text
from lxml import etree

# COMMAND ----------

# MAGIC %md
# MAGIC #Parâmetros

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

# DBTITLE 1,html_to_text
def html_to_text(laudo):
    try:
        parser = etree.HTMLParser()
        tree = etree.fromstring(laudo, parser)        
        return etree.tostring(tree, encoding='unicode', method='text').strip().replace ("\n", " ")
    except:
        return laudo

# COMMAND ----------

# DBTITLE 1,converte_laudo
def converte_laudo(laudo_rtf, laudo_html):
    """Converte laudo para texto, se estiver no formato rtf ou html"""
    is_rtf = laudo_rtf.strip().lower().startswith("{\\rtf1\\")

    if is_rtf:
        retorno = rtf_to_text(laudo_rtf, errors="ignore")

    if retorno.strip() != "":
        return retorno

    try:
        retorno = html_to_text(laudo_html)
    except:
        retorno = ""

    return retorno.strip()

converte_laudo_udf = spark.udf.register("converte_laudo_udf", converte_laudo)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas

# COMMAND ----------

tbl_modelo_rads_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_entrada"
tbl_wrk_api_birads = f"{work_catalog}.{environment_tbl}tb_diamond_wkr_birads_carga_pacs_api"

# COMMAND ----------

print(f"{'tbl_modelo_rads_entrada':30}: {tbl_modelo_rads_entrada}")
print(f"{'tbl_wrk_api_birads':30}: {tbl_wrk_api_birads}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Queries

# COMMAND ----------

# DBTITLE 1,Carrega dados do RC
spark.sql(f"""
create or replace temp view vwApi
as
select
  *,
  gold_corporativo.default.rdsl_decrypt(num_cpf_paciente, 1) as cpfPacienteDecrypted,
  gold_corporativo.default.rdsl_decrypt(nome_paciente, 1) as nomePacienteDecrypted,
  emp_cnpj_unidade as cnpjHospitalDecrypted

from {tbl_modelo_rads_entrada}
where dt_execucao = date('{data_execucao_modelo}')
and ifnull(cod_origem, '') != 'PACS'
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) as qtd from vwApi

# COMMAND ----------

spark.sql(f"drop table if exists {tbl_wrk_api_birads}").display()

# COMMAND ----------

# DBTITLE 1,Carrega dados do PACS
df = spark.sql(f"""
    select * from ia.vw_enave_df_historico_birads
    where data_carga = '{data_execucao_modelo}'
""")

df = df.withColumn("laudoLimpo", converte_laudo_udf(df["resultado_exame"], df["LaudoHTML"]))

df.write.mode("overwrite").option("path", table_location(tbl_wrk_api_birads)).saveAsTable(tbl_wrk_api_birads)

# COMMAND ----------

spark.sql(f"""
    update {tbl_wrk_api_birads} set CNPJ = lpad(CNPJ, 14, 0)
""").display()

# COMMAND ----------

spark.sql(f"select count(1) as total from {tbl_wrk_api_birads}").display()

# COMMAND ----------

# DBTITLE 1,Exclui por idAtendimento
spark.sql(f"""
  MERGE INTO {tbl_wrk_api_birads} AS target
  USING vwApi AS source
  ON target.idAtendimento = source.num_pedido_integracao
  WHEN MATCHED THEN DELETE
""").display()

# COMMAND ----------

spark.sql(f"select count(1) as total from {tbl_wrk_api_birads}").display()

# COMMAND ----------

# DBTITLE 1,Exclui por CPF
spark.sql(f"""
  MERGE INTO {tbl_wrk_api_birads} AS target
  USING (
    select
      pacs.idAtendimento,
      pacs.idHospital,
      pacs.Unidade,
      pacs.Regional,
      pacs.CNPJ,
      pacs.CPF,
      pacs.Paciente,
      pacs.resultados,

      api.id_exame,
      api.id_unidade,
      api.emp_nome_unidade,
      api.cnpjHospitalDecrypted,
      api.cpfPacienteDecrypted,
      api.nomePacienteDecrypted,
      api.proced_laudo_exame
      
    from {tbl_wrk_api_birads} as pacs

    inner join vwApi as api
      on pacs.CPF = api.cpfPacienteDecrypted

  ) AS source
  ON target.idAtendimento = source.idAtendimento
  WHEN MATCHED THEN DELETE
""").display()

# COMMAND ----------

spark.sql(f"select count(1) as total from {tbl_wrk_api_birads}").display()

# COMMAND ----------

# DBTITLE 1,Exclui por Nome do Paciente
spark.sql(f"""
  MERGE INTO {tbl_wrk_api_birads} AS target
  USING (
    select
      pacs.idAtendimento,
      pacs.idHospital,
      pacs.Unidade,
      pacs.Regional,
      pacs.CNPJ,
      pacs.CPF,
      pacs.Paciente,
      pacs.resultados,

      api.id_exame,
      api.id_unidade,
      api.emp_nome_unidade,
      api.cnpjHospitalDecrypted,
      api.cpfPacienteDecrypted,
      api.nomePacienteDecrypted,
      api.proced_laudo_exame

    from {tbl_wrk_api_birads} as pacs

    inner join vwApi as api
      on pacs.Paciente = api.nomePacienteDecrypted

  ) AS source
  ON target.idAtendimento = source.idAtendimento
  WHEN MATCHED THEN DELETE
""").display()

# COMMAND ----------

spark.sql(f"select count(1) as total from {tbl_wrk_api_birads}").display()

# COMMAND ----------

spark.sql(f"select distinct idHospital, idUnidade, Unidade, CNPJ from {tbl_wrk_api_birads} where CNPJ is null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga dos dados

# COMMAND ----------

spark.sql(f"""
with cte as (
    select distinct
        dt_execucao,
        id_unidade,
        lpad(emp_cnpj_unidade, 14, '0') as emp_cnpj_unidade,
        emp_nome_unidade,
        emp_regional_unidade,
        ifnull(cod_origem, 'RC') as cod_origem
    from {tbl_modelo_rads_entrada}
    where emp_cnpj_unidade is not null
)
select * from cte
qualify row_number() over(partition by emp_cnpj_unidade order by cod_origem desc, dt_execucao desc) = 1
""").display()

# COMMAND ----------

spark.sql(f"""
create or replace temp view vwCnpj
as
with cte as (
    select distinct
        dt_execucao,
        id_unidade,
        lpad(emp_cnpj_unidade, 14, '0') as emp_cnpj_unidade,
        emp_nome_unidade,
        emp_regional_unidade,
        ifnull(cod_origem, 'RC') as cod_origem
    from {tbl_modelo_rads_entrada}
    where emp_cnpj_unidade is not null
)
select * from cte
qualify row_number() over(partition by emp_cnpj_unidade order by cod_origem desc, dt_execucao desc) = 1
""").display()

# COMMAND ----------

tbl_modelo_rads_entrada

# COMMAND ----------

spark.sql(f"delete from {tbl_modelo_rads_entrada} where dt_execucao = '{data_execucao_modelo}' and cod_origem = 'PACS'").display()

# COMMAND ----------

spark.sql(f"""
    insert into {tbl_modelo_rads_entrada}
    (
        dt_execucao,
        id_predicao,
        id_exame,
        num_pedido_integracao,
        id_unidade,
        emp_nome_unidade,
        emp_regional_unidade,
        emp_cnpj_unidade,
        num_porta_entrada,
        num_motivo_entrada,
        id_medico_encaminhador,
        nome_medico_encaminhador,
        doc_crm_medico_encaminhador,
        uf_crm_medico_encaminhador,
        tel_medico_encaminhador,
        id_paciente,
        nome_paciente,
        num_cpf_paciente,
        dt_nascimento_paciente,
        gen_sexo_paciente,
        tel_contato_paciente,
        cod_ans_convenio,
        nome_convenio,
        nome_plano,
        dt_exame,
        proced_nome_exame,
        proced_laudo_exame,
        cod_origem
    )
    select 
        '{data_execucao_modelo}' as dt_execucao,
        uuid() as id_predicao,
        pacs.idAtendimento as id_exame,
        pacs.idAtendimento as num_pedido_integracao,
        ifnull(vwCnpj.id_unidade, pacs.idHospital) as id_unidade,
        ifnull(vwCnpj.emp_nome_unidade, upper(pacs.Unidade)) as emp_nome_unidade,
        ifnull(vwCnpj.emp_regional_unidade, pacs.Regional) as emp_regional_unidade,
        ifnull(vwCnpj.emp_cnpj_unidade, if(trim(pacs.CNPJ) = '', null, trim(pacs.CNPJ))) as emp_cnpj_unidade,
        5 as num_porta_entrada,
        6 as num_motivo_entrada,
        null as id_medico_encaminhador,
        gold_corporativo.default.rdsl_encrypt(pacs.Solicitante, 1) as nome_medico_encaminhador,
        gold_corporativo.default.rdsl_encrypt(pacs.DocSolicitante, 1) as doc_crm_medico_encaminhador,
        pacs.UfDocSolicitante as uf_crm_medico_encaminhador,
        gold_corporativo.default.rdsl_encrypt(pacs.TelefoneSolicitante, 0) as tel_medico_encaminhador,
        null as id_paciente,
        gold_corporativo.default.rdsl_encrypt(pacs.Paciente, 1) as nome_paciente,
        gold_corporativo.default.rdsl_encrypt(pacs.CPF, 1) as num_cpf_paciente,
        date(pacs.Nascimento) as dt_nascimento_paciente,
        case pacs.SexoPaciente when 'F' then 'female' when 'M' then 'male' else 'NE' end as gen_sexo_paciente,
        gold_corporativo.default.rdsl_encrypt(pacs.TelefonePaciente, 0) as tel_contato_paciente,
        '' as cod_ans_convenio,
        'NÃO ENCONTRADO' as nome_convenio,
        '' as nome_plano,
        pacs.DataLiberacao as dt_exame,
        upper(trim(pacs.exame)) as proced_nome_exame,
        pacs.laudoLimpo as proced_laudo_exame,
        'PACS' as cod_origem

    from {tbl_wrk_api_birads} as pacs

    left join vwCnpj
        on pacs.CNPJ = vwCnpj.emp_cnpj_unidade

    where pacs.idAtendimento not in (select id_exame from {tbl_modelo_rads_entrada})
""").display()

# COMMAND ----------

optimize_table(tbl_modelo_rads_entrada)

# COMMAND ----------

spark.sql(f"""
select
    dt_execucao,
    count(*) as total,
    count_if(ifnull(cod_origem,'RC')='RC') as qtdRC,
    count_if(cod_origem='PACS') as qtdPACS
from {tbl_modelo_rads_entrada}
group by all
order by dt_execucao desc
""").display()

# COMMAND ----------

spark.sql(f"""
    select distinct
        emp_cnpj_unidade as emp_cnpj_unidade,
        len(emp_cnpj_unidade) as len_emp_cnpj_unidade,
        cod_origem,
        emp_nome_unidade
    from {tbl_modelo_rads_entrada}
    where dt_execucao = '{data_execucao_modelo}'
""").display()

# COMMAND ----------


