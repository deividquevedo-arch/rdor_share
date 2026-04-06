# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #Parâmetros

# COMMAND ----------

# {
#   "catalog": "diamond_birads",
#   "data_execucao_modelo": "2025-06-23",
#   "environment": "dev",
#   "id_projeto": "birads",
#   "schema": "birads",
#   "work_catalog": "diamond_birads",
#   "work_schema": "workarea"
# }

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
# MAGIC **Flávio - 13/11/2024**
# MAGIC - Task: [#28394](https://dev.azure.com/RDOr-Corp/IA/_workitems/edit/28394)
# MAGIC
# MAGIC Parâmetro utilizado para determinar a partir de qual data a nova regra de duplicidade será aplicada.
# MAGIC
# MAGIC Conforme descrito na task, a regra de duplicidade foi alterada para considerar somente um paciente por dia.
# MAGIC
# MAGIC Esse parâmetro foi criado para não alterar a quantidade de registros enviados no passado, utilizando a regra antiga.
# MAGIC
# MAGIC Como a tabela é recriada diariamente, caso isso não fosse feito o número de registros enviados no passado seria alterado nos relatórios de histórico de envios.

# COMMAND ----------

file = "/mnt/trusted/datalake/ia/projetos/api-birads/config/hml/configCorrecaoDuplicidade.json"
current_date = datetime.today().date()

try:
  df = spark.read.option("multiline","true").json(file)
  for row in df.collect():
    data_ajuste_duplicidade = datetime.strptime(row["dataCorrecaoDuplicidade"], "%Y-%m-%d").date()

except Exception as error:
  print(f"ERRO: {error}")
  print("Usando data de ajuste padrão!")
  data_ajuste_duplicidade = datetime.today().date() + timedelta(days=1)

finally:
  print(f"Data de ajuste de duplicidade: {data_ajuste_duplicidade}")

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
# MAGIC # Tabelas

# COMMAND ----------

tb_wrk_birads_complemento_tmp1 = f"{work_catalog}.{environment_tbl}tb_diamond_wrk_birads_complemento_tmp1"

tb_mod_birads_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_entrada"
tb_mod_birads_saida = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_saida"
tb_mod_birads_complemento = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_complemento"

vw_diamond_mod_birads_complemento = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_complemento"
vw_diamond_mod_birads_inconsistencias_total = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_inconsistencias_total"
vw_diamond_mod_birads_inconsistencias = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_inconsistencias"

# COMMAND ----------

print(f"{'tb_wrk_birads_complemento_tmp1':45}: {tb_wrk_birads_complemento_tmp1}")

print(f"{'tb_mod_birads_entrada':45}: {tb_mod_birads_entrada}")
print(f"{'tb_mod_birads_saida':45}: {tb_mod_birads_saida}")
print(f"{'tb_mod_birads_complemento':45}: {tb_mod_birads_complemento}")

print(f"{'vw_diamond_mod_birads_complemento':45}: {vw_diamond_mod_birads_complemento}")
print(f"{'vw_diamond_mod_birads_inconsistencias_total':45}: {vw_diamond_mod_birads_inconsistencias_total}")
print(f"{'vw_diamond_mod_birads_inconsistencias':45}: {vw_diamond_mod_birads_inconsistencias}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Queries

# COMMAND ----------

tb_wrk_birads_complemento_tmp1

# COMMAND ----------

try:
    spark.sql(f"drop table if exists {tb_wrk_birads_complemento_tmp1}")
except Exception as e:
    print(e)

# COMMAND ----------

spark.sql(f"""
create or replace table {tb_wrk_birads_complemento_tmp1}
location '{table_location(tb_wrk_birads_complemento_tmp1)}'
partitioned by (dt_execucao)
as
select distinct 
    entr.id_predicao,
    entr.id_exame,
    entr.num_pedido_integracao,
    entr.dt_exame,
    entr.proced_nome_exame,
    entr.id_medico_encaminhador,
    if(trim(coalesce(doc_crm_medico_encaminhador, "")) != "", doc_crm_medico_encaminhador, gold_corporativo.default.rdsl_encrypt('-1', 1)) as doc_crm_medico_encaminhador,
    coalesce(uf_crm_medico_encaminhador, "NE") as uf_crm_medico_encaminhador,
    if(trim(ifnull(nome_medico_encaminhador, "")) != "", nome_medico_encaminhador, gold_corporativo.default.rdsl_encrypt('MÉDICO DESCONHECIDO', 1)) as nome_medico_encaminhador,    
    entr.tel_medico_encaminhador,
    entr.id_paciente,
    ifnull(entr.num_cpf_paciente, gold_corporativo.default.rdsl_encrypt('00000000000', 1)) as num_cpf_paciente,
    ifnull(entr.nome_paciente, gold_corporativo.default.rdsl_encrypt('NÃO ENCONTRADO', 1)) as nome_paciente,    
    ifnull(entr.dt_nascimento_paciente, date('1900-01-01')) as dt_nascimento_paciente,
    ifnull(entr.gen_sexo_paciente, 'NE') as gen_sexo_paciente,
    ifnull(entr.tel_contato_paciente, gold_corporativo.default.rdsl_encrypt('00000000000', 0)) as tel_contato_paciente,
    ifnull(entr.cod_ans_convenio, '') as cod_ans_convenio,
    ifnull(entr.nome_convenio, 'SEM CONVÊNIO') as nome_convenio,
    ifnull(entr.nome_plano, '') as nome_plano,
    entr.id_unidade,    
    ifnull(entr.emp_regional_unidade, 'NE') as emp_regional_unidade,
    ifnull(entr.emp_cnpj_unidade, '00000000000000') as emp_cnpj_unidade,
    ifnull(trim(entr.emp_nome_unidade), 'NÃO ENCONTRADO') as emp_nome_unidade,
    entr.num_porta_entrada,
    entr.num_motivo_entrada,
    ifnull(entr.cod_origem, 'RC') as cod_origem,
    sai.proced_laudo_limpo as proced_laudo_exame,
    sai.vl_proced_birads,
    ifnull(sai.fl_integracao_concluida, false) as fl_integracao_concluida,
    sai.dt_execucao as dt_execucao,
    row_number() over(
      partition by
        sai.dt_execucao,
        entr.nome_paciente_decrypted
        {"" if data_ajuste_duplicidade <= current_date else ", sai.laudoExame"}
      order by
        sai.vl_proced_birads desc,
        entr.dt_exame
    ) as num_ranking_laudo

from(
   select
    *,
    gold_corporativo.default.rdsl_decrypt(nome_paciente, 1) as nome_paciente_decrypted
  from {tb_mod_birads_entrada}
) as entr

inner join {tb_mod_birads_saida} as sai
    on entr.id_predicao = sai.id_predicao

where len(ifnull(sai.proced_laudo_limpo,'')) > 10

""").display()

# COMMAND ----------

optimize_table(tb_wrk_birads_complemento_tmp1)

# COMMAND ----------

# DBTITLE 1,Tabela Auxiliar
spark.sql(f"""
create or replace table {tb_mod_birads_complemento}
location '{table_location(tb_mod_birads_complemento)}'
partitioned by (dt_execucao)
as
with cte_decrypt as (
  select 
    id_predicao,
    
    gold_corporativo.default.rdsl_decrypt(doc_crm_medico_encaminhador, 1) as doc_crm_medico_encaminhador,
    gold_corporativo.default.rdsl_decrypt(nome_medico_encaminhador, 1) as nome_medico_encaminhador,
    gold_corporativo.default.rdsl_decrypt(num_cpf_paciente, 1) as num_cpf_paciente,
    gold_corporativo.default.rdsl_decrypt(nome_paciente, 1) as nome_paciente,
    right(trim(replace(gold_corporativo.default.rdsl_decrypt(tel_medico_encaminhador, 0), ' ', '')), 11) as tel_medico_encaminhador,
    emp_regional_unidade,
    emp_cnpj_unidade
  from {tb_wrk_birads_complemento_tmp1}
)
,cte as (
  select
    tmp1.*,

    filter(concat(
      array(if(tmp1.vl_proced_birads is null, "vl_proced_birads null", null)),
      array(if(trim(ifnull(tmp1.proced_laudo_exame,'')) = '', "laudo null", null)),
      array(if(tmp1.emp_nome_unidade is null, "emp_nome_unidade null", null)),
      array(if(cte_decrypt.emp_cnpj_unidade is null, "emp_cnpj_unidade null", null)),
      array(if(cte_decrypt.emp_regional_unidade is null, "emp_regional_unidade null", null)),
      array(if(tmp1.nome_convenio is null, "nome_convenio null", null)),
      array(if(cte_decrypt.tel_medico_encaminhador is null, "tel_medico_encaminhador null", null)),
      array(if(tmp1.gen_sexo_paciente is null, "gen_sexo_paciente null", null)),
      array(if(tmp1.dt_nascimento_paciente is null, "dt_nascimento_paciente null", null)),
      array(if(cte_decrypt.nome_paciente is null, "nome_paciente null", null)),
      array(if(cte_decrypt.nome_medico_encaminhador is null, "nome_medico_encaminhador null", null)),
      array(if(tmp1.uf_crm_medico_encaminhador is null, "uf_crm_medico_encaminhador null", null)),
      array(if(cte_decrypt.doc_crm_medico_encaminhador is null, "doc_crm_medico_encaminhador null", null)),
      array(if(tmp1.id_exame is null, "id_exame null", null))
    ), x -> x IS NOT NULL) as audit_motivo_inconsistencia,

    filter(concat(
      array(if(tmp1.vl_proced_birads not in (-1,0,1,2,3,4,5,6), "vl_proced_birads inválido", null)),
      array(if(trim(ifnull(tmp1.proced_laudo_exame,'')) = '', "laudo não preenchido", null)),
      array(if(len(ifnull(tmp1.emp_nome_unidade,'')) > 250, "emp_nome_unidade com mais de 250 caracteres", null)),
      array(if(len(ifnull(cte_decrypt.emp_cnpj_unidade,'')) < 14, "emp_cnpj_unidade com menos de 14 caracteres", null)),
      array(if(len(ifnull(cte_decrypt.emp_cnpj_unidade,'')) > 14, "emp_cnpj_unidade com mais de 14 caracteres", null)),
      array(if(len(ifnull(cte_decrypt.emp_regional_unidade,'')) > 2, "emp_regional_unidade com mais de 2 caracteres", null)),
      array(if(len(ifnull(tmp1.nome_plano,'')) > 150, "nome_plano com mais de 150 caracteres", null)),
      array(if(len(ifnull(tmp1.nome_convenio,'')) > 250, "nome_convenio com mais de 250 caracteres", null)),
      array(if(len(ifnull(tmp1.cod_ans_convenio,'')) > 20, "cod_ans_convenio com mais de 20 caracteres", null)),
      array(if(len(ifnull(cte_decrypt.tel_medico_encaminhador,'')) > 11, "tel_medico_encaminhador com mais de 11 caracteres", null)),
      array(if(lower(tmp1.gen_sexo_paciente) not in ("female", "male", "ne"), "gen_sexo_paciente inválido", null)),
      array(if(len(ifnull(cte_decrypt.nome_paciente,'')) > 100, "nome_paciente com mais de 100 caracteres", null)),
      array(if(len(cte_decrypt.num_cpf_paciente) > 11, "num_cpf_paciente com mais de 11 caracteres", null)),
      array(if(len(cte_decrypt.nome_medico_encaminhador) > 100, "nome_medico_encaminhador com mais de 100 caracteres", null)),
      array(if(len(tmp1.uf_crm_medico_encaminhador) < 2, "uf_crm_medico_encaminhador com menos de 2 caracteres", null)),
      array(if(len(tmp1.uf_crm_medico_encaminhador) > 2, "uf_crm_medico_encaminhador com mais de 2 caracteres", null)),
      array(if(len(cte_decrypt.doc_crm_medico_encaminhador) > 25, "doc_crm_medico_encaminhador com mais de 25 caracteres", null)),
      array(if(len(tmp1.id_exame) > 100, "id_exame com mais de 100 caracteres", null))
    ), x -> x IS NOT NULL) as audit_motivo_recusado_api,
  
    filter(concat(
      array(if(cte_decrypt.doc_crm_medico_encaminhador = '-1', "doc_crm_medico_encaminhador default", null)),
      array(if(tmp1.uf_crm_medico_encaminhador = 'NE', "uf_crm_medico_encaminhador default", null)),
      array(if(cte_decrypt.nome_medico_encaminhador = 'MÉDICO DESCONHECIDO', "nome_medico_encaminhador default", null)),
      array(if(cte_decrypt.num_cpf_paciente = '00000000000', "num_cpf_paciente default", null)),
      array(if(cte_decrypt.nome_paciente = 'NÃO ENCONTRADO', "nome_paciente default", null)),
      array(if(tmp1.dt_nascimento_paciente = '1900-01-01', "dt_nascimento_paciente default", null)),
      array(if(tmp1.gen_sexo_paciente = 'NE', "gen_sexo_paciente default", null)),
      array(if(cte_decrypt.tel_medico_encaminhador = '00000000000', "tel_medico_encaminhador default", null)),
      array(if(tmp1.nome_convenio = 'SEM CONVÊNIO', "nome_convenio default", null)),
      array(if(cte_decrypt.emp_regional_unidade = 'NE', "emp_regional_unidade default", null)),
      array(if(cte_decrypt.emp_cnpj_unidade = '00000000000000', "emp_cnpj_unidade default", null)),
      array(if(tmp1.emp_nome_unidade = 'NÃO ENCONTRADO', "emp_nome_unidade default", null))
  ), x -> x IS NOT NULL) as audit_valor_default,

  filter(concat(
      array(if(cte_decrypt.doc_crm_medico_encaminhador = '-1', "CRM", null)),
      array(if(cte_decrypt.num_cpf_paciente = '00000000000', "CPF Paciente", null)),
      array(if(cte_decrypt.nome_paciente = 'NÃO ENCONTRADO', "Nome Paciente", null)),
      array(if(cte_decrypt.emp_regional_unidade = 'NE', "Regional", null))
  ), x -> x IS NOT NULL) as audit_motivo_inconsistencia_negocio

  from {tb_wrk_birads_complemento_tmp1} as tmp1

  inner join cte_decrypt
    on  tmp1.id_predicao = cte_decrypt.id_predicao
)
select
  *,
  if(array_size(audit_motivo_inconsistencia) > 0, 1, 0) as fl_inconsistencias,
  array_size(audit_motivo_inconsistencia) as total_inconsistencias,
  if(array_size(audit_motivo_recusado_api) > 0, 1, 0) as fl_recusado_api,
  array_size(audit_motivo_recusado_api) as total_recusado_api,
  if(array_size(audit_valor_default) > 0, 1, 0) as fl_valor_default,
  array_size(audit_valor_default) as total_valor_default,
  if(array_size(audit_motivo_inconsistencia_negocio) > 0, 1, 0) as fl_inconsistencia_negocio,
  array_size(audit_motivo_inconsistencia_negocio) as total_inconsistencia_negocio
from cte
""").display()

# COMMAND ----------

optimize_table(tb_mod_birads_complemento)

# COMMAND ----------

spark.sql(f"""
    select dt_execucao, count(*) as qtd
    from {tb_mod_birads_complemento}
    where num_ranking_laudo = 1
    group by all
    order by dt_execucao desc;
""").display()

# COMMAND ----------

vw_diamond_mod_birads_complemento

# COMMAND ----------

# DBTITLE 1,View que será consumida pela API
spark.sql(f"""
create or replace view {vw_diamond_mod_birads_complemento}
as
with cte as (
    select 
        id_predicao
        ,id_medico_encaminhador
        ,gold_corporativo.default.rdsl_decrypt(doc_crm_medico_encaminhador, 1) as doc_crm_medico_encaminhador
        ,uf_crm_medico_encaminhador
        ,gold_corporativo.default.rdsl_decrypt(nome_medico_encaminhador, 1) as nome_medico_encaminhador
        ,right(trim(replace(gold_corporativo.default.rdsl_decrypt(tel_medico_encaminhador, 0), ' ', '')), 11) as tel_medico_encaminhador
        ,id_paciente
        ,gold_corporativo.default.rdsl_decrypt(num_cpf_paciente, 1) as num_cpf_paciente
        ,gold_corporativo.default.rdsl_decrypt(nome_paciente, 1) as nome_paciente
        ,dt_nascimento_paciente
        ,gen_sexo_paciente
        ,right(trim(replace(gold_corporativo.default.rdsl_decrypt(tel_contato_paciente, 0), ' ', '')), 11) as tel_contato_paciente
        ,cod_ans_convenio
        ,nome_convenio
        ,nome_plano
        ,id_unidade
        ,emp_regional_unidade
        ,lpad(emp_cnpj_unidade, 14, '0') as emp_cnpj_unidade
        ,emp_nome_unidade as emp_nome_unidade
        ,num_porta_entrada
        ,num_motivo_entrada
        ,cod_origem
        ,id_exame
        ,num_pedido_integracao
        ,proced_nome_exame
        ,proced_laudo_exame
        ,vl_proced_birads
        ,dt_exame
        ,fl_integracao_concluida
        ,dt_execucao
        ,audit_motivo_inconsistencia_negocio
        ,fl_inconsistencia_negocio
        ,total_inconsistencia_negocio

    from {tb_mod_birads_complemento}

    where num_ranking_laudo = 1
)
select * from cte
where true
and dt_nascimento_paciente is not null
and nome_convenio is not null
and gen_sexo_paciente is not null
and nome_paciente is not null
and tel_contato_paciente is not null
and emp_nome_unidade is not null
and emp_regional_unidade is not null
and emp_cnpj_unidade is not null
""").display()

# COMMAND ----------

# spark.sql(f"select * from {vw_diamond_mod_birads_complemento}").display()

# COMMAND ----------

vw_diamond_mod_birads_inconsistencias_total

# COMMAND ----------

# DBTITLE 1,View com todas as inconsistências
spark.sql(f"""
create or replace view {vw_diamond_mod_birads_inconsistencias_total}
as
select 
        id_predicao
        ,id_medico_encaminhador
        ,gold_corporativo.default.rdsl_decrypt(doc_crm_medico_encaminhador, 1) as doc_crm_medico_encaminhador
        ,uf_crm_medico_encaminhador
        ,gold_corporativo.default.rdsl_decrypt(nome_medico_encaminhador, 1) as nome_medico_encaminhador
        ,gold_corporativo.default.rdsl_decrypt(tel_medico_encaminhador, 0) as tel_medico_encaminhador
        ,id_paciente
        ,gold_corporativo.default.rdsl_decrypt(num_cpf_paciente, 1) as num_cpf_paciente
        ,gold_corporativo.default.rdsl_decrypt(nome_paciente, 1) as nome_paciente
        ,dt_nascimento_paciente
        ,gen_sexo_paciente
        ,gold_corporativo.default.rdsl_decrypt(tel_contato_paciente, 0) as tel_contato_paciente
        ,cod_ans_convenio
        ,nome_convenio
        ,nome_plano
        ,id_unidade
        ,emp_regional_unidade
        ,lpad(emp_cnpj_unidade, 14, '0') as emp_cnpj_unidade
        ,emp_nome_unidade as emp_nome_unidade
        ,num_porta_entrada
        ,num_motivo_entrada
        ,cod_origem
        ,id_exame
        ,num_pedido_integracao
        ,proced_nome_exame
        ,proced_laudo_exame
        ,vl_proced_birads
        ,dt_exame
        ,fl_integracao_concluida
        ,dt_execucao
        ,num_ranking_laudo
        ,audit_motivo_inconsistencia_negocio
        ,fl_inconsistencia_negocio
        ,total_inconsistencia_negocio

from {tb_mod_birads_complemento}
""").display()

# COMMAND ----------

# spark.sql(f"select * from {vw_diamond_mod_birads_inconsistencias_total}").display()

# COMMAND ----------

# DBTITLE 1,View com inconsistências rankingLaudo = 1
spark.sql(f"""
create or replace view {vw_diamond_mod_birads_inconsistencias}
as
select * from {vw_diamond_mod_birads_inconsistencias_total}
where num_ranking_laudo = 1
""").display()

# COMMAND ----------

spark.sql(f"""
    select
        dt_execucao,
        count(1) as qtd_total,
        count_if(cod_origem='RC') as qtd_rc,
        count_if(cod_origem='PACS') as qtd_pacs
    from  {vw_diamond_mod_birads_complemento}
    group by all
    order by
        dt_execucao desc
""").display()

# COMMAND ----------

dbutils.notebook.exit(0)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamond_birads.workarea.dev_vw_diamond_mod_birads_complemento

# COMMAND ----------


