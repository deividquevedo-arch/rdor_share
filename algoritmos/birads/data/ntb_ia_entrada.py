# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

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
# MAGIC # Tabelas Work

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variáveis com os nomes das tabelas

# COMMAND ----------

# DBTITLE 1,Define variáveis com os nomes das tabelas
table_name_wrk_exame = f"{work_catalog}.{environment_tbl}tb_diamond_wrk_birads_exame"
table_name_wrk_exame_mama = f"{table_name_wrk_exame}_mama"
table_name_wrk_exame_mama_paciente = f"{table_name_wrk_exame_mama}_paciente"

table_name_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_entrada"

# COMMAND ----------

# DBTITLE 1,Exibe valores das variáveis com nomes das tabelas
print(f"{'table_name_wrk_exame':40}: {table_name_wrk_exame}")
print(f"{'table_name_wrk_exame_mama':40}: {table_name_wrk_exame_mama}")
print(f"{'table_name_wrk_exame_mama_paciente':40}: {table_name_wrk_exame_mama_paciente}")

print(f"{'table_name_entrada':40}: {table_name_entrada}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### tb_wrk_birads_exame

# COMMAND ----------

table_name_wrk_exame

# COMMAND ----------

spark.sql(f"""
    create or replace table {table_name_wrk_exame}
    location '{table_location(table_name_wrk_exame)}'
    as
    with cte_exame as (
      select
        *,
        greatest(
          if(to_date(dt_issued) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_issued), null),
          if(to_date(dt_entrega_laudo) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_entrega_laudo), null),
          if(to_date(dt_pedido) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_pedido), null),
          if(to_date(dt_liberacao_laudo) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_liberacao_laudo), null)
        ) as dataExameBirads
      -- from ia.tb_diamond_vis_exame
      from gold_corporativo_ia.corporativo.tb_gold_mov_exame
    )
    select * from cte_exame
    where true
      and date(dataExameBirads) between date('{data_execucao_modelo}') - 30 and date_sub(date('{data_execucao_modelo}'),1)       
      and upper(trim(tp_procedimento)) IN  ('IMG', 'IMA')
      and id_unidade not ilike 'RICHET%'
    qualify row_number() over (partition by id_exame order by etl_data_atualizacao_carga_gold desc) = 1
""").display()

# COMMAND ----------

optimize_table(table_name_wrk_exame)

# COMMAND ----------

spark.sql(f"select count(1) from {table_name_wrk_exame}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### tb_wrk_birads_exame_mama

# COMMAND ----------

table_name_wrk_exame_mama

# COMMAND ----------

spark.sql(f"""
    create or replace table {table_name_wrk_exame_mama}
    location '{table_location(table_name_wrk_exame_mama)}'
    as
    with cte_mama as (
        select distinct
            exame.id_exame,
            dadosExame.nme_exame as nomeExame,
            dadosExame.laudo_transformado
        from {table_name_wrk_exame} as exame
        lateral view explode(proced_lista_exames) as dadosExame
        where (
               ifnull(exame.dsc_procedimento_integracao, '') RLIKE '(?i)mam[o|a|á]'
            or ifnull(exame.nme_codigo, '') RLIKE '(?i)mam[o|a|á]'
            or ifnull(exame.dsc_codigo, '') RLIKE '(?i)mam[o|a|á]'
            or ifnull(dadosExame.nme_exame, '') RLIKE '(?i)mam[o|a|á]'
        )
        and not ifnull(dadosExame.nme_exame, '') RLIKE '(?i)bi[o|ó]psia'
        and len(trim(ifnull(dadosExame.laudo_transformado, ''))) > 10
    )
    select
        a.*,
        case
            when ifnull(b.nomeExame, '') RLIKE '(?i)mam[o|a|á]'
            then b.nomeExame

            when ifnull(a.nme_codigo, '') RLIKE '(?i)mam[o|a|á]'
            then a.nme_codigo

            when ifnull(a.dsc_codigo, '') RLIKE '(?i)mam[o|a|á]'
            then a.dsc_codigo
            
            else a.dsc_procedimento_integracao
        end as tituloExame,
        b.laudo_transformado
    from {table_name_wrk_exame} as a
    inner join cte_mama as b
        on a.id_exame = b.id_exame
    qualify row_number() over (partition by a.id_exame order by a.etl_data_atualizacao_carga_gold desc) = 1

""").display()

# COMMAND ----------

optimize_table(table_name_wrk_exame_mama)

# COMMAND ----------

spark.sql(f"select count(1) as qtd from {table_name_wrk_exame_mama}").display()

# COMMAND ----------

spark.sql(f"select id_exame, count(1) as qtd from {table_name_wrk_exame_mama} group by all having qtd > 1").display()

# COMMAND ----------

# DBTITLE 1,Remove laudos em HTML
# TODO: Verificar se será necessário excluir laudos em HTML

# spark.sql(f"""
#     delete from {table_name_wrk_exame_mama}
#     where (
#            contains(laudoTransformado, 'align=') = true
#         or contains(laudoTransformado, 'class=') = true
#         or contains(laudoTransformado, 'style=') = true
#         or contains(laudoTransformado, 'href=') = true
#         or contains(laudoTransformado, '/p>') = true
#         or contains(laudoTransformado, '/strong>') = true
#         or contains(laudoTransformado, 'data-wate-document=') = true
#     )
# """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### tb_diamond_mod_birads_entrada

# COMMAND ----------

table_name_entrada

# COMMAND ----------

# DBTITLE 1,DDL Tabela Entrada
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name_entrada} (
  dt_execucao DATE,
  id_predicao STRING,
  id_exame STRING,
  num_pedido_integracao STRING,
  id_unidade STRING,
  emp_nome_unidade STRING,
  emp_regional_unidade STRING,
  emp_cnpj_unidade STRING,
  num_porta_entrada INT,
  num_motivo_entrada INT,
  id_medico_encaminhador STRING,
  nome_medico_encaminhador STRING,
  doc_crm_medico_encaminhador STRING,
  uf_crm_medico_encaminhador STRING,
  tel_medico_encaminhador STRING,
  id_paciente STRING,
  nome_paciente STRING,
  num_cpf_paciente STRING,
  dt_nascimento_paciente TIMESTAMP,
  gen_sexo_paciente STRING,
  tel_contato_paciente STRING,
  cod_ans_convenio STRING,
  nome_convenio STRING,
  nome_plano STRING,
  dt_exame TIMESTAMP,
  proced_nome_exame STRING,
  proced_laudo_exame STRING,
  cod_origem STRING
) USING delta
PARTITIONED BY (dt_execucao)
LOCATION '{table_location(table_name_entrada)}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### tb_wrk_birads_exame_mama_paciente

# COMMAND ----------

spark.sql(f"""
    create or replace table {table_name_wrk_exame_mama_paciente}
    location '{table_location(table_name_wrk_exame_mama_paciente)}'
    as
    select
        exame.*,
        paciente.cli_nome as nomePaciente,
        paciente.doc_cpf as numCPF,
        paciente.cli_data_nascimento as dataNascimento,
        paciente.cli_genero as nomeGenero,
        paciente.cli_telefone_numero as telefoneContato
    from {table_name_wrk_exame_mama} as exame
    inner join gold_corporativo_ia.corporativo.tb_gold_mov_paciente as paciente
        on exame.id_patient = paciente.id_paciente
    where id_exame not in (select id_exame from {table_name_entrada} where dt_execucao != date('{data_execucao_modelo}'))
    qualify row_number() over (partition by exame.id_exame order by paciente.etl_data_atualizacao_carga_gold desc) = 1
""").display()

# COMMAND ----------

optimize_table(table_name_wrk_exame_mama_paciente)

# COMMAND ----------

spark.sql(f"select count(1) from {table_name_wrk_exame_mama_paciente}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela Entrada

# COMMAND ----------

table_name_entrada

# COMMAND ----------

# DBTITLE 1,Delete Tabela Entrada
spark.sql(f"delete from {table_name_entrada} where dt_execucao = '{data_execucao_modelo}'").display()

# COMMAND ----------

# DBTITLE 1,Insert Tabela Entrada
spark.sql(f"""
    insert into {table_name_entrada} (
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
        date('{data_execucao_modelo}') as dt_execucao,
        uuid()                         as id_predicao,
        id_exame                       as id_exame,
        num_pedido_integracao          as num_pedido_integracao,
        id_unidade                     as id_unidade,
        nme_hospital                   as emp_nome_unidade,
        nme_regional_hospital          as emp_regional_unidade,
        cnpj_hospital                  as emp_cnpj_unidade,
        5                              as num_porta_entrada,
        6                              as num_motivo_entrada,
        cod_medico_encaminhador        as id_medico_encaminhador,
        nme_medico_encaminhador        as nome_medico_encaminhador,
        reg_crm_medico_encaminhador    as doc_crm_medico_encaminhador,
        reg_uf_crm_medico_encaminhador as uf_crm_medico_encaminhador,
        tel_medico_encaminhador        as tel_medico_encaminhador,
        id_patient                     as id_paciente,
        nomePaciente                   as nome_paciente,
        numCPF                         as num_cpf_paciente,
        dataNascimento                 as dt_nascimento_paciente,
        nomeGenero                     as gen_sexo_paciente,
        telefoneContato                as tel_contato_paciente,
        cast(null as string)           as cod_ans_convenio,
        nme_convenio                   as nome_convenio,
        cast(null as string)           as nome_plano,
        dataExameBirads                as dt_exame,
        tituloExame                    as proced_nome_exame,
        laudo_transformado             as proced_laudo_exame,
        'RC'                           AS cod_origem

    from {table_name_wrk_exame_mama_paciente}
""").display()


# COMMAND ----------

optimize_table(table_name_entrada)

# COMMAND ----------

spark.sql(f"""
    select
        dt_execucao,
        count(1) as qtd
    from {table_name_entrada}
    group by all
    order by dt_execucao desc
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Fim do Processamento

# COMMAND ----------

dbutils.notebook.exit("Fim da execução!")

# COMMAND ----------


