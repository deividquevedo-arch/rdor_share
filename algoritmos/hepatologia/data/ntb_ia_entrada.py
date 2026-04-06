# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# DBTITLE 1,import de libs
from datetime import datetime, timedelta

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

# DBTITLE 1,Define variáveis com os nomes das tabelas
tb_wrk_exame = f"{work_catalog}.{environment_tbl}tb_diamond_wrk_hepatologia_exame"
view_name_hepato_entr = f"{environment_tbl}vw_temp_hepatologia_entrada"
table_name_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_entrada"

# COMMAND ----------

# DBTITLE 0,Exibe valores das variáveis com nomes das tabelas
print(f"{'tb_wrk_exame':25}: {tb_wrk_exame}")
print(f"{'view_name_hepato_entr':25}: {view_name_hepato_entr}")
print(f"{'table_name_entrada':25}: {table_name_entrada}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas Work

# COMMAND ----------

# DBTITLE 1,Definine data inicial da carga
df = spark.sql(f"SELECT COUNT(1) FROM {table_name_entrada} WHERE dt_execucao < '{data_execucao_modelo}'")
count = df.collect()[0][0]

if count > 0:
    current_date = datetime.strptime(data_execucao_modelo, "%Y-%m-%d").date()
    seven_days_ago = current_date - timedelta(days=7)
    initial_date = seven_days_ago.strftime("%Y-%m-%d")
else:
    initial_date = '2024-01-01'

print(f"initial_date: {initial_date}")

# COMMAND ----------

# DBTITLE 1,Work Exames
spark.sql(f"""
    CREATE OR REPLACE TABLE {tb_wrk_exame}
    LOCATION '{table_location(tb_wrk_exame)}'
    AS
    WITH cte_exame AS (
        SELECT
            *,
            greatest(
                if(to_date(dt_issued) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_issued), null),
                if(to_date(dt_entrega_laudo) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_entrega_laudo), null),
                if(to_date(dt_pedido) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_pedido), null),
                if(to_date(dt_liberacao_laudo) <= to_date('{data_execucao_modelo}'), to_timestamp(dt_liberacao_laudo), null)
            ) as dt_exame_calculado
            
        FROM gold_corporativo_ia.corporativo.tb_gold_mov_exame

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                id_exame
            ORDER BY
                etl_data_atualizacao_carga_gold DESC
        ) = 1
    )
    SELECT
        exam.*,
        explode(exam.proced_lista_exames) as exploded
    FROM cte_exame as exam
    WHERE dt_exame_calculado >= DATE('{initial_date}')
      AND dt_exame_calculado <= DATE('{data_execucao_modelo}')

""").display()

# COMMAND ----------

optimize_table(tb_wrk_exame)

# COMMAND ----------

spark.sql(f"select count(1) as total from {tb_wrk_exame}").display()

# COMMAND ----------

# DBTITLE 1,Remove laudos em HTML
spark.sql(f"""
    delete from {tb_wrk_exame}
    where (
           contains(exploded.laudo_transformado, 'align=') = true
        or contains(exploded.laudo_transformado, 'class=') = true
        or contains(exploded.laudo_transformado, 'style=') = true
        or contains(exploded.laudo_transformado, 'href=') = true
        or contains(exploded.laudo_transformado, '/p>') = true
        or contains(exploded.laudo_transformado, '/strong>') = true
        or contains(exploded.laudo_transformado, 'data-wate-document=') = true
    )
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # View Entrada Modelo Hepatologia

# COMMAND ----------

# DBTITLE 1,View entrada
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW {view_name_hepato_entr}
    AS 
    SELECT
        exam_alias.id_exame as idExame,
        exam_alias.dt_exame_calculado as dataExame,
        exploded.nme_exame as tipoExame,
        exam_alias.nme_convenio as nomeConvenio,
        exploded.laudo_transformado as laudoExameOriginal,
        exam_alias.id_unidade as idHospital,
        exam_alias.nme_hospital as nomehospital,
        exam_alias.cnpj_hospital as cnpjHospital,
        exam_alias.nme_regional_hospital as regionalHospital,
        exam_alias.cod_medico_encaminhador as idMedico,
        exam_alias.reg_crm_medico_encaminhador as numCrm,
        exam_alias.reg_uf_crm_medico_encaminhador as ufCrm,
        exam_alias.nme_medico_encaminhador as nomeMedico,
        exam_alias.id_patient as idPaciente,
        patient.doc_cpf as cpfPaciente,
        patient.cli_nome as nomePaciente,
        patient.cli_data_nascimento as dataNascimentoPaciente,
        patient.cli_idade as idadePaciente,
        case
            when lower(trim(patient.cli_genero)) = 'male' then 'M'
            when lower(trim(patient.cli_genero)) = 'female' then 'F'
        else null
        end as sexoPaciente,

        patient.cli_telefone_numero as telefoneContato,

        patient.dt_exame_coleta_plt as dataColetaPlt,
        patient.vl_exame_PLT as ValorPlt,
        datediff(patient.dt_exame_coleta_plt, exam_alias.dt_exame_calculado) as difTempoImagemPlt,

        exam_alias.nme_codigo as codigo,
        exam_alias.tp_codigo as tipoCodigo,
        exam_alias.dsc_codigo as nomeCodigo,

        exam_alias.dsc_procedimento_integracao as descricaoProcedimento,
        exam_alias.proced_descricao_ajustado as descricaoProcedimentoLimpo,

        row_number() over(
            partition by
                exam_alias.id_patient,
                exam_alias.dt_exame_calculado,
                regexp_replace(exploded.laudo_transformado, '[^a-zA-Z0-9]', '')
            order by
                exam_alias.dt_exame_calculado,
                exploded.nme_exame,
                exam_alias.id_exame
        ) as laudoDuplicado

    FROM {tb_wrk_exame} as exam_alias

    LEFT JOIN gold_corporativo_ia.corporativo.tb_gold_mov_paciente as patient
        ON exam_alias.id_patient = patient.id_paciente

    WHERE (
           IFNULL(exploded.nme_exame, '') ilike '%figado%'
        OR IFNULL(exploded.nme_exame, '') ilike '%fígado%'
        OR IFNULL(exploded.nme_exame, '') ilike '%abd%'
        OR IFNULL(exploded.nme_exame, '') ilike '%bdo%'
        OR exam_alias.dsc_codigo ilike '%figado%'
        OR exam_alias.dsc_codigo ilike '%fígado%'
        OR exam_alias.dsc_codigo ilike '%abd%'
        OR exam_alias.dsc_codigo ilike '%bdo%'
        OR exam_alias.dsc_procedimento_integracao ilike '%figado%'
        OR exam_alias.dsc_procedimento_integracao ilike '%fígado%'
        OR exam_alias.dsc_procedimento_integracao ilike '%abd%'
        OR exam_alias.dsc_procedimento_integracao ilike '%bdo%'
        OR exam_alias.proced_descricao_ajustado ilike '%figado%'
        OR exam_alias.proced_descricao_ajustado ilike '%fígado%'
        OR exam_alias.proced_descricao_ajustado ilike '%abd%'
        OR exam_alias.proced_descricao_ajustado ilike '%bdo%'
        OR IFNULL(exploded.laudo_transformado, '') ilike '%figado%'
        OR IFNULL(exploded.laudo_transformado, '') ilike '%fígado%'
        OR IFNULL(exploded.laudo_transformado, '') ilike '%abd%'
        OR IFNULL(exploded.laudo_transformado, '') ilike '%bdo%'
    )
    AND TRIM(IFNULL(exploded.laudo_transformado, '')) != ''
    AND CONTAINS(exploded.laudo_transformado, '\u0000') = FALSE
    
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega Tabela Entrada Modelo Hepatologia

# COMMAND ----------

# DBTITLE 1,delete from
spark.sql(f"""
    delete from {table_name_entrada}
    where dt_execucao = date('{data_execucao_modelo}')
""").display()

# COMMAND ----------

# DBTITLE 1,verificando nome das tbls
print(table_name_entrada)
print(view_name_hepato_entr)

# COMMAND ----------

# DBTITLE 1,insert into
spark.sql(f"""
    insert into {table_name_entrada}
    (
        dt_execucao,
        id_predicao,
        id_exame,
        dt_exame,
        proced_nome_exame,
        nome_convenio,
        proced_laudo_exame_original,
        id_unidade,
        emp_nome_unidade,
        emp_cnpj_unidade,
        emp_regional_unidade,
        id_medico,
        doc_crm_medico,
        uf_crm_medico,
        nome_medico,
        id_paciente,
        num_cpf_paciente,
        nome_paciente,
        dt_nascimento_paciente,
        num_idade_paciente,
        gen_sexo_paciente,
        tel_contato_paciente,
        dt_coleta_plt,
        vl_plt,
        num_dif_tempo_imagem_plt,
        fl_laudo_duplicado,
        cod_procedimento,
        tp_codigo_procedimento,
        proced_nome_procedimento,
        etl_data_carga
    )
    select
        date('{data_execucao_modelo}') as dt_execucao,
        uuid()                         as id_predicao,
        idExame                        as id_exame,
        dataExame                      as dt_exame,
        tipoExame                      as proced_nome_exame,
        nomeConvenio                   as nome_convenio,
        laudoExameOriginal             as proced_laudo_exame_original,
        idHospital                     as id_unidade,
        nomeHospital                   as emp_nome_unidade,
        cnpjHospital                   as emp_cnpj_unidade,
        regionalHospital               as emp_regional_unidade,
        idMedico                       as id_medico,
        numCrm                         as doc_crm_medico,
        ufCrm                          as uf_crm_medico,
        nomeMedico                     as nome_medico,
        idPaciente                     as id_paciente,
        cpfPaciente                    as num_cpf_paciente,
        nomePaciente                   as nome_paciente,
        dataNascimentoPaciente         as dt_nascimento_paciente,
        idadePaciente                  as num_idade_paciente,
        sexoPaciente                   as gen_sexo_paciente,
        telefoneContato                as tel_contato_paciente,
        dataColetaPlt                  as dt_coleta_plt,
        valorPlt                       as vl_plt,
        difTempoImagemPlt              as num_dif_tempo_imagem_plt,
        laudoDuplicado                 as fl_laudo_duplicado,
        codigo                         as cod_procedimento,
        tipoCodigo                     as tp_codigo_procedimento,
        nomeCodigo                     as proced_nome_procedimento,
        current_timestamp()            as etl_data_carga

    from {view_name_hepato_entr}

    where idExame not in (
          select id_exame from {table_name_entrada}
      )
""").display()

# COMMAND ----------

# DBTITLE 1,otimização da tbl
optimize_table(f"{table_name_entrada}")

# COMMAND ----------

# DBTITLE 1,Fim da execução!
dbutils.notebook.exit("Fim da execução!")

# COMMAND ----------


