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
# MAGIC # DDL das tabelas e views do modelo de hepatologia

# COMMAND ----------

# MAGIC %md
# MAGIC ## tb_diamond_mod_hepatologia_entrada

# COMMAND ----------

# DBTITLE 1,Define variável com o nome da tabela
table_name_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_entrada"
print(table_name_entrada)

# COMMAND ----------

# DBTITLE 1,DDL
spark.sql(f"""
    create table if not exists {table_name_entrada} 
    (
        dt_execucao date,
        id_predicao string,
        id_exame string,
        dt_exame string,
        proced_nome_exame string,
        nome_convenio string,
        proced_laudo_exame_original string,
        id_unidade string,
        emp_nome_unidade string,
        emp_cnpj_unidade string,
        emp_regional_unidade string,
        id_medico string,
        doc_crm_medico string,
        uf_crm_medico string,
        nome_medico string,
        id_paciente string,
        num_cpf_paciente string,
        nome_paciente string,
        dt_nascimento_paciente date,
        num_idade_paciente int,
        gen_sexo_paciente string,
        tel_contato_paciente string,
        dt_coleta_plt date,    
        vl_plt float,
        num_dif_tempo_imagem_plt int,
        etl_data_carga timestamp,
        fl_laudo_duplicado integer,
        cod_procedimento string,
        tp_codigo_procedimento string,
        proced_nome_procedimento string
    )
    partitioned by (dt_execucao)
    location '{table_location(table_name_entrada)}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_diamond_mod_hepatologia_saida

# COMMAND ----------

# DBTITLE 1,Define variável com o nome da tabela
table_name_saida = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_saida"
print(table_name_saida)

# COMMAND ----------

# DBTITLE 1,DDL
spark.sql(f"""
    create table if not exists {table_name_saida}
    (
        dt_execucao date,
        id_predicao string,
        id_exame string,
        id_paciente string,
        proced_laudo_exame string,
        proced_laudo_tokens array<string>,
        analise_blocos array<
            struct<
                bloco: string,
                orgaos_encontrados: array<string>,
                palavras_chave: array<string>,
                problemas_encontrados: array<
                    struct<
                        anteriores: string,
                        palavras_irrelevantes: array<string>,
                        posteriores: string,
                        problema: string,
                        relevante: boolean
                    >
                >,
                relevante: boolean
            >
        >,
        fl_relevante boolean
    )
    partitioned by (dt_execucao)
    location '{table_location(table_name_saida)}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_diamond_mod_hepatologia

# COMMAND ----------

# DBTITLE 1,Define variável com o nome da view
view_name = f"{main_catalog}.{environment_tbl}vw_diamond_mod_hepatologia"
print(view_name)

# COMMAND ----------

# DBTITLE 1,DDL
spark.sql(f"""
    create or replace view {view_name}
    as
    select
        ent.dt_execucao,
        ent.id_predicao,
        ent.id_exame,
        ent.id_unidade,
        ent.emp_nome_unidade,
        ent.emp_cnpj_unidade,
        ent.emp_regional_unidade,
        ent.id_medico,
        gold_corporativo.default.rdsl_decrypt(ent.doc_crm_medico, 1) as doc_crm_medico,
        ent.uf_crm_medico,
        gold_corporativo.default.rdsl_decrypt(ent.nome_medico, 1) as nome_medico,
        ent.id_paciente,
        gold_corporativo.default.rdsl_decrypt(ent.num_cpf_paciente, 1) as num_cpf_paciente,
        gold_corporativo.default.rdsl_decrypt(ent.nome_paciente, 1) as nome_paciente,
        ent.dt_nascimento_paciente,
        ent.num_idade_paciente,
        ent.gen_sexo_paciente,
        gold_corporativo.default.rdsl_decrypt(ent.tel_contato_paciente, 0) as tel_contato_paciente,
        ent.dt_exame,
        ent.proced_nome_exame,
        ent.nome_convenio,
        ent.cod_procedimento,
        ent.tp_codigo_procedimento,
        ent.proced_nome_procedimento,
        ent.proced_laudo_exame_original,
        sai.proced_laudo_exame,
        ent.dt_coleta_plt,
        ent.vl_plt,
        ent.num_dif_tempo_imagem_plt,
        sai.fl_relevante

    from {table_name_entrada} ent

    inner join {table_name_saida} sai
        on ent.id_predicao = sai.id_predicao

    where proced_nome_exame not like '%DOPPLER COLORIDO VEIA CAVA INFERIOR%'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_diamond_mod_hepatologia_retorno

# COMMAND ----------

# DBTITLE 1,Define variável com o nome da tabela
table_name_retorno = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_retorno"
table_name_retorno_hist = f"{table_name_retorno}_historico"
print(table_name_retorno)
print(table_name_retorno_hist)

# COMMAND ----------

# DBTITLE 1,DDL
spark.sql(f"""
    create table if not exists {table_name_retorno}
    (
        id_retorno string,
        dt_execucao date,
        id_predicao string,
        id_exame string,
        ts_retorno timestamp,
        ts_atualizacao timestamp,
        cod_arquivo_retorno string,
        cod_achado_relevante string,
        nome_achado string,
        cod_linha_cuidado string,
        obs_achado string,
        cod_destino string,
        cod_prioridade string
    )
    partitioned by (dt_execucao)
    location '{table_location(table_name_retorno)}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_diamond_mod_hepatologia_analise_retorno

# COMMAND ----------

# DBTITLE 1,Define variável com o nome da view
view_name_analise_retorno = f"{main_catalog}.{environment_tbl}vw_diamond_mod_hepatologia_analise_retorno"
print(view_name_analise_retorno)

# COMMAND ----------

# DBTITLE 1,DDL
spark.sql(f"""
    create or replace view {view_name_analise_retorno}
    as
    select
        entr.dt_execucao,
        entr.id_predicao,
        entr.id_exame,
        entr.id_unidade,
        entr.emp_nome_unidade,
        entr.emp_cnpj_unidade,
        entr.emp_regional_unidade,
        entr.id_medico,
        gold_corporativo.default.rdsl_decrypt(entr.doc_crm_medico, 1) as doc_crm_medico,
        entr.uf_crm_medico,
        gold_corporativo.default.rdsl_decrypt(entr.nome_medico, 1) as nome_medico,
        entr.id_paciente,
        gold_corporativo.default.rdsl_decrypt(entr.num_cpf_paciente, 1) as num_cpf_paciente,
        gold_corporativo.default.rdsl_decrypt(entr.nome_paciente, 1) as nome_paciente,
        entr.dt_nascimento_paciente,
        entr.num_idade_paciente,
        entr.gen_sexo_paciente,
        gold_corporativo.default.rdsl_decrypt(entr.tel_contato_paciente, 0) as tel_contato_paciente,
        entr.dt_exame,
        entr.proced_nome_exame,
        entr.nome_convenio,
        entr.proced_laudo_exame_original,
        entr.dt_coleta_plt,
        entr.vl_plt,
        entr.num_dif_tempo_imagem_plt,
        entr.etl_data_carga,

        said.fl_relevante,

        reto.id_retorno,
        reto.ts_retorno,
        reto.ts_atualizacao,
        reto.cod_arquivo_retorno,
        reto.cod_achado_relevante,
        reto.nome_achado,
        reto.cod_linha_cuidado,
        reto.obs_achado,
        reto.cod_destino,
        reto.cod_prioridade,

        TRANSLATE(
            reto.obs_achado,
            'ÀÁÂÃÄÅàáâãäåÇçÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖòóôõöÙÚÛÜùúûüÝý',
            'AAAAAAaaaaaaCcEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuYy'
        ) as obs_achado_limpo,

        TRANSLATE(
            reto.cod_destino,
            'ÀÁÂÃÄÅàáâãäåÇçÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖòóôõöÙÚÛÜùúûüÝý',
            'AAAAAAaaaaaaCcEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuYy'
        ) as cod_destino_limpo,

        1 as fl_entrada,
        if(said.id_predicao is not null, 1, 0) as fl_saida,
        if(said.id_predicao is not null and said.fl_relevante = True, 1, 0) as fl_enviado,
        if(reto.id_retorno is not null, 1, 0) as fl_retorno,
        if(reto.id_retorno is not null and reto.cod_achado_relevante is not null, 1, 0) as fl_rotulado,
        if(reto.id_retorno is not null and reto.cod_achado_relevante is not null and reto.cod_achado_relevante = '1 - Sim (Tem Doença Fígado)', 1, 0) as fl_sim_com_doenca_figado,
        if(reto.id_retorno is not null and reto.cod_achado_relevante is not null and reto.cod_achado_relevante = '2 - Sim (Mas Não Tem Doença Fígado)', 1, 0) as fl_sim_sem_doenca_figado,
        if(reto.id_retorno is not null and reto.cod_achado_relevante is not null and reto.cod_achado_relevante in ('1 - Sim (Tem Doença Fígado)', '2 - Sim (Mas Não Tem Doença Fígado)'), 1, 0) as fl_total_sim,
        if(reto.id_retorno is not null and reto.cod_achado_relevante is not null and reto.cod_achado_relevante = '3 - Não', 1, 0) as fl_total_nao,
        if(reto.id_retorno is not null and (reto.obs_achado ilike '%naveg%' or reto.cod_destino ilike '%navega%'), 1, 0) as fl_navegacao

    from {table_name_entrada} as entr

    left join {table_name_saida} as said
        on entr.id_predicao = said.id_predicao

    left join {table_name_retorno} as reto
        on said.id_predicao = reto.id_predicao
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_diamond_mod_hepatologia_navegacao

# COMMAND ----------

# DBTITLE 1,Define variável com o nome da tabela
table_name_navegacao = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_navegacao"
print(table_name_navegacao)

# COMMAND ----------

# DBTITLE 1,DDL
spark.sql(f"""
    create table if not exists {table_name_navegacao}
    (
        id_navegacao string,
        dt_enviado_navegacao date,
        dt_execucao date,
        id_predicao string,
        id_exame string,
        dt_exame timestamp,
        proced_nome_exame string,
        nome_convenio string,
        id_unidade string,
        emp_nome_unidade string,
        emp_nome_unidade_tratado string,
        emp_cnpj_unidade string,
        emp_regional_unidade string,
        id_medico string,
        doc_crm_medico string,
        uf_crm_medico string,
        nome_medico string,
        id_paciente string,
        nome_paciente string,
        num_cpf_paciente string,
        num_idade_paciente INT,
        tel_contato_paciente string,
        id_retorno string,
        cod_achado_relevante string,
        nome_achado string,
        cod_linha_cuidado string,
        obs_achado string,
        cod_destino string
    )
    location '{table_location(table_name_navegacao)}'
""").display()

# COMMAND ----------

# DBTITLE 1,Fim da execução!
dbutils.notebook.exit("Fim da execução!")

# COMMAND ----------


