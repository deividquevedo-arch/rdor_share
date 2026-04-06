# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import time

from datetime import datetime, timedelta

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

# DBTITLE 1,Data Update Log
data_exec_tmp = datetime.strptime(data_execucao_modelo, "%Y-%m-%d")
data_update_log = (data_exec_tmp - timedelta(days=30)).strftime("%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,Exibe variáveis
def print_params():
    print(f"{'id_projeto':25}: {id_projeto}")
    print(f"{'environment':25}: {environment}")
    print(f"{'environment_tbl':25}: {environment_tbl}")
    print(f"{'catalog_name':25}: {catalog_name}")
    print(f"{'schema_name':25}: {schema_name}")
    print(f"{'work_catalog_name':25}: {work_catalog_name}")
    print(f"{'work_schema_name':25}: {work_schema_name}")
    print(f"{'data_execucao_modelo':25}: {data_execucao_modelo}")
    print(f"{'main_catalog':25}: {main_catalog}")
    print(f"{'work_catalog':25}: {work_catalog}")
    print(f"{'root_folder':25}: {root_folder}")
    print(f"{'data_update_log':25}: {data_update_log}")

print_params()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas

# COMMAND ----------

tbl_gold_modelo_rads_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_entrada"
tbl_gold_modelo_birads_saida = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_saida"
tbl_temp_modelo_birads_complemento_v2 = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_complemento"
vw_gold_modelo_birads_complemento = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_complemento"
tbl_temp_gold_modelo_birads_complemento_v2_lake2 = f"ia.{environment_tbl}tbl_gold_modelo_birads_complemento_v2_lake2"

# COMMAND ----------

print(f"{'tbl_gold_modelo_rads_entrada':45}: {tbl_gold_modelo_rads_entrada}")
print(f"{'tbl_gold_modelo_birads_saida':45}: {tbl_gold_modelo_birads_saida}")
print(f"{'tbl_temp_modelo_birads_complemento_v2':45}: {tbl_temp_modelo_birads_complemento_v2}")
print(f"{'vw_gold_modelo_birads_complemento':45}: {vw_gold_modelo_birads_complemento}")
print(f"{'tbl_temp_gold_modelo_birads_complemento_v2_lake2':45}: {tbl_temp_gold_modelo_birads_complemento_v2_lake2}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

def update_tbl_saida():
    """Atualiza fl_integracao_concluida da tabela de saida"""

    print(f"Atualizando {tbl_gold_modelo_birads_saida}")

    spark.sql(f"""
        MERGE INTO {tbl_gold_modelo_birads_saida} AS target
        USING (
            SELECT DISTINCT
                idExame
            FROM logs.api_birads
            WHERE status_integracao = '200'
              AND DATE(data_hora) >= '{data_update_log}'
        ) AS source
            ON target.id_exame = source.idExame
        WHEN MATCHED THEN
            UPDATE SET target.fl_integracao_concluida = true
    """)

# COMMAND ----------

def hide_pii():
    """Remove dados pessoais"""

    print("Removendo PII")

    sql = f"""
        UPDATE logs.api_birads SET
            mensagem = REGEXP_REPLACE(
                        mensagem,
                        'NUM_CNPJ=[^,}}]*|STR_CONTATO=[^,}}]*|DAT_NASCIMENTO=[^,}}]*|STR_NOME_COMPLETO=[^,}}]*|NUM_CPF=[^,}}]*|STR_NOME_MEDICO=[^,}}]*|COD_CRM=[^,}}]*',
                        '[OMITIDO]'
                    )
        WHERE DATE(data_hora) >= '{data_update_log}'
    """

    try:
        spark.sql(sql)
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred.\n- Arguments: {ex.args}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualiza flgIntegracaoConcluida da tabela de complemento

# COMMAND ----------

def update_tbl_complemento():
    """Atualiza fl_integracao_concluida da tabela de complemento"""

    print(f"Atualizando {tbl_temp_modelo_birads_complemento_v2}")
    
    spark.sql(f"""
        MERGE INTO {tbl_temp_modelo_birads_complemento_v2} AS target
        USING (
            SELECT DISTINCT
                idExame
            FROM logs.api_birads
            WHERE status_integracao = '200'
              AND DATE(data_hora) >= '{data_update_log}'
        ) AS source
            ON target.id_exame = source.idExame
        WHEN MATCHED THEN
            UPDATE SET target.fl_integracao_concluida = true
    """)


    print(f"Atualizando {tbl_temp_gold_modelo_birads_complemento_v2_lake2}")
    
    spark.sql(f"""
        MERGE INTO {tbl_temp_gold_modelo_birads_complemento_v2_lake2} AS target
        USING (
            SELECT DISTINCT
                idExame
            FROM logs.api_birads
            WHERE status_integracao = '200'
              AND DATE(data_hora) >= '{data_update_log}'
        ) AS source
            ON target.idExame = source.idExame
        WHEN MATCHED THEN
            UPDATE SET target.flgIntegracaoConcluida = true
    """)

# COMMAND ----------

def verifica_pendentes():
    """Verifica a quantidade de registros com integração pendente"""

    df = spark.sql(f"""
        select count(1) as qtd
        from {vw_gold_modelo_birads_complemento}
        where dt_execucao = '{data_execucao_modelo}'
        and fl_integracao_concluida = false
    """)

    return df.collect()[0]['qtd']

# COMMAND ----------

def enviados():
    """Retorna a quantidade de registros enviados"""

    df = spark.sql(f"""
        select count(1) as qtd
        from {vw_gold_modelo_birads_complemento}
        where dt_execucao = '{data_execucao_modelo}'
        and fl_integracao_concluida = true
    """)

    return df.collect()[0]['qtd']

# COMMAND ----------

# MAGIC %md
# MAGIC # Verifica a quantidade de registros com integração pendente

# COMMAND ----------

tentativas = 1
max_tentativas = 3
qtd_pendente = 0
qtd_anterior = 0
sleep_time = 180

while True:
    # Atualiza flags
    update_tbl_saida()
    update_tbl_complemento()

    # Oculta dados pessoais se existirem
    # hide_pii()

    # Verifica quantidade de registros pendentes
    qtd_pendente = verifica_pendentes()
    print(f"Quantidade de registros pendentes: {qtd_pendente}")

    # Se não houver registros pendentes, sai do loop
    if qtd_pendente == 0:
        print(f"Não há mais registros pendentes. Saindo do loop!")
        break

    # Verifica se a quantidade anterior é igual a quantidade pendente
    # Se sim, significa que a API parou de enviar os registros
    if qtd_anterior == qtd_pendente:
        tentativas += 1
        print(f"A quantidade de registros pendentes não mudou")
        print(f"- Anterior: {qtd_anterior}")
        print(f"- Pendente: {qtd_pendente}")
        print(f"- Iniciando tentativa: {tentativas}")
        
    # Se a qtd tentativas exceder o limite, sai do loop 
    if tentativas > max_tentativas:
        print(f"A quantidade máxima de tentativas ({max_tentativas}) foi excedida! Saindo do loop!")
        break

    # Atualiza a quantidade anterior
    qtd_anterior = qtd_pendente

    # Aguarda X minutos para um nova tentativa
    print("Aguardando próxima execução...")
    print("-" * 120)
    time.sleep(sleep_time)


# COMMAND ----------

if qtd_pendente == 0:
    try:
        spark.sql("optimize logs.api_birads").display()
        spark.sql("analyze table logs.api_birads compute statistics").display()
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred.\n- Arguments: {ex.args}")

    dbutils.notebook.exit(f"Todos os {enviados()} registros foram enviados!")
else:
    # Atualiza flags
    update_tbl_saida()
    update_tbl_complemento()

    # Oculta dados pessoais se existirem
    hide_pii()
    
    raise Exception(f"Ainda existem {qtd_pendente} registros pendentes de envio!")

# COMMAND ----------


