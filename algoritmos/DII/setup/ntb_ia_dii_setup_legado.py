# Databricks notebook source
# MAGIC %md
# MAGIC # Parâmetros do notebook

# COMMAND ----------

# DBTITLE 1,Ambiente
dbutils.widgets.text("environment", "dev", "Environment")

environment = dbutils.widgets.get("environment")

print("Environment:", environment)

# COMMAND ----------

# DBTITLE 1,ID do Projeto
dbutils.widgets.text("id_projeto", "dii", "ID Projeto")
id_projeto = dbutils.widgets.get("id_projeto")
print("id_projeto:", id_projeto)

# COMMAND ----------

# DBTITLE 1,Define variável que será injetada no nome das tabelas
if environment in ["hml", "prd"]:
    environment_tbl = ""
else:
    environment_tbl = f"{environment}_"

print(f"environment_tbl: {environment_tbl}")

# COMMAND ----------

root_path_gold = f"/mnt/trusted/datalake/ia/data/{id_projeto}/gold/"
print(f"root_path_gold: {root_path_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC # DDL das tabelas e views

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variáveis com os nomes das tabelas

# COMMAND ----------

# DBTITLE 1,Define variáveis comvw_ os nomes das tabelas
table_name_entrada = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_entrada"
table_name_pre_processamento = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_pre_processamento"
table_name_vetores = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_vetor"
table_name_saida = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_saida"
view_name = f"{environment_tbl}vw_gold_modelo_{id_projeto}"
table_name_retorno = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_retorno"
table_name_retorno_hist = f"{table_name_retorno}_historico"
view_name_analise_retorno = f"{environment_tbl}vw_gold_modelo_{id_projeto}_analise_retorno"
table_name_navegacao = f"{environment_tbl}tbl_gold_modelo_{id_projeto}_navegacao"

# COMMAND ----------

# DBTITLE 1,Exibe valores das variáveis com nomes das tabelas
print(f"{'table_name_entrada':40}: {table_name_entrada}")
print(f"{'table_name_pre_processamento':40}: {table_name_pre_processamento}")
print(f"{'table_name_vetores':40}: {table_name_vetores}")
print(f"{'table_name_saida':40}: {table_name_saida}")
print(f"{'view_name':40}: {view_name}")
print(f"{'table_name_retorno':40}: {table_name_retorno}")
print(f"{'table_name_retorno_hist':40}: {table_name_retorno_hist}")
print(f"{'view_name_analise_retorno':40}: {view_name_analise_retorno}")
print(f"{'table_name_navegacao':40}: {table_name_navegacao}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_gold_entrada

# COMMAND ----------

table_name_entrada

# COMMAND ----------

# spark.sql(f"drop table if exists ia.{table_name_entrada}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_entrada}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ia.{table_name_entrada} (
        dataExecucaoModelo DATE,
        idPredicao STRING,
        idExame STRING,
        dataExame TIMESTAMP,
        dataLiberacaoLaudo TIMESTAMP,
        nomeConvenio STRING,
        tipoCodigo STRING,
        codigo STRING,
        nomeCodigo STRING,
        descricaoProcedimento STRING,
        idUnidade STRING,
        nomeUnidade STRING,
        cnpjUnidade STRING,
        regionalUnidade STRING,
        tipoUnidade STRING,
        idMedico STRING,
        numCrm STRING,
        ufCrm STRING,
        nomeMedico STRING,
        idPaciente STRING,
        cpfPaciente STRING,
        nomePaciente STRING,
        dataNascimentoPaciente TIMESTAMP,
        idadePaciente BIGINT,
        sexoPaciente STRING,
        telefoneContato STRING,
        laudoOriginal STRING,
        laudoDuplicado INTEGER,
        dataCarga TIMESTAMP
    )
    PARTITIONED BY (dataExecucaoModelo)
    LOCATION '{root_path_gold}{table_name_entrada}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_gold_pre_processamento

# COMMAND ----------

table_name_pre_processamento

# COMMAND ----------

# spark.sql(f"drop table if exists ia.{table_name_pre_processamento}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_pre_processamento}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create table if not exists ia.{table_name_pre_processamento}
    (
        dataExecucaoModelo DATE,
        idPredicao STRING,
        idExame STRING,
        tokens ARRAY<STRING>,
        resultadosBigrams ARRAY<STRING>,
        resultadosTrigrams ARRAY<STRING>,
        textoTrigrams STRING,
        score DOUBLE,
        decil STRING,
        dataCargaPreProcessamento TIMESTAMP,
        nomeModeloScore STRING,
        estagioModeloScore STRING,
        versaoModeloScore STRING,
        idTreinamentoScore STRING
    )
    partitioned by (dataExecucaoModelo)
    location '{root_path_gold}{table_name_pre_processamento}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_gold_vetores

# COMMAND ----------

table_name_vetores

# COMMAND ----------

# spark.sql(f"drop table if exists ia.{table_name_vetores}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_vetores}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create table if not exists ia.{table_name_vetores} (
        idExame STRING,
        laudoLimpo STRING,
        laudoVetorizado ARRAY<FLOAT>,
        dataCarga TIMESTAMP
    )
    location '{root_path_gold}{table_name_vetores}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_gold_saida

# COMMAND ----------

table_name_saida

# COMMAND ----------

# spark.sql(f"drop table if exists ia.{table_name_saida}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_saida}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create table if not exists ia.{table_name_saida}
    (
        dataExecucaoModelo date,
        idPredicao string,
        idExame string,
        laudoLimpo string,
        laudoVetorizado array<float>,
        scoreProbabilidade float,
        threshold float,
        classeProbabilidade string,
        dataPredicao timestamp,
        nomeModelo string,
        estagioModelo string,
        versaoModelo string,
        idTreinamento string
    )
    partitioned by (dataExecucaoModelo)
    location '{root_path_gold}{table_name_saida}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_gold_entrada_saida

# COMMAND ----------

view_name

# COMMAND ----------

# spark.sql(f"drop view if exists ia.{view_name}").display()

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create or replace view ia.{view_name}
    as
    select
        ent.dataExecucaoModelo,
        ent.idPredicao,
        ent.idExame,
        ent.dataExame,
        ent.dataLiberacaoLaudo,
        ent.nomeConvenio,
        ent.tipoCodigo,
        ent.codigo,
        ent.nomeCodigo,
        ent.descricaoProcedimento,
        ent.idUnidade,
        ent.nomeUnidade,
        default.unencrypt(ent.cnpjUnidade) as cnpjUnidade,
        default.unencrypt(ent.regionalUnidade) as regionalUnidade,
        ent.tipoUnidade,
        ent.idMedico,
        default.unencrypt(ent.numCrm) as numCrm,
        ent.ufCrm,
        if(trim(ent.nomeMedico) != "", default.unencrypt(ent.nomeMedico), null) as nomeMedico,
        ent.idPaciente,
        default.unencrypt(ent.cpfPaciente) as cpfPaciente,
        default.unencrypt(ent.nomePaciente) as nomePaciente,
        ent.dataNascimentoPaciente,
        ent.idadePaciente,
        ent.sexoPaciente,
        ent.telefoneContato,
        ent.laudoOriginal,
        ent.laudoDuplicado,
        ent.dataCarga,

        pre.tokens,
        pre.resultadosBigrams,
        pre.resultadosTrigrams,
        pre.textoTrigrams,
        pre.score,
        pre.decil,
        pre.dataCargaPreProcessamento,
        pre.nomeModeloScore,
        pre.estagioModeloScore,
        pre.versaoModeloScore,
        pre.idTreinamentoScore,

        sai.laudoLimpo,
        sai.laudoVetorizado,
        sai.scoreProbabilidade,
        sai.threshold,
        sai.classeProbabilidade,
        sai.dataPredicao,
        sai.nomeModelo,
        sai.estagioModelo,
        sai.versaoModelo,
        sai.idTreinamento

    from ia.{table_name_entrada} as ent

    left join ia.{table_name_pre_processamento} as pre
        on ent.idPredicao = pre.idPredicao

    left join ia.{table_name_saida} as sai
        on ent.idPredicao = sai.idPredicao
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_gold_retorno

# COMMAND ----------

table_name_retorno

# COMMAND ----------

# spark.sql(f"drop table if exists ia.{table_name_retorno}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_retorno}", recurse=True)

# # Criada dinamicamente, qdo o notebook de retorno for processado
# spark.sql(f"drop table if exists ia.{table_name_retorno_hist}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_retorno_hist}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create table if not exists ia.{table_name_retorno}
    (
        idRetorno string,
        dataExecucaoModelo date,
        idPredicao string,
        idExame string,
        dataHoraRetorno timestamp,
        dataHoraAtualizacao timestamp,
        arquivoRetorno string,
        achado string,
        observacoes string
    )
    partitioned by (dataExecucaoModelo)
    location '{root_path_gold}{table_name_retorno}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_gold_analise_retorno

# COMMAND ----------

view_name_analise_retorno

# COMMAND ----------

# spark.sql(f"drop view if exists ia.{view_name_analise_retorno}").display()
# dbutils.fs.rm(f"{root_path_gold}{view_name_analise_retorno}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create or replace view ia.{view_name_analise_retorno}
    as
    select
        ent.dataExecucaoModelo,
        ent.idPredicao,
        ent.idExame,
        ent.dataExame,
        ent.dataLiberacaoLaudo,
        ent.nomeConvenio,
        ent.tipoCodigo,
        ent.codigo,
        ent.nomeCodigo,
        ent.descricaoProcedimento,
        ent.idUnidade,
        ent.nomeUnidade,
        default.unencrypt(ent.cnpjUnidade, 0) as cnpjUnidade,
        default.unencrypt(ent.regionalUnidade, 0) as regionalUnidade,
        ent.tipoUnidade,
        ent.idMedico,
        default.unencrypt(ent.numCrm, 0) as numCrm,
        ent.ufCrm,
        default.unencrypt(ent.nomeMedico, 0) as nomeMedico,
        ent.idPaciente,
        default.unencrypt(ent.cpfPaciente, 0) as cpfPaciente,
        default.unencrypt(ent.nomePaciente, 0) as nomePaciente,
        ent.dataNascimentoPaciente,
        ent.idadePaciente,
        ent.sexoPaciente,
        ent.telefoneContato,
        ent.laudoOriginal,
        ent.laudoDuplicado,
        ent.dataCarga,
        -- pre.dataExecucaoModelo,
        -- pre.idPredicao,
        -- pre.idExame,
        pre.tokens,
        pre.resultadosBigrams,
        pre.resultadosTrigrams,
        pre.textoTrigrams,
        pre.score,
        pre.decil,
        pre.dataCargaPreProcessamento,
        pre.nomeModeloScore,
        pre.estagioModeloScore,
        pre.versaoModeloScore,
        pre.idTreinamentoScore,
        -- sai.dataExecucaoModelo,
        -- sai.idPredicao,
        -- sai.idExame,
        sai.laudoLimpo,
        sai.laudoVetorizado,
        sai.scoreProbabilidade,
        sai.threshold,
        sai.classeProbabilidade,
        sai.dataPredicao,
        sai.nomeModelo,
        sai.estagioModelo,
        sai.versaoModelo,
        sai.idTreinamento,

        ret.idRetorno,
        -- ret.dataExecucaoModelo,
        -- ret.idPredicao,
        -- ret.idExame,
        ret.dataHoraRetorno,
        ret.dataHoraAtualizacao,
        ret.arquivoRetorno,
        ret.achado,

        case ret.achado
            when '1 - Alta probabilidade' then 'alta'
            when '2 - Média probabilidade' then 'alta'
            when '3 - Baixa probabilidade' then 'baixa'
            when '4 - Descartada probabilidade' then 'nd'
        end as achadoTratado,

        ret.observacoes

        -- 1 as flgEntrada,
        -- if(said.idPredicao is not null, 1, 0) as flgSaida,
        -- if(said.idPredicao is not null and said.flgRelevante = True, 1, 0) as flgEnviado,
        -- if(reto.idRetorno is not null, 1, 0) as flgRetorno,
        -- if(reto.idRetorno is not null and reto.achadoRelevante is not null, 1, 0) as flgRotulado,
        -- if(reto.idRetorno is not null and reto.achadoRelevante is not null and reto.achadoRelevante = '1 - Sim (Tem Doença Fígado)', 1, 0) as flgSimComDoencaFigado,
        -- if(reto.idRetorno is not null and reto.achadoRelevante is not null and reto.achadoRelevante = '2 - Sim (Mas Não Tem Doença Fígado)', 1, 0) as flgSimSemDoencaFigado,
        -- if(reto.idRetorno is not null and reto.achadoRelevante is not null and reto.achadoRelevante in ('1 - Sim (Tem Doença Fígado)', '2 - Sim (Mas Não Tem Doença Fígado)'), 1, 0) as -- flgTotalSim,
        -- if(reto.idRetorno is not null and reto.achadoRelevante is not null and reto.achadoRelevante = '3 - Não', 1, 0) as flgNao,
        -- if(reto.idRetorno is not null and (reto.observacao ilike '%naveg%' or reto.destino ilike '%navega%'), 1, 0) as flgNavegacao


    from ia.{table_name_entrada} as ent

    left join ia.{table_name_pre_processamento} as pre
        on ent.idPredicao = pre.idPredicao

    left join ia.{table_name_saida} as sai
        on ent.idPredicao = sai.idPredicao

    left join ia.{table_name_retorno} as ret
        on ent.idPredicao = ret.idPredicao

""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tbl_gold_navegacao

# COMMAND ----------

table_name_navegacao

# COMMAND ----------

# spark.sql(f"drop table if exists ia.{table_name_navegacao}").display()
# dbutils.fs.rm(f"{root_path_gold}{table_name_navegacao}", recurse=True)

# COMMAND ----------

# DBTITLE 1,ddl
spark.sql(f"""
    create table if not exists ia.{table_name_navegacao}
    (
        idNavegacao string,
        dataEnviadoNavegacao date,
        dataExecucaoModelo date,
        idPredicao string,
        idExame string
    )
    partitioned by (dataEnviadoNavegacao)
    location '{root_path_gold}{table_name_navegacao}'
""").display()

# COMMAND ----------


