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

# spark.sql(f"""
#     create or replace table ia.{table_name_entrada}_tmp as 
#     SELECT DISTINCT
#         uuid() as idPredicao
#         , exame.idPatient
#         , exame.idExame
#         , exame.idMedico
#         , exame.


#         , listaExame.laudoOriginal as laudoExame
#     FROM ia.tbl_gold_exame as exame
#     LATERAL VIEW EXPLODE(listaExames) listaExames AS listaExame
#     where listaExame.laudoOriginal is not null
#     and exame.dataExame >= DATE_SUB(CURRENT_DATE(), 7)
#     and cast(codigo as bigint) IN (
#         20010141	,
#         20101201	,
#         33010021	,
#         33010048	,
#         33010129	,
#         33010323	,
#         34010130	,
#         36010022	,
#         36010030	,
#         36010103	,
#         36010138	,
#         36010162	,
#         36010170	,
#         40202666	,
#         40304728	,
#         40403254	,
#         40708128	,
#         40803120	,
#         40808262	,
#         40809161	,
#         40901092	,
#         40901106	,
#         40901114	,
#         40901211	,
#         40901220	,
#         40901386	,
#         40901483	,
#         40902072	,
#         40902080	,
#         41001010	,
#         41001079	,
#         41001095	,
#         41001117	,
#         41001125	,
#         41001133	,
#         41001176	,
#         41001184	,
#         41001222	,
#         41001230	,
#         41001290	,
#         41101014	,
#         41101022	,
#         41101073	,
#         41101120	,
#         41101227	,
#         41101260	,
#         41101278	,
#         41101308	,
#         41101316	,
#         41101480	,
#         41101537	
#     ) 
#     and tipoCodigo = 'TUSS'
# """).display()

# COMMAND ----------

spark.sql(f"""
    create or replace table ia.{table_name_entrada}_tmp as 
SELECT DISTINCT
        uuid()                                    as idPredicao
        , exame.idPatient
        , exame.idExame
        , exame.idMedico
        , exame.idOrganization                    as idUnidade
        , exame.cnpjHospital                      as cnpjUnidade
        , exame.nomeHospital                      as nomeUnidade
        , exame.nomeRegionalHospital              as regionalUnidade
        , exame.numCrm                            as numCrmSolicitante
        , exame.ufCrm                             as ufCrmSolicitante
        , exame.nomeConvenio                      as nomeConvenio
        , exame.medicoEncaminhador                as nomeMedicoSolicitante
        , date(exame.dataExame)                   as dataExame
        , exame.codigo                            as codigoTuss
        , exame.descricaoProcedimento             as descricaoTuss
        , exame.nomeCategoria                     as descricaoProcedimento

        , listaExame.laudoOriginal as laudoExame
    FROM ia.tbl_gold_exame as exame
    LATERAL VIEW EXPLODE(listaExames) listaExames AS listaExame
    where listaExame.laudoOriginal is not null
    and exame.dataExame >= DATE_SUB(CURRENT_DATE(), 7)
    and cast(codigo as bigint) IN (
        20010141	,
        20101201	,
        33010021	,
        33010048	,
        33010129	,
        33010323	,
        34010130	,
        36010022	,
        36010030	,
        36010103	,
        36010138	,
        36010162	,
        36010170	,
        40202666	,
        40304728	,
        40403254	,
        40708128	,
        40803120	,
        40808262	,
        40809161	,
        40901092	,
        40901106	,
        40901114	,
        40901211	,
        40901220	,
        40901386	,
        40901483	,
        40902072	,
        40902080	,
        41001010	,
        41001079	,
        41001095	,
        41001117	,
        41001125	,
        41001133	,
        41001176	,
        41001184	,
        41001222	,
        41001230	,
        41001290	,
        41101014	,
        41101022	,
        41101073	,
        41101120	,
        41101227	,
        41101260	,
        41101278	,
        41101308	,
        41101316	,
        41101480	,
        41101537	
    ) 
    and tipoCodigo = 'TUSS'
""").display()

# COMMAND ----------

spark.sql(f"""
    create or replace table ia.{table_name_entrada}_tmp_final as 
    select 
        tmp.* 
    , ptt.numCPF
    , ptt.nomePaciente
    , ptt.idadePaciente
    , case lower(ptt.nomeGenero)
            when 'female' then 'F'
            when 'male' then 'M'
            else '-'
        end as sexoPaciente
    , ptel.telefoneContato
    from ia.{table_name_entrada}_tmp as tmp
    left join ia.tbl_gold_paciente as ptt
    on tmp.idPatient = ptt.idPatient 

    left join  (
    select
        idPatient,
        coalesce(concat(telecom_ddd, telecom_number),  telecom_value) as telefoneContato
    from ia.tbl_silver_patient_telecom
    where lower(trim(telecom_system)) = "phone"
    qualify
        row_number() over (
        partition by idPatient
        order by telecom_updateDate desc
        ) = 1
    ) as ptel
    on ptt.idPatient = ptel.idPatient
""").display()

# COMMAND ----------

print(f"ia.{table_name_entrada}_tmp_final")

# COMMAND ----------

spark.sql(f"""
    INSERT INTO ia.{table_name_entrada} (
        idPredicao
        ,idPatient 
        ,idExame 
        ,idMedico 
        ,idUnidade 
        ,numCPF 
        ,nomePaciente
        ,idadePaciente
        ,sexoPaciente 
        ,telefoneContato 
        ,nomeConvenio 
        ,cnpjUnidade 
        ,nomeUnidade 
        ,regionalUnidade 
        ,numCrmSolicitante 
        ,ufCrmSolicitante 
        ,nomeMedicoSolicitante 
        ,dataExame 
        ,codigoTuss 
        ,descricaoTuss 
        ,descricaoProcedimento 
        ,laudoExame 
        ,datParticao 
    )
    SELECT 
        idPredicao
        ,idPatient 
        ,idExame 
        ,idMedico 
        ,idUnidade 
        ,numCPF 
        ,nomePaciente 
        ,idadePaciente
        ,sexoPaciente 
        ,telefoneContato 
        ,nomeConvenio 
        ,cnpjUnidade 
        ,nomeUnidade 
        ,regionalUnidade 
        ,numCrmSolicitante 
        ,ufCrmSolicitante 
        ,nomeMedicoSolicitante 
        ,dataExame 
        ,codigoTuss 
        ,descricaoTuss 
        ,descricaoProcedimento 
        ,laudoExame 
        ,CURRENT_DATE() 
    FROM ia.{table_name_entrada}_tmp_final
    WHERE idExame NOT IN (
        SELECT idExame FROM ia.{table_name_entrada}
    )
""").display()

# COMMAND ----------

print(table_name_entrada)

# COMMAND ----------

c = spark.sql(f"SELECT COUNT(*) as total FROM ia.{table_name_entrada} WHERE datParticao = current_date()")
if c.collect()[0].total == 0:
    raise Exception("Sem dados para hoje")

# COMMAND ----------

spark.sql(f"VACUUM ia.{table_name_entrada}")

# COMMAND ----------

spark.sql(f"OPTIMIZE ia.{table_name_entrada}")
