# Databricks notebook source
# MAGIC %md
# MAGIC #Importações

# COMMAND ----------

# DBTITLE 1,libs
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC #Modelo

# COMMAND ----------

# DBTITLE 1,Método de cálculo de clearance
class DataTransform:
    def __init__(self):
        pass

    def muda_caractere(self, texto):
        texto = texto.lower()
        texto = texto.replace('inferior a ', '< ')
        texto = texto.replace('<0.8', '< 0.8')
        texto = texto.replace('<0.2', '< 0.2')
        texto = texto.replace('0..40','0.40')
        texto = texto.replace('1.90]', '1.90')
        texto = texto.replace('.0.20', '0.20')
        texto = texto.replace('0.8.', '0.8')
        texto = texto.replace('0.3.16', '0.3')
        texto = texto.replace('1..4', '1.4')

        texto = re.sub(r'(\d+),(\d+)', r'\1.\2', texto)

        return texto
    

    def calc_cleareance(self, idade, genero, altura, peso, valor_exame):
        '''
        Função que calcula o Clearance de creatinina
        usando a fórmula de Cockcroft & Gault para pessoas maiores de 16 anos
        e a fórmula Schwartz para idade <= 16 anos

        entrada: dataframe

        retorna: valor cleareance
        '''
        cleareance_cg = -1
        if idade > 16:
            if (genero == 'male'):
                cleareance_cg = (140 - idade) * (peso/(72*valor_exame))
            elif (genero == 'female') : 
                cleareance_cg = ((140 - idade) * (peso/(72*valor_exame)))*0.8
        else:
            cleareance_cg = 0.55*(altura/valor_exame)

        return cleareance_cg
    

    def calc_cleareance_without_weigth(self, valor_exame, idade, genero):
        """
            Função que calcula o clearance baseado na fórmula enviada na tarefa #9383
        """
        cleareance_cg = -1

        try:
            idade = int(idade)
            step1 = 142
            step3 = 0.9938 ** idade
            if genero == 'female':
                step2 = (valor_exame / 0.7) ** -1.2
                step4 = step3 * 1.012
                cleareance_cg = step1 * step2 * step4

                return cleareance_cg
            else:
                step2 = (valor_exame / 0.9) ** -1.2

                cleareance_cg = step1 * step2 * step3

                return cleareance_cg
            
        except Exception as e:
            return -2

        return cleareance_cg
    

    def transform_peso(self, df):
        for x in df['peso']:
            if x > 10000 and x < 100000:
                y = x / 1000
                df = df.replace({x:y})
            else:
                if x > 100000:
                    y = x / 10000
                    df = df.replace({x:y})
                else:
                    if x > 250:
                        y = x / 10
                        df = df.replace({x:y})

        return df



# COMMAND ----------

# MAGIC %md
# MAGIC #Criação das tabelas

# COMMAND ----------

# DBTITLE 1,Criando tabela de exames
# MAGIC %sql
# MAGIC create or replace table ia.tb_diamond_creatinina_wrk_01 as
# MAGIC select _id                                                                                              as id_exame
# MAGIC     , cast(replace(obs.subject.reference, 'Patient/', '') as bigint)                                    as id_paciente
# MAGIC     , cast(replace(obs.identifier.assigner.reference, 'Organization/', '') as bigint)                   as id_unidade
# MAGIC     , obs.identifier.assigner.display                                                                   as nme_unidade
# MAGIC     , replace(obs.encounter.reference, 'Encounter/', '')                                                as id_encontro
# MAGIC     , obs.extension.codigo_corporativo_profissional_solicitante                                         as cod_medico_solicitante
# MAGIC     , obs.extension.nome_corporativo_profissional_solicitante                                           as nme_medico_solicitante
# MAGIC     , obs.code.coding.system                                                                            as tp_cod_exame
# MAGIC     , obs.code.coding.code                                                                              as cod_exame
# MAGIC     , obs.code.text                                                                                     as nme_exame
# MAGIC     , cast(
# MAGIC         coalesce(
# MAGIC                 obs.issued,
# MAGIC                 obs.extension.data_local_entrada,
# MAGIC                 obs.extension.data_local_prescricao,
# MAGIC                 obs.extension.data_local_pedido,
# MAGIC                 obs.extension.data_local_coleta,
# MAGIC                 obs.extension.data_local_triagem,
# MAGIC                 obs.extension.data_local_digitacao_laudo,
# MAGIC                 obs.extension.data_local_assinatura_laudo,
# MAGIC                 obs.extension.data_local_liberacao_laudo,
# MAGIC                 obs.extension.data_local_previsao_laudo,
# MAGIC                 obs.extension.data_local_entrega_laudo
# MAGIC                 ) as timestamp) as dt_exame
# MAGIC     , cast(case 
# MAGIC             when obs.component[0].code.coding.code = 'CRE' then replace(replace(obs.component[0].value.valueString, 'Inferior a ', ''), ',', '.')
# MAGIC             when obs.component[1].code.coding.code = 'CRE' then replace(replace(obs.component[1].value.valueString, 'Inferior a ', ''), ',', '.')
# MAGIC             when obs.component[2].code.coding.code = 'CRE' then replace(replace(obs.component[2].value.valueString, 'Inferior a ', ''), ',', '.')
# MAGIC             else 0.0
# MAGIC         end as double) as vl_creatinina    
# MAGIC     , current_date() as dt_execucao
# MAGIC             
# MAGIC from        gold_corporativo.observation.tb_gold_mov_observation_exames as obs 
# MAGIC where 
# MAGIC     obs.code.coding.system = 'TUSS'
# MAGIC and cast(obs.code.coding.code as bigint) = 40301630
# MAGIC and cast(
# MAGIC       coalesce(
# MAGIC             obs.extension.data_local_entrada,
# MAGIC             obs.extension.data_local_prescricao,
# MAGIC             obs.extension.data_local_pedido,
# MAGIC             obs.extension.data_local_coleta,
# MAGIC             obs.extension.data_local_triagem,
# MAGIC             obs.extension.data_local_digitacao_laudo,
# MAGIC             obs.extension.data_local_assinatura_laudo,
# MAGIC             obs.extension.data_local_liberacao_laudo,
# MAGIC             obs.extension.data_local_previsao_laudo,
# MAGIC             obs.extension.data_local_entrega_laudo
# MAGIC             ) as timestamp) between date_sub(current_timestamp(), 5) and current_timestamp()

# COMMAND ----------

# DBTITLE 1,adicionando dados do paciente
# MAGIC %sql
# MAGIC create or replace table ia.tb_diamond_creatinina_wrk_03 as 
# MAGIC select 
# MAGIC   wrk.*, 
# MAGIC   lower(trim(
# MAGIC       case 
# MAGIC         when pat.gender = 'f' then 'female'
# MAGIC         when pat.gender = 'm' then 'male'
# MAGIC         else pat.gender
# MAGIC       end
# MAGIC     )) as pat_genero,
# MAGIC   pat.birthDate                                                     as pat_data_nascimento, 
# MAGIC   floor((datediff(current_date, to_date(pat.birthDate)) / 365.25),0)  as pat_idade,
# MAGIC   pat.telecom[0].ddd as pat_tel_ddd,
# MAGIC   pat.telecom[0].number as pat_tel_numero
# MAGIC from      ia.tb_diamond_creatinina_wrk_01 as wrk 
# MAGIC left join gold_corporativo.patient.tb_gold_mov_patient as pat 
# MAGIC on wrk.id_paciente = pat._id

# COMMAND ----------

# DBTITLE 1,Adicionando crm e uf do medico
# MAGIC %sql
# MAGIC create or replace table ia.tb_diamond_creatinina_wrk_04 as 
# MAGIC select 
# MAGIC   wrk.*,
# MAGIC   case 
# MAGIC     when prac.identifier[0].type = 'CRM' then prac.identifier[0].value
# MAGIC     when prac.identifier[1].type = 'CRM' then prac.identifier[1].value
# MAGIC     when prac.identifier[2].type = 'CRM' then prac.identifier[3].value
# MAGIC   else null 
# MAGIC   end med_num_crm,
# MAGIC   case 
# MAGIC     when prac.identifier[0].type = 'CRM' then prac.identifier[0].system
# MAGIC     when prac.identifier[1].type = 'CRM' then prac.identifier[1].system
# MAGIC     when prac.identifier[2].type = 'CRM' then prac.identifier[3].system
# MAGIC   else null 
# MAGIC   end med_uf_crm
# MAGIC
# MAGIC from ia.tb_diamond_creatinina_wrk_03 as wrk 
# MAGIC left join gold_corporativo.practitioner.tb_gold_mov_practitioner as prac
# MAGIC on wrk.cod_medico_solicitante = prac._id
# MAGIC

# COMMAND ----------

# DBTITLE 1,Adicionando convenio e plano
# MAGIC %sql
# MAGIC create or replace table  ia.tb_diamond_creatinina_wrk_05 as 
# MAGIC select   
# MAGIC     wrk.*
# MAGIC   , coalesce(acc.extension.nome_local_convenio, enc.extension.nome_local_convenio)  as nme_convenio 
# MAGIC   , enc.extension.nome_local_plano as nme_plano 
# MAGIC from ia.tb_diamond_creatinina_wrk_04  as wrk 
# MAGIC left join gold_corporativo.encounter.tb_gold_mov_encounter as enc
# MAGIC on wrk.id_encontro = enc.`_id`
# MAGIC
# MAGIC left join gold_corporativo.account.tb_gold_mov_account  as acc 
# MAGIC on wrk.id_encontro = cast(replace(acc.extension.encounter_id, 'Encounter/', '') as bigint)

# COMMAND ----------

# DBTITLE 1,adicionando dados da unidade
# MAGIC %sql
# MAGIC create or replace table  ia.tb_diamond_creatinina_wrk_06 as 
# MAGIC select   
# MAGIC     wrk.*
# MAGIC   , org.numCNPJ           as est_cnpj_unidade
# MAGIC   , org.sigEstado          as est_nome_regional
# MAGIC
# MAGIC from ia.tb_diamond_creatinina_wrk_05                              as wrk 
# MAGIC left join (select `_id` as id, 
# MAGIC                   documento.numCNPJ,
# MAGIC                   endereco.sigEstado
# MAGIC           from gold_corporativo.organization.tb_gold_mov_organization 
# MAGIC           qualify row_number () over (partition by `_id` order by tmpDataAtualizacao desc) = 1 
# MAGIC           )
# MAGIC           as org 
# MAGIC   on wrk.id_unidade = id

# COMMAND ----------

# DBTITLE 1,gerando dataframe spark
df = spark.table("ia.tb_diamond_creatinina_wrk_06")

# COMMAND ----------

# MAGIC %md
# MAGIC #Consumo modelo

# COMMAND ----------

# DBTITLE 1,criando udf para calculo
def calc_cleareance(valor_exame, idade, gender):
    return DataTransform().calc_cleareance_without_weigth(valor_exame, idade, gender)

calc_cleareance_udf = udf(calc_cleareance, StringType()) 

# COMMAND ----------

# DBTITLE 1,executando calculo
df = df.withColumn("vl_clearance", calc_cleareance_udf(df["vl_creatinina"], df["pat_idade"], df["pat_genero"]))

# COMMAND ----------

# DBTITLE 1,create tb_diamond_clearance
df.createOrReplaceTempView("tb_diamond_clearance")

# COMMAND ----------

# MAGIC %md
# MAGIC #Criação tabela de saida

# COMMAND ----------

# DBTITLE 1,tb_diamond_clearance_un
# MAGIC %sql
# MAGIC create or replace temp view tb_diamond_clearance_un as 
# MAGIC select 
# MAGIC     * except(
# MAGIC         med_num_crm,
# MAGIC         pat_tel_numero,
# MAGIC         nme_medico_solicitante
# MAGIC     )
# MAGIC      , case when array_size(split(pat_tel_numero, ' ')) = 1
# MAGIC         and (
# MAGIC             (
# MAGIC                 contains(pat_tel_numero, '/') or
# MAGIC                 contains(pat_tel_numero, '+') or
# MAGIC                 contains(pat_tel_numero, '=')
# MAGIC             )
# MAGIC             or 
# MAGIC             len(pat_tel_numero) >= 24
# MAGIC         )
# MAGIC         then gold_corporativo.default.rdsl_decrypt(pat_tel_numero, 0)
# MAGIC         else  pat_tel_numero end 
# MAGIC             as pat_tel_numero
# MAGIC     , gold_corporativo.default.rdsl_decrypt(med_num_crm, 1) as med_num_crm
# MAGIC     , gold_corporativo.default.rdsl_decrypt(nme_medico_solicitante, 1) as nme_medico_solicitante
# MAGIC    
# MAGIC from tb_diamond_clearance

# COMMAND ----------

# DBTITLE 1,ia.tb_diamond_creatinina_wrk_07
# MAGIC %sql 
# MAGIC create or replace table ia.tb_diamond_creatinina_wrk_07 as 
# MAGIC select *  except(
# MAGIC         med_num_crm,
# MAGIC         pat_tel_numero,
# MAGIC         nme_medico_solicitante
# MAGIC     )
# MAGIC , default.rdsl_encrypt(med_num_crm, 0) as med_num_crm
# MAGIC , default.rdsl_encrypt(pat_tel_numero, 0) as pat_tel_numero
# MAGIC , default.rdsl_encrypt(nme_medico_solicitante, 0) as nme_medico_solicitante
# MAGIC from tb_diamond_clearance_un

# COMMAND ----------

# DBTITLE 1,create ia.tb_diamond_nefrologia_clearance_saida
# MAGIC %sql
# MAGIC merge into ia.tb_diamond_nefrologia_clearance_saida as target
# MAGIC using (
# MAGIC   select 
# MAGIC     uuid() as idPredicao
# MAGIC     , id_paciente as idPatient
# MAGIC     , id_exame as idExame
# MAGIC     , cod_medico_solicitante as idMedico
# MAGIC     , pat_data_nascimento as dataNascimento
# MAGIC     , pat_idade as idade
# MAGIC     , pat_genero as sexoPaciente
# MAGIC     , pat_tel_numero as telefoneContato
# MAGIC     , id_unidade as idUnidade
# MAGIC     , est_cnpj_unidade as cnpjUnidade
# MAGIC     , nme_unidade as nomeUnidade
# MAGIC     , est_nome_regional as regionalUnidade
# MAGIC     , med_num_crm as numCrmSolicitante
# MAGIC     , med_uf_crm as ufCrmSolicitante
# MAGIC     , nme_convenio as nomeConvenio
# MAGIC     , nme_plano as nomePlano
# MAGIC     , nme_medico_solicitante as nomeMedicoSolicitante
# MAGIC     , dt_exame as dataExameRecente
# MAGIC     , cod_exame as codigoTuss
# MAGIC     , nme_exame as descricaoTuss
# MAGIC     , nme_exame as descricaoProcedimento
# MAGIC     , vl_creatinina as valorExameCreatinina
# MAGIC     , cast(vl_clearance as numeric) as valorClearance,
# MAGIC     current_date() as dt_execucao 
# MAGIC   from ia.tb_diamond_creatinina_wrk_07 as crt
# MAGIC   where cast(vl_clearance as numeric) > 0 
# MAGIC ) as source
# MAGIC on target.idExame = source.idExame
# MAGIC when matched then
# MAGIC   update set
# MAGIC     idPredicao = source.idPredicao,
# MAGIC     idPatient = source.idPatient,
# MAGIC     idMedico = source.idMedico,
# MAGIC     dataNascimento = source.dataNascimento,
# MAGIC     idade = source.idade,
# MAGIC     sexoPaciente = source.sexoPaciente,
# MAGIC     telefoneContato = source.telefoneContato,
# MAGIC     idUnidade = source.idUnidade,
# MAGIC     cnpjUnidade = source.cnpjUnidade,
# MAGIC     nomeUnidade = source.nomeUnidade,
# MAGIC     regionalUnidade = source.regionalUnidade,
# MAGIC     numCrmSolicitante = source.numCrmSolicitante,
# MAGIC     ufCrmSolicitante = source.ufCrmSolicitante,
# MAGIC     nomeConvenio = source.nomeConvenio,
# MAGIC     nomePlano = source.nomePlano,
# MAGIC     nomeMedicoSolicitante = source.nomeMedicoSolicitante,
# MAGIC     dataExameRecente = source.dataExameRecente,
# MAGIC     codigoTuss = source.codigoTuss,
# MAGIC     descricaoTuss = source.descricaoTuss,
# MAGIC     descricaoProcedimento = source.descricaoProcedimento,
# MAGIC     dt_execucao = current_date()/*,
# MAGIC     valorExameCreatinina = source.valorExameCreatinina,
# MAGIC     valorClearance = source.valorClearance*/
# MAGIC when not matched then
# MAGIC   insert (
# MAGIC     idPredicao,
# MAGIC     idPatient,
# MAGIC     idExame,
# MAGIC     idMedico,
# MAGIC     dataNascimento,
# MAGIC     idade,
# MAGIC     sexoPaciente,
# MAGIC     telefoneContato,
# MAGIC     idUnidade,
# MAGIC     cnpjUnidade,
# MAGIC     nomeUnidade,
# MAGIC     regionalUnidade,
# MAGIC     numCrmSolicitante,
# MAGIC     ufCrmSolicitante,
# MAGIC     nomeConvenio,
# MAGIC     nomePlano,
# MAGIC     nomeMedicoSolicitante,
# MAGIC     dataExameRecente,
# MAGIC     codigoTuss,
# MAGIC     descricaoTuss,
# MAGIC     descricaoProcedimento,
# MAGIC     valorExameCreatinina,
# MAGIC     valorClearance,
# MAGIC     dt_execucao
# MAGIC   ) values (
# MAGIC     source.idPredicao,
# MAGIC     source.idPatient,
# MAGIC     source.idExame,
# MAGIC     source.idMedico,
# MAGIC     source.dataNascimento,
# MAGIC     source.idade,
# MAGIC     source.sexoPaciente,
# MAGIC     source.telefoneContato,
# MAGIC     source.idUnidade,
# MAGIC     source.cnpjUnidade,
# MAGIC     source.nomeUnidade,
# MAGIC     source.regionalUnidade,
# MAGIC     source.numCrmSolicitante,
# MAGIC     source.ufCrmSolicitante,
# MAGIC     source.nomeConvenio,
# MAGIC     source.nomePlano,
# MAGIC     source.nomeMedicoSolicitante,
# MAGIC     source.dataExameRecente,
# MAGIC     source.codigoTuss,
# MAGIC     source.descricaoTuss,
# MAGIC     source.descricaoProcedimento,
# MAGIC     source.valorExameCreatinina,
# MAGIC     source.valorClearance,
# MAGIC     source.dt_execucao
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,vw saida
# MAGIC %sql
# MAGIC create or replace view ia.vw_gold_creatinina as 
# MAGIC -- create or replace view ia.vw_diamond_nefrologia_clearance as 
# MAGIC select 
# MAGIC   cast(idPredicao as string) as idPredicao
# MAGIC   , cast(idPatient as string) as idPatient
# MAGIC   , idExame
# MAGIC   , idMedico
# MAGIC   , cast(dataNascimento as timestamp) as dataNascimento
# MAGIC   , idade
# MAGIC   , if(sexoPaciente = 'female', 'F', ' M') as sexoPaciente
# MAGIC   , default.unencrypt(telefoneContato, 0) as telefoneContato
# MAGIC   , idUnidade
# MAGIC   , cnpjUnidade
# MAGIC   , nomeUnidade
# MAGIC   , regionalUnidade
# MAGIC   , default.unencrypt(numCrmSolicitante, 0) as numCrmSolicitante
# MAGIC   , trim(replace(ufCrmSolicitante, '-', '')) as ufCrmSolicitante
# MAGIC   , nomeConvenio
# MAGIC   , nomePlano
# MAGIC   , default.unencrypt(nomeMedicoSolicitante, 0) as nomeMedicoSolicitante
# MAGIC   , cast(dataExameRecente as date) as dataExameRecente
# MAGIC   , codigoTuss
# MAGIC   , descricaoTuss
# MAGIC   , descricaoProcedimento
# MAGIC   , valorExameCreatinina
# MAGIC   , cast(valorClearance as numeric) as valorClearance, -- TODO: ALTERAR PARA ESSA LINHA PARA GARANTIR A CONVERSAO
# MAGIC   dt_execucao  
# MAGIC   -- , valorClearance
# MAGIC from ia.tb_diamond_nefrologia_clearance_saida as crt
# MAGIC where true
# MAGIC and cast(valorClearance as numeric) between 0.1 and 60
# MAGIC  /*and valorExameCreatinina <= 5*/
# MAGIC and nomeConvenio  not rlike r'(?i)(?<!\w)AMIL(?:\s*[-/().A-Za-z0-9]+)?' 
# MAGIC and cast(dataExameRecente as date) <= current_date()

# COMMAND ----------

# DBTITLE 1,Fim execucao
dbutils.notebook.exit("Fim do processo")
