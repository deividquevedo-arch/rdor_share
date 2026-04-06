# Databricks notebook source
# MAGIC %md
# MAGIC # Enfermeiras Navegadoras
# MAGIC ## Bi-Rads
# MAGIC > Notebook do processamento do modelo bi-ras do projeto enfermeiras navegadoras

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Objetos
# MAGIC - Tabela de entrada: ia.tb_diamond_mod_birads_entrada
# MAGIC - Tabela de saída: ia.tb_diamond_mod_birads_saida

# COMMAND ----------

!pip install -q \
    striprtf==0.0.22 \
    fuzzywuzzy \
    gensim \
    nltk

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import gensim
import nltk
import numpy as np
import os
import pandas as pd
import re
import unicodedata
import warnings

from datetime import datetime
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from striprtf.striprtf import rtf_to_text
from tqdm.notebook import tqdm

# COMMAND ----------

pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

# COMMAND ----------

nltk.download('all')
warnings.filterwarnings("ignore")
tqdm.pandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #Parâmetros

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
# MAGIC # Variáveis com os nomes das tabelas

# COMMAND ----------

# DBTITLE 1,Define variáveis com os nomes das tabelas
tb_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_entrada"
tb_saida = f"{main_catalog}.{environment_tbl}tb_diamond_mod_birads_saida"

# COMMAND ----------

# DBTITLE 1,Exibe valores das variáveis com nomes das tabelas
print(f"{'tb_entrada':30}: {tb_entrada}")
print(f"{'tb_saida':30}: {tb_saida}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga dos dados

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {tb_saida}").display()

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tb_saida} (
  id_predicao STRING,
  dt_execucao DATE,
  id_exame STRING,
  id_paciente STRING,
  id_medico_encaminhador STRING,
  id_unidade STRING,
  dt_exame TIMESTAMP,
  vl_proced_birads LONG,
  proced_laudo_exame STRING,
  proced_laudo_limpo STRING,
  fl_integracao_concluida BOOLEAN
)
USING delta
PARTITIONED BY (dt_execucao)
LOCATION '{table_location(tb_saida)}'
""").display()

# COMMAND ----------

def remover_acentos_caracteres_especiais(texto):
    while True:
        # Normaliza os caracteres para remover acentos
        texto_normalizado = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
        texto_normalizado = texto_normalizado.lower()

        # Remove caracteres especiais
        texto_normalizado = re.sub(r'\b\d{1,2}[.,]?\d*\s*(?:cm|h)\b', '', texto_normalizado)
        texto_normalizado = re.sub(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b', '', texto_normalizado)
        texto_normalizado = re.sub(r'\b0(\d)\b', r'\1', texto_normalizado)
        texto_normalizado = re.sub(r'\b(?:ha|existem|existe|esquerda|esquerdo|controle|direito|direita)\s*(?!\(\s*\d+\s+\w+\s*\))\d+\b', '', texto_normalizado)
        texto_normalizado = re.sub(r'\b\d{1,2}\s*mes(?:es)?\b', '', texto_normalizado)
        texto_normalizado = re.sub(r'^\d+\s*-\s*', '', texto_normalizado, flags=re.MULTILINE)
        texto_normalizado = re.sub(r'(\d+)\s*-\s*[A-Za-z]{1,2}\b', r'\1', texto_normalizado)
        texto_normalizado = re.sub(r'(\d+)\s*-\s*', r'\1', texto_normalizado)
        #texto_normalizado = re.sub(r'\d+- ', '', texto_normalizado)
        texto_normalizado = re.sub(r'-', '_', texto_normalizado)
        texto_normalizado = re.sub(r'\niv', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'\nv', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'\niii', ' ', texto_normalizado)
        #### Substituindo algarismos romanos acompanhados com o .
        texto_normalizado = re.sub(r'/\b[iIvVxXlLcCdDmM]+\./g', ' ', texto_normalizado)
        #### Substituindo numeros acompanhados com anos e horas
        texto_normalizado = re.sub(r'\b\d+\s*(?:horas?|anos?)\b', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'\nii', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'\ni', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'\n', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'[^\w\s()]', '', texto_normalizado)
        texto_normalizado = re.sub(r'\s+', ' ', texto_normalizado)
        texto_normalizado = re.sub(r' _ ', ' ', texto_normalizado)
        texto_normalizado = re.sub(r'categoria', ' categoria ', texto_normalizado)
        #### Adição para normalização do caso de erro 30/11/23
        texto_normalizado = re.sub(r'(\d+)\s*(mes(es)?)', ' ', texto_normalizado)
        texto_normalizado = texto_normalizado.strip().lower()
        texto_normalizado = re.sub(r'\((\d+)\s+(bi_rads)\)', r'(\2 \1)', texto_normalizado)
        texto_normalizado = re.sub(r'[^\w\s]', '', texto_normalizado)

        if texto_normalizado == texto:
            break

        texto = texto_normalizado

    return texto_normalizado

# COMMAND ----------

def clean_text(text):
    
    text = re.sub(r'bi rads', 'bi_rads', text)
    text = re.sub(r'bi rad ', 'bi_rads', text)
    text = re.sub(r'\b\d+\s*(?:mm|x|cm)\b', ' ' , text)
    text = text.replace('relatorio baseado 5a edicao acr bi_rads', ' ')
    text = text.replace('colegio americano radiologia bi_rads 5 ed 2013', ' ')
    text = re.sub(r'colegio americano (?:de )?radiologia bi_rads 5.? ed(?:icao)? 2013', ' ', text)
    
     
    # Remove "relatorio baseado" + 18 caracteres
    pattern1 = r"relatorio baseado.{25}"
    text = re.sub(pattern1, " ", text)
    # Remove "colegio americano{prox 30 caracteres}" 17/10/2025
    pattern2 = r"colegio americano.{30}"
    text = re.sub(pattern2, " ", text)
    
    #text = text.replace(' 5a edicao bi_rads', ' ')
    # Separa em palavras/tokens
    tokens = word_tokenize(text)
    
    # Converte para lower case
    tokens = [w.lower() for w in tokens]
    
    # Remove stopwords
    stop_words = set(stopwords.words('portuguese'))
    tokens = [w for w in tokens if w not in stop_words]
    
    #tokens = ' '.join(tokens)
    
    return tokens

# COMMAND ----------

def get_num_birads(string):

    #### Padrão regex para encontrar números
    padrao = r'\d+'

    #### Encontrar todos os números na string
    numeros_encontrados = re.findall(padrao, string)

    return numeros_encontrados

# COMMAND ----------

def get_next_words(sentence):
    sentence = sentence.replace('bi _rads','bi_rads')
    #### Refinamento 15/12/23 processo identificação
    #sentence = re.sub(r'\b(?:bi[ _] ?rads|:bi[_] ?rads|birads_acr|acr[ _]?birads|bi[_] ?rads|birad|acrbi[ _]?rads)\b', 'bi_rads' , sentence)
    sentence = re.sub(r'\b(?:bi[ _]?rads|br[ _]?rads|:bi[_]?rads|:br[_]?rads|birads_acr|brads_acr|acr[ _]?(?:bi|br)rads|acrbi[ _]?rads|acr b[a-z]{1}[ _]?rads)\b', ' bi_rads ', sentence)
    list_of_words = sentence.split()
    next_word = {'key_words': [], 'associated_words': []}
    
    for idx, word in enumerate(list_of_words):
        if re.match(r'birads', word) or re.match(r'bi_rads', word) or re.match(r'categoria', word):
            next_word['key_words'].append(word)
            
            associated_words = []
            for i in range(idx - 3, idx + 3):
                if 0 <= i < len(list_of_words) and i != idx:
                    associated_words.append(list_of_words[i])
            
            next_word['associated_words'].append(associated_words)
    
    return next_word

# COMMAND ----------

def generate_birads(df):

    df['birads'] = ''

    for ind, val in tqdm(df.iterrows(), total=df.shape[0]):
        try:
            sentence = val['resultados']
            result = get_next_words(sentence)

            key_words = result['key_words']
            palavras_associadas = result['associated_words']
            palavras_associadas.append(key_words)

            b = ' '.join(eval(str([elemento for lista in palavras_associadas for elemento in lista])))
            b = b.replace(' i ', ' 1 ').replace(' ii ', ' 2 ').replace(' iii ', ' 3 ')\
            .replace(' iv ', ' 4 ').replace(' v ', ' 5 ')#.replace('negativo', ' 9 ')
            
            #print("\nPalavras:", palavras_associadas)
            pattern = [list(p) for p in get_num_birads(b)]
            #print("\nPalavras:", pattern)
            #category = get_highest_category(eval(str(pattern)))
            pattern = [z for z in pattern if len(z) < 2]
            pattern = [elemento for lista in pattern for elemento in lista]
            #print("\n", val['resultados'])
            #print("\n", key_words, palavras_associadas)
            lista_de_elemento = []
            
            if (pattern == []) and (len(key_words) == 0):
                pass;
            
            if (pattern == []) and (len(key_words) > 0):
                pattern = ['0']

            for elemento in pattern:
                if (elemento in [9, '9']) and (len(pattern) == 2):
                    lista_de_elemento = [int(-1)]
                    break;
                elif elemento in [3, 4, 5, "3", "4", "5"]:
                    lista_de_elemento.append(int(elemento))
                elif elemento in [0, 1, 2, 6, "0", "1", "2", "6"]:
                    lista_de_elemento.append(int(elemento))
                else:
                    lista_de_elemento.append(int(-1))
            
            categoria = max(lista_de_elemento) 
            df.at[ind,'birads'] = categoria
        except:
            categoria = int(-1)
            df.at[ind,'birads'] = categoria

        #print("Lista elemento:", lista_de_elemento) 
        #print("Categoria:", df.at[ind,'birads'])
    return df


# COMMAND ----------

def compare(df):
    for ind, val in tqdm(df.iterrows(), total=df.shape[0]):
        if val['birads_old'] == val['birads']:
            valor = 'SIMIL'
            df.at[ind ,'match'] = valor
        else:
            valor = 'DIF'
            df.at[ind ,'match'] = valor

    return df

# COMMAND ----------

# DBTITLE 1,Carrega os dados da tabela de entrada
query = f"""
    select distinct
        dt_execucao                 as dataExecucao,
        id_predicao                 as idPredicao,
        id_exame                    as idExame,
        num_pedido_integracao       as idAtendimento,
        id_unidade                  as idHospital,
        emp_nome_unidade            as nomeHospital,
        emp_regional_unidade        as regional,
        emp_cnpj_unidade            as cnpjHospital,
        num_porta_entrada           as portaEntrada,
        num_motivo_entrada          as motivoEntrada,
        id_medico_encaminhador      as idMedico,
        nome_medico_encaminhador    as medicoEncaminhador,
        doc_crm_medico_encaminhador as numCrm,
        uf_crm_medico_encaminhador  as ufCrm,
        tel_medico_encaminhador     as telefoneMedicoEncaminhador,
        id_paciente                 as idPaciente,
        nome_paciente               as nomePaciente,
        num_cpf_paciente            as cpfPaciente,
        dt_nascimento_paciente      as dataNascimentoPaciente,
        gen_sexo_paciente           as sexoPaciente,
        tel_contato_paciente        as telefoneContato,
        cod_ans_convenio            as codigoAnsConvenio,
        nome_convenio               as nomeConvenio,
        nome_plano                  as nomePlano,
        dt_exame                    as dataExame,
        proced_nome_exame           as tituloExame,
        proced_laudo_exame          as laudoExame,
        cod_origem                  as origem
    from {tb_entrada}
    where dt_execucao = date('{data_execucao_modelo}')
"""
df_spark = spark.sql(query)
df_source = df_spark.toPandas()

# COMMAND ----------

df_source.shape

# COMMAND ----------

# df_source.head()

# COMMAND ----------

# Ajustando data
df_source['data_ajustada'] = pd.to_datetime(df_source['dataExame'])

# COMMAND ----------

def laudo_rtf_to_text(laudo):
    """Converte laudo para texto, se estiver no formato rtf"""
    is_rtf = laudo.strip().lower().startswith("{\\rtf1\\")

    if is_rtf:
        return rtf_to_text(laudo, errors="ignore")

    return laudo

# COMMAND ----------

df_source["resultados_rtf"] = df_source["laudoExame"]

# COMMAND ----------

df_source["resultados"] = df_source["resultados_rtf"].apply(remover_acentos_caracteres_especiais)

# COMMAND ----------

df_source['resultados'] = df_source['resultados'].progress_apply(lambda x: clean_text(x))

# COMMAND ----------

df_source['resultados'] = df_source['resultados'].apply(lambda x: ' '.join(x))

# COMMAND ----------

df_source = generate_birads(df_source)

# COMMAND ----------

df_source[["birads"]].value_counts()

# COMMAND ----------

df_enave = df_source

# COMMAND ----------

tb_saida

# COMMAND ----------

# DBTITLE 1,Remove os registros carregados no dia
spark.sql(f"""
    delete from {tb_saida}
    where dt_execucao = '{data_execucao_modelo}'
""").display()

# COMMAND ----------

# DBTITLE 1,Carrega o histórico
df_historico = spark.sql(f"""
  select
    id_predicao             as idPredicao,
    dt_execucao             as dataExecucao,
    id_exame                as idExame,
    id_paciente             as idPaciente,
    id_medico_encaminhador  as idMedico,
    id_unidade              as idHospital,
    dt_exame                as dataExame,
    vl_proced_birads        as birads,
    proced_laudo_exame      as observacoes,
    proced_laudo_limpo      as resultados,
    fl_integracao_concluida as flgIntegracaoConcluida
  from {tb_saida}
""").toPandas()

# COMMAND ----------

df_historico.shape

# COMMAND ----------

df_report = pd.merge(df_enave, df_historico, on='idExame', how="outer", indicator=True).query('_merge=="left_only"')[['idExame']]
df_report = df_enave.loc[df_enave['idExame'].isin(df_report['idExame'].to_list())]

# COMMAND ----------

df_report.shape

# COMMAND ----------

# DBTITLE 1,Adiciona data de carga
df_report["dataExecucao"] = datetime.strptime(data_execucao_modelo, "%Y-%m-%d").date()
df_report["observacoes"] = df_report["resultados"]

# COMMAND ----------

df_report["birads"] = df_report["birads"].astype(int)

# COMMAND ----------

# DBTITLE 1,Define o schema do DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, LongType

schema = StructType([
    StructField("id_predicao", StringType(), True),
    StructField("dt_execucao", DateType(), True),
    StructField("id_exame", StringType(), True),
    StructField("id_paciente", StringType(), True),
    StructField("id_medico_encaminhador", StringType(), True),
    StructField("id_unidade", StringType(), True),
    StructField("dt_exame", TimestampType(), True),
    StructField("vl_proced_birads", LongType(), True),
    StructField("proced_laudo_exame", StringType(), True),
    StructField("proced_laudo_limpo", StringType(), True),
])

# COMMAND ----------

df_report.rename(
    columns={
        "idPredicao": "id_predicao",
        "dataExecucao": "dt_execucao",
        "idExame": "id_exame",
        "idPaciente": "id_paciente",
        "idMedico": "id_medico_encaminhador",
        "idHospital": "id_unidade",
        "dataExame": "dt_exame",
        "birads": "vl_proced_birads",
        "observacoes": "proced_laudo_exame",
        "resultados": "proced_laudo_limpo",
    },
    inplace=True,
)

# COMMAND ----------

spark_df = spark.createDataFrame(
    df_report[[
        'id_predicao',
        'dt_execucao',
        'id_exame',
        'id_paciente',
        'id_medico_encaminhador',
        'id_unidade',
        'dt_exame',
        'vl_proced_birads',
        'proced_laudo_exame',
        'proced_laudo_limpo',
    ]],
    schema=schema
)

# COMMAND ----------

tb_saida

# COMMAND ----------

# DBTITLE 1,Grava os dados na tabela de saída
spark_df.write.partitionBy("dt_execucao").mode("append").option("mergeSchema", "true").saveAsTable(tb_saida)

# COMMAND ----------

optimize_table(tb_saida)

# COMMAND ----------

spark.sql(f"""
    select dt_execucao, count(*) as qtd
    from {tb_saida}
    group by dt_execucao
    order by dt_execucao desc
""").display()


# COMMAND ----------

spark.sql(f"""
    select dt_execucao, vl_proced_birads, count(*) as qtd
    from {tb_saida}
    where dt_execucao = '{data_execucao_modelo}'
    group by all
    order by
        dt_execucao desc,
        vl_proced_birads
""").display()


# COMMAND ----------

tb_saida
