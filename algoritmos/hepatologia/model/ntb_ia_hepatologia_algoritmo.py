# Databricks notebook source
# MAGIC %md
# MAGIC # Instalação de libs

# COMMAND ----------

!pip install -q \
    spacy \
    fuzzywuzzy \
    nltk \
    unidecode \
    gensim \
    striprtf==0.0.22 \
    tqdm \
    openpyxl

# COMMAND ----------

!python -m spacy download 'pt_core_news_lg'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import math
import matplotlib.pyplot as plt
import nltk
import numpy as np
import pandas as pd
import pytz
import re
import spacy
import string
import unidecode

from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
from gensim.models import phrases, word2vec
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize, RegexpTokenizer
from nltk.util import ngrams
from pyspark.sql.types import StructType, StructField, ArrayType, BooleanType, DateType, DoubleType, IntegerType, StringType, TimestampType
from sklearn.feature_extraction.text import CountVectorizer
from spacy.matcher import Matcher
from striprtf.striprtf import rtf_to_text
from tqdm.notebook import tqdm
from unicodedata import normalize

# COMMAND ----------

data_hora_inicio = datetime.now(pytz.timezone('America/Sao_Paulo'))
print(data_hora_inicio)

# COMMAND ----------

tqdm.pandas()
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('punkt_tab')
stop_words = stopwords.words('portuguese')

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

# DBTITLE 1,Define variáveis com nomes das tabelas
tbl_entrada = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_entrada"
tbl_saida = f"{main_catalog}.{environment_tbl}tb_diamond_mod_hepatologia_saida"

# COMMAND ----------

# DBTITLE 1,Exibe valores das variáveis com nomes das tabelas
print(f"{'tbl_entrada':15}: {tbl_entrada}")
print(f"{'tbl_saida':15}: {tbl_saida}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração do spacy

# COMMAND ----------

nlp = spacy.load("pt_core_news_lg", disable=["ner"])
nlp.add_pipe("sentencizer", config={"punct_chars": ["\n"]}, before="parser")
nlp.max_length = 1500000

# COMMAND ----------

nlp.pipe_names

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração de display

# COMMAND ----------

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 230)

# COMMAND ----------

# MAGIC %md
# MAGIC # Stop words

# COMMAND ----------

# DBTITLE 1,Adiciona mais stop words
new_stop_words = [
    "direito",
    "direita",
    "esquerdo",
    "esquerda",
    "cm",
    "x"
]
stop_words.extend(new_stop_words)

# COMMAND ----------

# DBTITLE 1,Stop words que serão mantidas
stop_words = set(stop_words)
operators = set(("não", "nao", "há", "ha", "sem"))
stop_words = set(stop_words) - operators

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# DBTITLE 1,retira_acentos
def retira_acentos(texto):
    if not isinstance(texto, str):
        return None
    return normalize('NFKD', texto.lower()).encode('ASCII','ignore').decode('ASCII')

# COMMAND ----------

# DBTITLE 1,tipo_exame
def tipo_exame(texto):
    if not isinstance(texto, str):
        return

    texto = texto.lower()
    
    # EXAMES
    if (
        texto.find("tomografia") >= 0
        or texto.find("tc") >= 0
        or texto.find("tomografiacomputadorizada") >= 0
        or texto.find("ttomografia computadorizada") >= 0
        or texto.find("enterotomografia computadorizada") >= 0
        or texto.find("tomofrafia computadorizada") >= 0
        or texto.find("angiotomografia computadorizada") >= 0
        or texto.find("tomo comput.") >= 0
        or texto.find("enterotomografia") >= 0
    ):
        return "tomografia computadorizada abdomen"
    elif (
        texto.find("ressonancia") >= 0
        or texto.find("rm") >= 0
        or texto.find("rnm") >= 0
    ):
        return "ressonancia magnetica abdomen"
    else:
        return "ultrassonografia do abdomen/figado"

# COMMAND ----------

# DBTITLE 1,retira_pontuacao_stopwords
def retira_pontuacao_stopwords(texto):
    texto = texto.replace("\n", " xxenterxx ")

    texto = texto.lower()

    # tokenização
    texto_token = word_tokenize(texto, language="portuguese")

    lista = [
        palavra
        for palavra in texto_token
        if palavra not in string.punctuation
        and palavra != "\r"
        and palavra not in stop_words
    ]

    texto_limpo = " ".join([str(elemento) for elemento in lista])

    texto_limpo = texto_limpo.replace(" xxenterxx ", "\n")

    return texto_limpo

# COMMAND ----------

# DBTITLE 1,remove_final_laudo
def remove_final_laudo(texto):
    texto = texto.lower()

    final_laudo = [
        "laudo pode nao completo aqui funcao padrao arquivo rtf utilizado recomendamos visualizacao sistema radiologia webris",
        "laudo pode nao completo",
        "visualizacao rtf",
        "nao completo visualizacao rtf",
        "completo visualizacao rtf",
        'este laudo pode não estar completo aqui em função do padrão de arquivo "rtf" utilizado. recomendamos a sua visualização no sistema da radiologia "webris".'
    ]

    for sentence in final_laudo:
        texto = texto.replace(sentence, "")

    return texto

# COMMAND ----------

# DBTITLE 1,preprocess_text
def preprocess_text(text):
    """
    Remove linhas em branco do texto.
    """
    return "\n".join([line for line in text.splitlines() if line.strip()])

# COMMAND ----------

# DBTITLE 1,corrige_orgaos
def corrige_orgaos(texto):
    texto = texto.lower()
    texto = texto.replace("atenuafigado", "atenua figado")
    texto = texto.replace("ffigado", "figado")
    texto = texto.replace("lfigado", "figado")
    texto = texto.replace("figado-rim", "figado - rim")
    texto = texto.replace("figado.nao", "figado nao")
    texto = texto.replace("figado.o", "figado o")
    texto = texto.replace("figados", "figado")
    texto = texto.replace("intra-cavitfigado", "intra - cavit figado")
    texto = texto.replace("figadow", "figado")
    texto = texto.replace("gastroplastiafigado", "gastroplastia figado")
    texto = texto.replace("wrfigado", "figado")
    texto = texto.replace("hemifigado", "hemi figado")
    texto = texto.replace("zfigado", "figado")
    texto = texto.replace("fi gado", "figado")
    texto = texto.replace("normais", "normal")
    texto = texto.replace("dimensoes", "dimensao")
    texto = texto.replace("lirads:4", "lirads 4")

    return texto

# COMMAND ----------

def remove_plurais(texto):
    texto = texto.lower()

    texto = texto.replace("contornos", "contorno")
    texto = texto.replace("lobulados", "lobulado")
    
    return texto

# COMMAND ----------

# DBTITLE 1,tokenizer
def tokenizer(texto):
    doc = nlp(texto)

    tokens = [token.text for token in doc]

    return tokens

# COMMAND ----------

# MAGIC %md
# MAGIC # Seleção de dados

# COMMAND ----------

tbl_entrada

# COMMAND ----------

# DBTITLE 1,query
query = f"""
    SELECT
        dt_execucao as dataExecucaoModelo,
        id_predicao as idPredicao,
        id_exame as idExame,
        id_paciente as id,
        dt_exame as dt_exame,
        proced_nome_exame as tipo_exame,
        proced_laudo_exame_original as laudoExameOriginal

    from {tbl_entrada}

    where dt_execucao = date('{data_execucao_modelo}')
      and fl_laudo_duplicado = 1
"""

df_spark = spark.sql(query)
df = df_spark.toPandas()

# COMMAND ----------

total_records = len(df.index)
total_records

# COMMAND ----------

# DBTITLE 1,shape entrada
df.shape

# COMMAND ----------

display(df.head())

# COMMAND ----------

# DBTITLE 1,columns
df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento de dados

# COMMAND ----------

# DBTITLE 1,Converte datas
df['dt_exame'] = df['dt_exame'].apply(pd.to_datetime)

# COMMAND ----------

# DBTITLE 1,tipo_exame
df['tipo_exame'] = df['tipo_exame'].apply(retira_acentos)
df['tipo_exame'] = df['tipo_exame'].apply(tipo_exame)

df = df.dropna(subset=['tipo_exame'])

# COMMAND ----------

# DBTITLE 1,aplica funções
df.loc[:, 'laudo_exame_modificado'] = df['laudoExameOriginal'].apply(remove_final_laudo)
df.loc[:, 'laudo_exame_modificado'] = df['laudo_exame_modificado'].apply(preprocess_text)
df.loc[:, 'laudo_exame_modificado'] = df['laudo_exame_modificado'].apply(retira_acentos)
df.loc[:, 'laudo_exame_modificado'] = df['laudo_exame_modificado'].apply(retira_pontuacao_stopwords)
df.loc[:, 'laudo_exame_modificado'] = df['laudo_exame_modificado'].apply(corrige_orgaos)
df.loc[:, 'laudo_exame_modificado'] = df['laudo_exame_modificado'].apply(remove_plurais)

# COMMAND ----------

# DBTITLE 1,tokenizer
df['laudoTokens'] = df['laudo_exame_modificado'].apply(tokenizer)

# COMMAND ----------

df_hepato = df

# COMMAND ----------

# MAGIC %md
# MAGIC # Análise Laudos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listas
# MAGIC - Palavras-chave
# MAGIC - Problemas do fígado
# MAGIC - Órgãos
# MAGIC - Palavras irrelevantes

# COMMAND ----------

# DBTITLE 1,Palavras-chave laudo
palavras_chave_laudo = [
    "figado",
    "hepat",
    "vias biliares",
    "vesicula biliar"
]

# COMMAND ----------

# DBTITLE 1,Problemas fígado
problemas_figado_novo = [
    "baco aumentado",
    "cavernoma porta",
    "circulacao colateral",
    "cirrose",
    "congestao passiva cronica figado",
    "contorno irregular",
    "degeneracao gordurosa figado ",
    "dilatacao vias-biliares",
    "doenca alcoolica figado",
    "doenca hepatica",
    "encefalopatia hepatica",
    "esclerose hepatica",
    "fibrose esclerose alcoolicas ",
    "fibrose",
    "figado gorduroso alcoolico",
    "hepatite",
    "hipertensao portal",
    "hipertrofia lobo caudado",
    "infarto figado",
    "insuficiencia hepatica alcoolica",
    "insuficiencia hepatica",
    "lirads 4",
    "lirads 5",
    "lobulado",
    "necrose hemorragica ",
    "nodulo hipervascular",
    "reduzido",
    "sindrome obstrucao sinusoidal hepatica",
    "trombose veia porta",
    "varizes esofagianas",
    "varizes gastricas",
    "veia porta dilatada",

    # Posso incluir na v4?
    "hepatopatia cronica", 
    "microlitiase",
    "multiplos calculos",
    # "esplenomegalia", --removido na v5
]

# COMMAND ----------

# DBTITLE 1,Lista de Orgãos
# Se o nome do orgão contiver mais de uma palavra, separar com "." exemplo: "vesicula.biliar"
# E adicionar um replace na função corrige_orgaos
orgaos = [
    "figado",
    "baco",
    "bexiga",
    "endometrio",
    "esofago",
    "estomago",
    "ovario",
    "ovarios",
    "pancreas",
    "reto",
    "rim",
    "rins",
    "utero",
    # Posso considerar como um orgão na v4?
    "vesicula biliar",
    "vias biliares",
]

# COMMAND ----------

# DBTITLE 1,Lista de palavras irrelevantes
palavras_irrelevantes = [
    "ausencia",
    "nao ha"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento dos laudos

# COMMAND ----------

# DBTITLE 1,matcher_builder
def matcher_builder(key: str, list_of_terms, *, regex=False) -> Matcher:
    _matcher = Matcher(nlp.vocab)

    _patterns = []

    for term in list_of_terms:
        _pattern = []

        for idx, item in enumerate(term.split()):
            _item = item

            if idx > 0:
                _pattern.append({"IS_SPACE": True, "OP": "*"})

            if regex:
                _item = {"REGEX": item}

            _pattern.append({"LOWER": _item})

        _patterns.append(_pattern)

    _matcher.add(key, _patterns)

    return _matcher

# COMMAND ----------

# DBTITLE 1,monta_blocos_spacy
def monta_blocos_spacy(laudo):
    _blocos = []

    _doc = nlp(laudo)
    _matcher_orgaos = matcher_builder("Orgaos", orgaos)
    _found_matches = _matcher_orgaos(_doc)
    
    for _sent in _doc.sents:
        _found_matches = _matcher_orgaos(_sent)

        # Se encontrar algum órgão
        if len(_found_matches):
            _bloco_nlp = nlp(" ".join(_blocos))
            yield _bloco_nlp

            _blocos = []
        
        _blocos.append(_sent.text.replace("\n", ""))

    if len(_blocos):
        _bloco_nlp = nlp(" ".join(_blocos))
        yield _bloco_nlp

# COMMAND ----------

# DBTITLE 1,extrai_termos_v2
def extrai_termos(
    doc: spacy.tokens.doc.Doc,
    matcher: spacy.matcher.matcher.Matcher,
    *,
    return_dict = False,
    matcher_irrelevants: spacy.matcher.matcher.Matcher = None,
    tokens_before = 0,
    tokens_after = 0
):
    _found_matches = matcher(doc)
    _founds = []

    for _match_id, _start, _end in _found_matches:
        # string_id = nlp.vocab.strings[_match_id]
        _span = doc[_start:_end]
        _before = doc[_start - tokens_before:_start]
        _after = doc[_end:_end + tokens_after]

        if not return_dict:
            _founds.append(_span.text)
            continue
        
        _irrelevant_words = []

        if matcher_irrelevants:
            _irrelevant_matches = matcher_irrelevants(_before) + matcher_irrelevants(_after)
            _irrelevant_words = [doc[_start:_end].text for _match_id, _start, _end in _irrelevant_matches]

        _found = {
            "problema": _span.text,
            "anteriores": _before.text,
            "posteriores": _after.text,
            "palavras_irrelevantes": list(set(_irrelevant_words)),
            "relevante": True if len(_irrelevant_words) == 0 else False
        }

        _founds.append(_found)

    return _founds

# COMMAND ----------

# DBTITLE 1,analisa_blocos_spacy
def analisa_blocos_spacy(blocos):
    _dic_default = {
        "bloco": "",
        "orgaos_encontrados": [""],
        "palavras_chave": [""],
        "problemas_encontrados": [
            {
                "problema": "",
                "anteriores": "",
                "posteriores": "",
                "palavras_irrelevantes": [],
                "relevante": False,
            }
        ],
        "relevante": False,
    }

    _blocos_analisados = []

    _matcher_orgaos = matcher_builder("Orgaos", orgaos)
    _matcher_palavras_chave = matcher_builder("PalavrasChave", palavras_chave_laudo, regex=True)
    _matcher_problemas = matcher_builder("Problemas", problemas_figado_novo)
    _matcher_palavras_irrelevantes = matcher_builder("PalavrasIrrelevantes", palavras_irrelevantes)

    for _bloco in blocos:
        _dic_bloco = _dic_default.copy()
        _dic_bloco["bloco"] = _bloco.text
        _dic_bloco["orgaos_encontrados"] = list(set(extrai_termos(_bloco, _matcher_orgaos)))
        
        _palavras_chave_encontradas = list(set(extrai_termos(_bloco, _matcher_palavras_chave)))
        _dic_bloco["palavras_chave"] = _palavras_chave_encontradas

        _problemas_encontrados = []
        if len(_palavras_chave_encontradas):
            _problemas_encontrados = extrai_termos(
                _bloco,
                _matcher_problemas,
                return_dict=True,
                matcher_irrelevants = _matcher_palavras_irrelevantes,
                tokens_before=3,
                tokens_after=3,
            )

            if len(_problemas_encontrados):
                _problemas_encontrados = _problemas_encontrados
                del _dic_bloco["problemas_encontrados"]
                _dic_bloco["problemas_encontrados"] = _problemas_encontrados

        _qtd_problemas_relevantes_bloco = len(list(filter(lambda x: x["relevante"], _problemas_encontrados)))
        _dic_bloco["relevante"] = True if _qtd_problemas_relevantes_bloco> 0 else False

        _blocos_analisados.append(_dic_bloco)

    _qtd_problemas_laudo = len(list(filter(lambda x: x["relevante"], _blocos_analisados)))
    _flg_relevante = True if _qtd_problemas_laudo > 0 else False

    return (_blocos_analisados, _flg_relevante)


# COMMAND ----------

# DBTITLE 1,processa_laudo_spacy_v2
def processa_laudo_spacy(laudo):
  _laudo_pre_proc = preprocess_text(laudo)
  _blocos = monta_blocos_spacy(_laudo_pre_proc)
  _blocos = analisa_blocos_spacy(_blocos)

  return _blocos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa a função analisa_laudo no dataframe de pacientes/laudos

# COMMAND ----------

# DBTITLE 1,analisa_laudos
def analisa_laudos(row):
    analise_blocos, flg_relevante = processa_laudo_spacy(row["laudo_exame_modificado"])
    
    row["analise_blocos"] = analise_blocos
    row["fl_relevante"] = flg_relevante

    return row

# COMMAND ----------

display(df_hepato.head())

# COMMAND ----------

# DBTITLE 1,Aplica a função analisa_laudos
df_hepato = df_hepato.apply(analisa_laudos, axis=1)

# COMMAND ----------

df_hepato.shape

# COMMAND ----------

display(df_hepato.head())

# COMMAND ----------

df_hepato.rename(
    columns={
        "dataExecucaoModelo": "dt_execucao",
        "idPredicao": "id_predicao",
        "idExame": "id_exame",
        "id": "id_paciente",
        "dt_exame": "dt_exame",
        "tipo_exame": "proced_nome_exame",
        "laudoExameOriginal": "proced_laudo_exame_original",
        "laudo_exame_modificado": "proced_laudo_exame",
        "laudoTokens": "proced_laudo_tokens",
        "analise_blocos": "analise_blocos",
        "fl_relevante": "fl_relevante",
    },
    inplace=True,
)

# COMMAND ----------

# df_hepato.rename(
#     columns={
#         "id": "idPaciente",
#         "laudo_exame_modificado": "laudoExame",
#         "tipo_exame": "tipoExame",
#         "dt_exame": "dataExame",
#         # "laudo_tokens": "laudoTokens",
#     },
#     inplace=True,
# )

# COMMAND ----------

display(df_hepato.head())

# COMMAND ----------

df_hepato['fl_relevante'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Salva os dados na tabela de saída

# COMMAND ----------

tbl_saida

# COMMAND ----------

# DBTITLE 1,Schema da tabela de saída
schema = StructType([
    StructField('dt_execucao', DateType(), True), 
    StructField('id_predicao', StringType(), True), 
    StructField('id_exame', StringType(), True), 
    StructField('id_paciente', StringType(), True), 
    StructField('proced_laudo_exame', StringType(), True), 
    StructField('proced_laudo_tokens', ArrayType(StringType(), True), True), 
    StructField('analise_blocos',
        ArrayType(
            StructType([
                StructField('bloco', StringType(), True), 
                StructField('orgaos_encontrados', ArrayType(StringType(), True), True), 
                StructField('palavras_chave', ArrayType(StringType(), True), True), 
                StructField('problemas_encontrados', ArrayType(
                    StructType([
                        StructField('anteriores', StringType(), True), 
                        StructField('palavras_irrelevantes', ArrayType(StringType(), True), True), 
                        StructField('posteriores', StringType(), True), 
                        StructField('problema', StringType(), True), 
                        StructField('relevante', BooleanType(), True)
                    ]),
                True),
            True), 
            StructField('relevante', BooleanType(), True)
        ]), True),
    True), 
    StructField('fl_relevante', BooleanType(), True)
])

spark_df = spark.createDataFrame(df_hepato[schema.fieldNames()], schema)
# spark_df.display()


# COMMAND ----------

# DBTITLE 1,Exclui os registros carregados no mesmo dia
table_exists = spark.catalog.tableExists(tbl_saida)

if table_exists:
    spark.sql(f"""
        delete from {tbl_saida}
        where dt_execucao = date('{data_execucao_modelo}')
    """).display()

# COMMAND ----------

# from math import ceil
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# NUM_RECORDS = 1500

# # total_records = spark_df.count()
# num_partitions = ceil(total_records / NUM_RECORDS)

# split_dfs = []
# for i in range(num_partitions):
#     split_df = spark_df.withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))) \
#         .filter((F.col("row_num") > i * NUM_RECORDS) & (F.col("row_num") <= (i + 1) * NUM_RECORDS)) \
#         .drop("row_num")
#     split_dfs.append(split_df)

# COMMAND ----------

# for split_df in split_dfs:
#     split_df.write.mode("append").option("mergeSchema", "true").partitionBy("dt_execucao").saveAsTable(tbl_saida)

# COMMAND ----------

# DBTITLE 1,Grava os dados na tabela de saída
spark_df.write.mode("append").option("mergeSchema", "true").partitionBy("dt_execucao").saveAsTable(tbl_saida)

# COMMAND ----------

optimize_table(tbl_saida)

# COMMAND ----------

spark.sql(f"""
    select
        dt_execucao,
        count(1) as qtd,
        sum(if(fl_relevante = 1, 1, 0)) as qtd_relevante
    from {tbl_saida}
    group by all
    order by
        dt_execucao desc
""").display()

# COMMAND ----------

# DBTITLE 1,Verifica duplicidades
spark.sql(f"""
    select
        id_exame,
        count(1) as qtd
    from {tbl_saida}
    group by all
    having qtd > 1
""").display()

# COMMAND ----------

data_hora_fim = datetime.now(pytz.timezone('America/Sao_Paulo'))

time_diff_minutes = (data_hora_fim - data_hora_inicio).total_seconds() / 60

print(data_hora_inicio)
print(data_hora_fim)
print(time_diff_minutes)

# COMMAND ----------

dbutils.notebook.exit("Fim do processamento!")

# COMMAND ----------



# COMMAND ----------

spark.sql(f"select * from {tbl_saida} where fl_relevante = true").display()

# COMMAND ----------


