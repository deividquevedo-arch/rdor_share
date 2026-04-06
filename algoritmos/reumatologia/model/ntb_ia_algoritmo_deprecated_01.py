# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Instalações

# COMMAND ----------

# MAGIC %pip install --quiet nltk

# COMMAND ----------

# MAGIC %pip install --quiet striprtf

# COMMAND ----------

# MAGIC %pip install --quiet rapidfuzz

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Importações

# COMMAND ----------

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.pipeline import make_pipeline
from sklearn.metrics import classification_report
import nltk
from nltk.corpus import stopwords

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

import re

import nltk
from nltk import word_tokenize
from nltk import ngrams
from nltk.tokenize import sent_tokenize

import matplotlib.pyplot as plt

from unicodedata import normalize

from rapidfuzz import fuzz

from striprtf.striprtf import rtf_to_text
from nltk import ngrams


# COMMAND ----------

import pandas as pd

pd.set_option('display.max_colwidth', None)

# COMMAND ----------

nltk.download('rslp')
nltk.download('stopwords')
nltk.download('punkt')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Definições Globais

# COMMAND ----------

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

table_name_saida = f"{environment_tbl}tbl_gold_modelo_{project_id}_saida"
print(table_name_saida)

# COMMAND ----------

table_name_saida_wrk = f"{environment_tbl}tbl_wrk_gold_modelo_{project_id}_saida"
print(table_name_saida_wrk)

# COMMAND ----------

# MAGIC %md
# MAGIC # Métodos para uso no modelo

# COMMAND ----------

def retira_acentos(value):
    value = value.lower()
    value = normalize('NFKD', value).encode('ASCII','ignore').decode('ASCII')
    return value 

# COMMAND ----------

def remove_special_characters(value):
    pattern = r'[^a-zA-Z0-9\s]'
    value = re.sub(pattern, '', value)
    return value

# COMMAND ----------

def transform_rtf(value):
    try:
            
        if '{\\rtf1' in value[:100]:
            return rtf_to_text(value)
        else:
            return value
    except Exception as e:
        return value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lógica
# MAGIC > Captura de palavras chaves para aceite e exclusão de score de probabilidade do laudos ser de um paciente reumatológico

# COMMAND ----------


keys = ['edema tecidos moles'
, 'edema osseo subcondral'
, 'diminuicao espaco articular simetrico'
, 'diminuicao espaco articular concentrico'
, 'erosoes marginais'
, 'erosoes periarticulares'
, 'erosoes pannus'
, 'periostite'
, 'entesite'
, 'entesopatia'
,' dactilite'
, 'sacroileite'
, 'acro-osteolise'
, 'sinovite'
, 'sinovite cronica'
, 'proliferacao membrana sinovial'
, 'capsulite'
, 'artropatia erosiva'
, 'les'
, 'espondilite anquilosante'
, 'osteoartrite'
, 'lupus eritematoso sistemico'
, 'artrose tibiotalar'
, 'tendinopatia distal'
]

# incluidos em 14/05/2024 - análise não médica
keys_generic = ['febre reumatica'
, 'gota'
, 'fibromialgia'
, 'artrite reumatoide'
, 'lupus'
, 'polimialgia reumatica'
, 'doença de behcet'
, 'sindrome de sjogren'
, 'vasculites'
, 'fibromialgia'
, 'esclerodermia'
, 'bursite'
, 'tendinopatia'
, 'tendinite'
, 'peritendinite'
, 'entesite']

keyword_ignore = ['nao ha erosoes'
, 'ausencia'
, 'nao ha'
, 'nao identificado'
, 'nao identificam sinais'
, 'sem sinais'
, 'nao se observa'
, 'nao observa'
]

sentences_ignores = ['webris'
        , ' rtf '
        , ' rtf'
        , 'este laudo pode não estar completo aqui em função do padrão de arquivo "rtf" utilizado.'
        , 'laudo pode nao completo aqui funcao padrao arquivo rtf utilizado recomendamos visualizacao sistema radiologia webris'
        , 'laudo pode nao completo'
        , 'visualizacao rtf'
        , 'nao completo visualizacao rtf'
        , 'completo visualizacao rtf']

# COMMAND ----------

# def score_keyword_find(value):
#     keys = ['edema tecidos moles'
#     , 'edema osseo subcondral'
#     , 'diminuicao espaco articular simetrico'
#     , 'diminuicao espaco articular concentrico'
#     , 'erosoes marginais'
#     , 'erosoes periarticulares'
#     , 'erosoes pannus'
#     , 'periostite'
#     , 'entesite'
#     , 'entesopatia'
#     ,' dactilite'
#     , 'sacroileite'
#     , 'acro-osteolise'
#     , 'sinovite'
#     , 'sinovite cronica'
#     , 'proliferacao membrana sinovial'
#     , 'capsulite'
#     , 'artropatia erosiva'
#     , 'les'
#     , 'espondilite anquilosante'
#     , 'osteoartrite'
#     , 'lupus eritematoso sistemico'
#     , 'artrose tibiotalar'
#     , 'tendinopatia distal'
#     ]

#     # incluidos em 14/05/2024 - análise não médica
#     keys_generic = ['febre reumatica'
#     , 'gota'
#     , 'fibromialgia'
#     , 'artrite reumatoide'
#     , 'lupus'
#     , 'polimialgia reumatica'
#     , 'doença de behcet'
#     , 'sindrome de sjogren'
#     , 'vasculites'
#     , 'fibromialgia'
#     , 'esclerodermia'
#     , 'bursite'
#     , 'tendinopatia'
#     , 'tendinite'
#     , 'peritendinite'
#     , 'entesite']

#     keyword_ignore = ['nao ha erosoes'
#     , 'ausencia'
#     , 'nao ha'
#     , 'nao identificado'
#     , 'nao identificam sinais'
#     , 'sem sinais'
#     , 'nao se observa'
#     , 'nao observa'
#     ]

    
#     if 'conclusao' in value:
#         value = value.split('conclusao')[-1]


#     sentences = sent_tokenize(value)
#     sentences = value.split('\n')

#     # Print each sentence
#     diagnosis = []
#     similarities = []
#     generic_similarities = []
#     similarities_ignore = []
#     scores = []
#     scores_ignore = []
#     for sentence in sentences:
#         for k in keys:
#             if k in sentence:
#                 score_ =  fuzz.token_sort_ratio(k, sentence)
#                 if score_ > 20:
#                     intra_score = []
#                     for sentence_ in sentence.split():
#                         intra_score.append(fuzz.token_sort_ratio(k, sentence_))

#                     score_ = max(intra_score)

#                 score_ignore = 0.0
                
#                 for k_exc in keyword_ignore:
#                     if k_exc in sentence:
#                         score_ignore = fuzz.token_sort_ratio(k_exc, sentence)
#                         similarities_ignore.append({
#                             "sentence": sentence
#                             , "keyword": k_exc
#                             , "score": score_ignore
#                         })
#                         scores_ignore.append(score_ignore)

#                 if not score_ignore:

#                     if k == "les":
#                         if score_ > 90.0:
                        
#                             similarities.append({
#                                 "sentence": sentence
#                                 , "keyword": k
#                                 , "score": score_
#                             })
#                             if len(k) > 0:
#                                 diagnosis.append(k)
#                             scores.append(score_)
#                     else:
                            
#                         similarities.append({
#                             "sentence": sentence
#                             , "keyword": k
#                             , "score": score_
#                         })
#                         if len(k) > 0:
#                             diagnosis.append(k)
#                         scores.append(score_)
                    
#         for k in keys_generic:
#             if k in sentence:
#                 score_ =  fuzz.token_sort_ratio(k, sentence)

#                 score_ignore = 0.0
                
#                 for k_exc in keyword_ignore:
#                     if k_exc in sentence:
#                         score_ignore = fuzz.token_sort_ratio(k_exc, sentence)

#                 if score_ignore < 10.0:
#                     generic_similarities.append({
#                         "sentence": sentence
#                         , "keyword": k
#                         , "score": score_
#                     })
                
            
#         # else:
#         #     ##### Usar somente nGrams não foi bom! 
#         #     for n in range(1,6):
#         #         grams = ngrams(value.split(), n)
#         #         for gram in grams:
#         #             if len(gram) > 2:
#         #                 for k in keys:
#         #                     score1 = fuzz.ratio(k, gram)
#         #                     score2 = fuzz.token_sort_ratio(k, gram)
#         #                     similarities.append(score1)
#         #                     similarities.append(score2)

#         #                     if max(similarities) > 70.0:
#         #                         return max(similarities)

#     # return max(similarities)
#     if scores == []:
#         return {
#         "chaves": None,
#         "encontrado": None,
#         "encontrado_ignore": None,
#         "probabilidade": 0.0,
#         "probabilidade_ignore": 0.0,
#         "encontrado_total": None,
#         "termos_genericos": []
#     }
    
#     if scores_ignore == []:
#         scores_ignore = [0] 
#     # else:
#     #     scores_ignore = max(scores_ignore)


#     return {
#         "chaves": list(set(diagnosis)),
#         "encontrado": similarities,
#         "encontrado_ignore": similarities_ignore,
#         "probabilidade": (sum(scores)/len(scores))-(sum(scores_ignore)/len(scores_ignore)),
#         "probabilidade_ignore":  max(scores_ignore) ,
#         "encontrado_total": len(similarities),
#         "termos_genericos": generic_similarities
#     }



# COMMAND ----------

# import pprint


# laudo_test = """ressonancia magnetica da bacia
 
# tecnica
# sequencias ponderadas em t1 t2 e dp em aquisicoes multiplanares
 
# analise
#  estruturas osseas de morfologia conservada com leve heterogeneidade inespecifica da medular ossea ausencia de fraturas ou de lesoes osseas focais de aspecto agressivo
#  articulacao femoroacetabular ausencia de derrame articular ou erosoes condrais profundas
#  articulacoes sacroiliacas irregularidades subcondrais no terco medio da borda iliaca media direita com esclerose subcondral e duvidoso foco de edema adjacente outro diminuto foco de edema subcondral na porcao inferior da borda iliaca direita os achados sao inespecificos sendo necessaria correlacao clinica e laboratorial
#  sinfise pubica alteracoes degenerativas
#  estruturas musculotendineas entesopatia da origem da banda iliotibial bilateral peritendinite dos gluteos minimos com edema da gordura interposta entre a banda iliotibial e o trocanter maior
#  bursas trocantericas nao ha distensao liquida significativa
#  planos superficiais sem alteracoes significativas
 
# conclusao
#  irregularidades subcondrais na articulacao sacroiliaca direita predominando na borda iliaca com esclerose subcondral e tenues focos de edema subcondral os achados sao inespecificos sendo necessaria correlacao clinica e laboratorial
#  entesopatia da origem da banda iliotibial bilateral 
#  peritendinite dos gluteos minimos com edema da gordura interposta entre a banda iliotibial e o trocanter maior
# este laudo pode nao estar completo aqui em funcao do padrao de arquivo rtf utilizado recomendamos a sua visualizacao no sistema da radiologia webris
# """



# # if 'conclusao' in laudo_test:
# #     laudo_test = laudo_test.split('conclusao')[-1]


# # # sentences = sent_tokenize(laudo_test)
# # sentences = laudo_test.split('\n')
# # print(sentences)

# pprint.pp(score_keyword_find(laudo_test))

# COMMAND ----------

# def remove_reserved_words(value):
#     words = ['webris'
#             , ' rtf '
#             , ' rtf'
#             , 'laudo pode nao completo aqui funcao padrao arquivo rtf utilizado recomendamos visualizacao sistema radiologia webris'
#             , 'laudo pode nao completo'
#             , 'visualizacao rtf'
#             , 'nao completo visualizacao rtf'
#             , 'completo visualizacao rtf']
    
#     for word in words:
#         value = value.replace(word, '')

#     return value

# COMMAND ----------



# COMMAND ----------

def search(value):

    sentences = sent_tokenize(value)

    findings = []
    scores = [0.0]
    scores_ignore = []

    for sentence_ in sentences:
        sentence = sentence_.lower().strip()

        findings_ = []
        scores_ = []

        for k in keys:
            n = len(k.split())
            ngram_len = ngrams(sentence.split(), n)



            for grams in ngram_len:
                gram_sentence = ' '.join(grams)
                s2 = fuzz.token_ratio(k, gram_sentence)

                d = {}
                if s2 > 50:
                    d = {
                        "fraseAvaliada": sentence,
                        "fraseParte": gram_sentence,
                        "fraseProcurado": k,
                        "valorProbabilidade": s2,
                    }

                    ignores = []
                    ignoress = []

                    for ki in keyword_ignore:
                        si_ = fuzz.token_sort_ratio(ki, gram_sentence)
                        ignores.append({
                            "fraseAvaliada": sentence,
                            "fraseParte": gram_sentence,
                            "fraseProcuradoIgnorar": ki,
                            "valorProbabilidadeIgnorar": si_
                        })
                        ignoress.append(si_)
                            
                        # findings.append({
                        #     "fraseAvaliada": sentence,
                        #     "fraseParte": gram_sentence,
                        #     "fraseProcurado": k,
                        #     "fraseProcuradoIgnorar": ki,
                        #     "valorProbabilidadeIgnorar": si_,
                        #     "valorProbabilidade": s2,
                        # })
                        # scores.append(s2)
                    
                    if ignoress != []:
                        for i in ignores:
                            if i['valorProbabilidadeIgnorar'] == max(ignoress):
                                d['fraseParteIgnorar'] = i['fraseParte']
                                d['fraseProcuradoIgnorar'] = i['fraseProcuradoIgnorar']
                                d['valorProbabilidadeIgnorar'] = i['valorProbabilidadeIgnorar']
                    
                    findings_.append(d)
                    scores_.append(s2)

        for i in findings_:
            if max(scores_) == i['valorProbabilidade']:
                d_ = i
                d_['valorProbabilidadeCorrigido'] = i['valorProbabilidade'] - i['valorProbabilidadeIgnorar']
                findings.append(i)
                scores.append(d_['valorProbabilidadeCorrigido'])
                


    return {"findings": findings, "score": max(scores)}

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataset de entrada

# COMMAND ----------

df = spark.sql(f"select * from ia.{table_name_entrada} where  datParticao = current_date() ").toPandas()

# COMMAND ----------

print(df.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC # Processando dataset

# COMMAND ----------

df['laudoExame_transformado'] = df['laudoExame'].apply(transform_rtf)

# COMMAND ----------

df['laudoExame_findings'] = df['laudoExame_transformado'].apply(search)

# COMMAND ----------

df['score'] = df['laudoExame_findings'].apply(lambda x: x['score'])
df['findings'] = df['laudoExame_findings'].apply(lambda x: x['findings'])

# COMMAND ----------

plt.figure(figsize=(10, 6))
plt.hist(df['score'], bins=20, edgecolor='k', alpha=0.7)
plt.title('Histogram of Scores')
plt.xlabel('Score')
plt.ylabel('Frequency')
plt.grid(False)
plt.show()

# COMMAND ----------



# COMMAND ----------

df['laudoExame'] = df['laudoExame_transformado']
df.drop(columns=['laudoExame_transformado', 'laudoExame_findings'], inplace=True)
df.rename(columns={'findings': 'encontrado', 'score': 'valorProbabilidade'}, inplace=True)


# COMMAND ----------

dfs = spark.createDataFrame(df)

# COMMAND ----------

print(f"Carregando dados na tabela work: {table_name_saida_wrk}")

# COMMAND ----------

dfs.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(f"ia.{table_name_saida_wrk}")

# COMMAND ----------

x = spark.sql(f"select count(*) as total from ia.{table_name_saida_wrk} where dataProcessamento = current_date()").collect()
print(x)

# COMMAND ----------

x = spark.sql(f"delete from ia.{table_name_saida} where dataExecucaoModelo = current_date()").collect()
print(x)

# COMMAND ----------

spark.sql(f""" insert into ia.{table_name_saida}
          select distinct 
                idPredicao, 
                idPatient, 
                idExame, 
                current_timestamp() as tmpPredicao,
                valorProbabilidade, 
                dataProcessamento
            from ia.{table_name_saida_wrk}
            where idExame not in (select idExame from ia.{table_name_saida})
          """).display()

# COMMAND ----------

x = spark.sql(f"select count(*) as total from ia.{table_name_saida} where dataExecucaoModelo = current_date()").collect()
print(x)
