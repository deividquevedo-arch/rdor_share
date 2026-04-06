# Databricks notebook source
# MAGIC %md
# MAGIC # Instalações

# COMMAND ----------

# MAGIC %pip install --quiet nltk

# COMMAND ----------

# MAGIC %pip install --quiet striprtf

# COMMAND ----------

# MAGIC %pip install --quiet rapidfuzz

# COMMAND ----------

# MAGIC %pip install spacy

# COMMAND ----------

!python -m spacy download pt_core_news_lg

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Importações

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


# COMMAND ----------

import spacy 

# COMMAND ----------

nlp = spacy.load('pt_core_news_lg')

# COMMAND ----------

import pandas as pd

pd.set_option('display.max_colwidth', None)

# COMMAND ----------

nltk.download('rslp')
nltk.download('stopwords')
nltk.download('punkt')

# COMMAND ----------



# COMMAND ----------

# instanciando stemmer
# stemmer = nltk.stem.RSLPStemmer()

# instanciando stopwords
stopwords = set(nltk.corpus.stopwords.words('portuguese'))

# COMMAND ----------

# MAGIC %md
# MAGIC #Definições Globais

# COMMAND ----------

table_name_entrada = f"tbl_gold_reumatologia_entrada"
table_name_saida = f"tbl_gold_reumatologia_saida"

print(table_name_entrada)
print(table_name_saida)

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

def score_keyword_find(value):
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

    
    if 'conclusao' in value:
        value = value.split('conclusao')[-1]


    sentences = sent_tokenize(value)

    # Print each sentence
    similarities = []
    generic_similarities = []
    similarities_ignore = []
    scores = []
    scores_ignore = []
    for sentence in sentences:
        for k in keys:
            if k in sentence:
                score_ =  fuzz.token_sort_ratio(k, sentence)
                if score_ > 20:
                    intra_score = []
                    for sentence_ in sentence.split():
                        intra_score.append(fuzz.token_sort_ratio(k, sentence_))

                    score_ = max(intra_score)

                score_ignore = 0.0
                
                for k_exc in keyword_ignore:
                    if k_exc in sentence:
                        score_ignore = fuzz.token_sort_ratio(k_exc, sentence)
                        similarities_ignore.append({
                            "sentence": sentence
                            , "keyword": k_exc
                            , "score": score_ignore
                        })
                        scores_ignore.append(score_ignore)

                if not score_ignore:

                    if k == "les":
                        if score_ > 90.0:
                        
                            similarities.append({
                                "sentence": sentence
                                , "keyword": k
                                , "score": score_
                            })

                            scores.append(score_)
                    else:
                            
                        similarities.append({
                            "sentence": sentence
                            , "keyword": k
                            , "score": score_
                        })

                        scores.append(score_)
                    
        for k in keys_generic:
            if k in sentence:
                score_ =  fuzz.token_sort_ratio(k, sentence)

                score_ignore = 0.0
                
                for k_exc in keyword_ignore:
                    if k_exc in sentence:
                        score_ignore = fuzz.token_sort_ratio(k_exc, sentence)

                if score_ignore < 10.0:
                    generic_similarities.append({
                        "sentence": sentence
                        , "keyword": k
                        , "score": score_
                    })
                
            
        # else:
        #     ##### Usar somente nGrams não foi bom! 
        #     for n in range(1,6):
        #         grams = ngrams(value.split(), n)
        #         for gram in grams:
        #             if len(gram) > 2:
        #                 for k in keys:
        #                     score1 = fuzz.ratio(k, gram)
        #                     score2 = fuzz.token_sort_ratio(k, gram)
        #                     similarities.append(score1)
        #                     similarities.append(score2)

        #                     if max(similarities) > 70.0:
        #                         return max(similarities)

    # return max(similarities)
    if scores == []:
        return {
        "encontrado": None,
        "encontrado_ignore": None,
        "probabilidade": 0.0,
        "probabilidade_ignore": 0.0,
        "encontrado_total": None,
        "termos_genericos": []
    }
    
    if scores_ignore == []:
        scores_ignore = 0 
    else:
        scores_ignore = max(scores_ignore)


    return {
        "encontrado": similarities,
        "encontrado_ignore": similarities_ignore,
        "probabilidade": max(scores),
        "probabilidade_ignore": scores_ignore ,
        "encontrado_total": len(similarities),
        "termos_genericos": generic_similarities
    }



# COMMAND ----------

def remove_reserved_words(value):
    words = ['webris'
            , ' rtf '
            , ' rtf'
            , 'laudo pode nao completo aqui funcao padrao arquivo rtf utilizado recomendamos visualizacao sistema radiologia webris'
            , 'laudo pode nao completo'
            , 'visualizacao rtf'
            , 'nao completo visualizacao rtf'
            , 'completo visualizacao rtf']
    
    for word in words:
        value = value.replace(word, '')

    return value

# COMMAND ----------

def verify_content(content):
    try:

        keyword_ignores = ['nao ha erosoes'
        , 'ausencia'
        , 'nao ha'
        , 'nao identificado'
        , 'nao identificam sinais'
        , 'sem sinais'
        , 'sem alterações'
        , 'nao se observa'
        , 'nao observa'
        ]


        keyword_searchs = ['edema tecidos moles'
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
        ]


        sentences = sent_tokenize(content, language='portuguese')

        ignores = []
        accepts = []
        scores = []
        for sentence in sentences:
            s1 = nlp(sentence)

            for keyword_ignore in keyword_ignores:
                ignores.append(s1.similarity(nlp(keyword_ignore)))

            for keyword_search in keyword_searchs:
                accepts.append(s1.similarity(nlp(keyword_search)))

            scores.append(
                (max(accepts) - max(ignores))
            )
            

        return max(scores)
    except:
        return 0


# COMMAND ----------

# MAGIC %md
# MAGIC # Dataset de entrada

# COMMAND ----------

# df = spark.sql(f"select * from ia.{table_name_entrada} where  dataProcessamento = current_date() ").toPandas()
df = spark.sql(f"select * from ia.{table_name_entrada} limit 1000 ").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Processando dataset

# COMMAND ----------

df['laudoExame_limpo'] = df['laudoExame'].apply(transform_rtf)

# COMMAND ----------

df['laudoExame_limpo'] = df['laudoExame_limpo'].apply(retira_acentos)

# COMMAND ----------

df['laudoExame_limpo'] = df['laudoExame_limpo'].apply(remove_special_characters)

# COMMAND ----------

df['laudoExame_limpo'] = df['laudoExame_limpo'].apply(remove_reserved_words)

# COMMAND ----------

df['score'] = df['laudoExame_limpo'].apply(verify_content)

# COMMAND ----------

df['score'] = (df['laudoExame']
            .pipe(transform_rtf)
            .pipe(retira_acentos)
            .pipe(remove_special_characters)
            .pipe(remove_reserved_words)
            .pipe(verify_content)
           )

# COMMAND ----------

# df['laudoExame_sent_tokenize'] = df['laudoExame_semCharEspecial'].apply(lambda x: sent_tokenize(x, language='portuguese'))
# # df['laudoExame_sent_tokenize'] = df['laudoExame_semCharEspecial'].apply(sent_tokenize)

# COMMAND ----------

display(df)

# COMMAND ----------

import matplotlib.pyplot as plt

# Assuming df is already defined and contains the relevant data
df['score'].hist(bins=30, figsize=(15, 10))
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

print(ignores)
print(accepts)
print("=" * 30 )
print(max(ignores))
print(max(accepts))
print("=" * 30 )
print(scores)


# COMMAND ----------

x = verify_negatives( "nao ha erosoes em Exame realizado com aquisição helicoidal \n volumétrica, meio de contraste venoso.")
x

# COMMAND ----------

display(df_test_senttoken)

# COMMAND ----------

df_test_senttoken["negatives"] = df_test_senttoken["laudoExame_sent_token"].apply(lambda x: verify_negatives(x))

# COMMAND ----------

df_test_senttoken.dtypes

# COMMAND ----------

display(df_test_senttoken)

# COMMAND ----------

display(df.head())

# COMMAND ----------

df['rawRespostaModelo'] = df['laudoExame_semAcentos'].apply(score_keyword_find)

# COMMAND ----------

df['rawRespostaModelo_encontrado'] = df['rawRespostaModelo'].apply(lambda x: x['encontrado'])
df['valorProbabilidade'] = df['rawRespostaModelo'].apply(lambda x: x['probabilidade'])
df['totalOcorrencia'] = df['rawRespostaModelo'].apply(lambda x: x['encontrado_total'])

df['rawRespostaModelo_probabilidade_ignore'] = df['rawRespostaModelo'].apply(lambda x: x['probabilidade_ignore'])
df['rawRespostaModelo_encontrado_ignore_total'] = df['rawRespostaModelo'].apply(lambda x: x['encontrado_ignore'])

df['rawRespostaModelo_encontrado_generico'] = df['rawRespostaModelo'].apply(lambda x: x['termos_genericos'])

# COMMAND ----------

df_filtered = df[df['valorProbabilidade'] > 0.0]

# COMMAND ----------

dfs = spark.createDataFrame(df_filtered)

# COMMAND ----------

dfs.write.format("delta").mode("overwrite").saveAsTable("ia.tbl_tmp_reumatologia_algoritmo")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from ia.tbl_gold_modelo_reumatologia_saida where dataExecucaoModelo = CURRENT_DATE()

# COMMAND ----------

dfs = spark.sql("select valorProbabilidade, idExame from ia.tbl_tmp_reumatologia_algoritmo  where dataExecucaoModelo = CURRENT_DATE() ").toPandas()
dfs.plot.hist()

# COMMAND ----------

# %sql
# insert into ia.tbl_gold_modelo_reumatologia_saida (
#   idPredicao ,
#   idPatient ,
#   idExame ,
#   tmpPredicao ,
#   valorProbabilidade ,
#   totalOcorrencia ,
#   rawRespostaModelo ,
#   dataExecucaoModelo
# )
# select 
#   idPredicao ,
#   idPatient ,
#   idExame ,
#   current_timestamp() ,
#   valorProbabilidade ,
#   totalOcorrencia ,
#   rawRespostaModelo,
#   CURRENT_DATE() as dataExecucaoModelo
# from ia.tbl_tmp_reumatologia_algoritmo
# where valorProbabilidade > 90
# order by valorProbabilidade desc 

# COMMAND ----------



# COMMAND ----------

# %sql
# select round(valorProbabilidade,0), count(idExame)
# from ia.tbl_gold_modelo_reumatologia_saida
# group by all
# order by 2 desc 

# COMMAND ----------

# %sql
# select *
# from ia.tbl_gold_modelo_reumatologia_saida
# limit 100 
