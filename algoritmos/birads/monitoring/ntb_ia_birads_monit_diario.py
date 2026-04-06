# Databricks notebook source
# MAGIC %md
# MAGIC #Install

# COMMAND ----------

import pandas as pd
import warnings
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

from pyspark.sql.types import StringType, DateType, TimestampType, DoubleType, ArrayType, StructType, StructField, IntegerType

warnings.filterwarnings("ignore")

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option('display.max_colwidth', None)

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

# DBTITLE 1,Data Execução Modelo (DateTime)
data_execucao_modelo_dtm = datetime.strptime(data_execucao_modelo, "%Y-%m-%d")

print(f"Data Execução Modelo (DateTime): {data_execucao_modelo_dtm}")

# COMMAND ----------

# DBTITLE 1,Origem
dbutils.widgets.text("origem", "", "Origem")
origem = dbutils.widgets.get("origem")
print(f"Origem: {origem}")

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
# MAGIC # Tabelas

# COMMAND ----------

vw_diamond_mod_birads_complemento = f"{main_catalog}.{environment_tbl}vw_diamond_mod_birads_complemento"
tb_diamond_mod_monitoramento = f"{main_catalog}.{environment_tbl}tb_diamond_mod_monitoramento_enfermeira_navegadora"

# COMMAND ----------

print(f"{'vw_diamond_mod_birads_complemento':45}: {vw_diamond_mod_birads_complemento}")
print(f"{'tb_diamond_mod_monitoramento':45}: {tb_diamond_mod_monitoramento}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Carregando os Dados

# COMMAND ----------

query = ''

if origem == "email":

    query = f"""
    SELECT 
        data_carga as dataExecucaoModelo,
        Unidade_tratado as nomeHospital,
        birads,
        '{origem}' as origem

    FROM ia.vw_enave_df_historico_birads
    WHERE data_carga <= '{data_execucao_modelo}'
    """

if origem == "api":

    query = f"""
    SELECT
        dt_execucao as dataExecucaoModelo,
        emp_nome_unidade as nomeHospital,
        vl_proced_birads as birads,
        '{origem}' as origem

    FROM {vw_diamond_mod_birads_complemento}
    WHERE dt_execucao <= '{data_execucao_modelo}'
    """

df_spark = spark.sql(query)
data = df_spark.toPandas()

data.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Funções: Tratamento e limpeza

# COMMAND ----------

# extrai o ano
def extrair_ano(df, coluna_data):
    df['ano'] = pd.to_datetime(df[coluna_data]).dt.year
    return df

# extrai o mes
def extrair_mes(df, coluna_data):
    df['mes'] = pd.to_datetime(df[coluna_data]).dt.month

    meses_ref = {
        1: 'janeiro',
        2: 'fevereiro',
        3: 'março',
        4: 'abril',
        5: 'maio',
        6: 'junho',
        7: 'julho',
        8: 'agosto',
        9: 'setembro',
        10: 'outubro',
        11: 'novembro',
        12: 'dezembro'
    }
    
    df['mes'] = df['mes'].map(meses_ref)

    return df

# extrai o dia da semana
def extrair_dia_da_semana(df, coluna_data):
    # Extrai o nome do dia da semana em inglês
    df['diaSemana'] = pd.to_datetime(df[coluna_data]).dt.day_name()
    
    dias_semana_traducao = {
        'Monday': 'segunda',
        'Tuesday': 'terça',
        'Wednesday': 'quarta',
        'Thursday': 'quinta',
        'Friday': 'sexta',
        'Saturday': 'sábado',
        'Sunday': 'domingo'
    }
    
    df['diaSemana'] = df['diaSemana'].map(dias_semana_traducao)
    return df

# extrai o dia da semana
def extrair_dia_da_semana_numero(df, coluna_numero):
    # Extrai o nome do dia da semana em inglês
    df['diaSemana'] = df[coluna_numero]
    
    dias_semana_traducao = {
        0: 'segunda',
        1: 'terça',
        2: 'quarta',
        3: 'quinta',
        4: 'sexta',
        5: 'sábado',
        6: 'domingo'
    }
    
    df['diaSemana'] = df['diaSemana'].map(dias_semana_traducao)
    return df

def extrair_semana_do_ano(df, coluna_data):
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    df['semanaInicial'] = ((df[coluna_data] - pd.to_datetime(df[coluna_data].dt.year.astype(str) + '-01-01')).dt.days // 7 + 1)
    return df

def extrair_dia_do_ano(df, coluna_data):
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    df['diaInicial'] = ((df[coluna_data] - pd.to_datetime(df[coluna_data].dt.year.astype(str) + '-01-01')).dt.days + 1)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #Manipulando Dados

# COMMAND ----------

data = extrair_ano(data, 'dataExecucaoModelo')
data = extrair_mes(data, 'dataExecucaoModelo')
data = extrair_dia_da_semana(data, 'dataExecucaoModelo')

data = extrair_dia_do_ano(data, 'dataExecucaoModelo')
data = extrair_semana_do_ano(data, 'dataExecucaoModelo')

# COMMAND ----------

# MAGIC %md
# MAGIC #Funções: Estatísticas

# COMMAND ----------

def sinalizar_pico_continuo_corrigido_diario(df, coluna_data, coluna_unidade, coluna_birads, 
                                             aumento_percentual=150, aumento_absoluto_min=15,
                                             queda_percentual=30, queda_absoluta_min=3):
    # converte a coluna usada como referencia de data para evitar possiveis erros
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    # faz a contagem agrupada de unidade, birads, ano... e coloca na coluna enviosDiario (contagem)
    df_diario = df.groupby([coluna_unidade, coluna_birads, 'ano', 'diaInicial', 'semanaInicial', 'diaSemana', 'origem']).size().reset_index(name='enviosDiario')
    # faz a media acumulada agrupando ano, unidade e birads, usando a coluna enviosDiario
    df_diario['mediaAcumulada'] = df_diario.groupby(['ano', coluna_unidade, coluna_birads])['enviosDiario'].transform(lambda x: x.expanding().mean())
    
    df_diario['sinalizacao'] = 'Estável'
    df_diario['variacaoPercentual'] = np.nan
    df_diario['variacaoAbsoluta'] = np.nan

    i = 0
    while i < len(df_diario):
        birads = df_diario.loc[i, coluna_birads]
        consecutivo = 3 if birads in [-1, 0, 4, 5] else 3
        
        if i + consecutivo <= len(df_diario):            
            sequencia_alta = True
            sequencia_baixa = True             
            for j in range(consecutivo):
                media_acumulada = df_diario.loc[i + j, 'mediaAcumulada']
                envios_diario = df_diario.loc[i + j, 'enviosDiario']
                
                # logica de alta quando o envio e maior que a media
                variacao_absoluta_alta = envios_diario - media_acumulada
                if media_acumulada > 0:
                    variacao_percentual_alta = ((envios_diario / media_acumulada) - 1) * 100
                else:
                    variacao_percentual_alta = np.inf

                # logica de baixa quando o envio e menor que a media
                variacao_absoluta_baixa = media_acumulada - envios_diario
                if envios_diario > 0:
                    variacao_percentual_baixa = ((media_acumulada / envios_diario) - 1) * 100
                else:
                    variacao_percentual_baixa = np.inf

                # verifica se ambas condicoes sao false para nao executar o if e manter sequencia como True
                if variacao_percentual_alta < aumento_percentual or variacao_absoluta_alta < aumento_absoluto_min:
                    sequencia_alta = False
                if variacao_percentual_baixa < queda_percentual or variacao_absoluta_baixa < queda_absoluta_min:
                    sequencia_baixa = False

                if not sequencia_alta and not sequencia_baixa:
                    break
            
            # sinaliza apenas o último dia da sequência se os critérios forem atendidos
            index_to_mark = i + consecutivo - 1
            if sequencia_alta:
                df_diario.loc[index_to_mark, 'sinalizacao'] = 'Aumento'
                df_diario.loc[index_to_mark, 'variacaoAbsoluta'] = variacao_absoluta_alta
                df_diario.loc[index_to_mark, 'variacaoPercentual'] = variacao_percentual_alta
            elif sequencia_baixa:
                df_diario.loc[index_to_mark, 'sinalizacao'] = 'Queda'
                df_diario.loc[index_to_mark, 'variacaoAbsoluta'] = variacao_absoluta_baixa
                df_diario.loc[index_to_mark, 'variacaoPercentual'] = variacao_percentual_baixa
            
            i += consecutivo
        else:
            break
    
    return df_diario

# COMMAND ----------

# def identificar_inatividade_consecutiva(df, coluna_data, coluna_unidade, dias_consecutivos=3, dias_recentes=15):
#     # Converte a coluna de data para datetime
#     df[coluna_data] = pd.to_datetime(df[coluna_data])
    
#     # Garante que cada combinação de unidade e data tenha um único valor de envios
#     df_contado = (df.groupby([coluna_unidade, coluna_data]).size().reset_index(name='envios'))
    
#     # Obtém todas as combinações possíveis de unidade e data
#     todas_datas = pd.date_range(df[coluna_data].min(), df[coluna_data].max()) # dastas corridas sem lacunas
#     unidades = df[coluna_unidade].unique() # lista de unidades
#     # todas combinacoes possiveis de data com unidade
#     index_continuo = pd.MultiIndex.from_product([unidades, todas_datas], names=[coluna_unidade, coluna_data])
    
#     # Reindexa o DataFrame para criar as datas faltantes com envios = 0
#     df_envios = (df_contado.set_index([coluna_unidade, coluna_data]).reindex(index_continuo, fill_value=0).reset_index())
    
#     # Cria uma coluna de flag indicando dias com 0 envios
#     df_envios['inatividade'] = (df_envios['envios'] == 0).astype(int)
    
#     # Verifica inatividade consecutiva
#     df_envios['inatividade_consecutiva'] = (
#         df_envios.groupby(coluna_unidade)['inatividade']
#         .transform(lambda x: x.rolling(dias_consecutivos, min_periods=dias_consecutivos).sum())
#     )
    
#     # Cria uma flag para os últimos `dias_recentes`
#     df_envios['atividade_recente'] = (
#         df_envios.groupby(coluna_unidade)['envios']
#         .transform(lambda x: x.rolling(dias_recentes, min_periods=1).sum())
#     )
    
#     # Calcula variação absoluta e percentual
#     df_envios['variacaoAbsoluta'] = df_envios.groupby(coluna_unidade)['envios'].diff()
#     df_envios['variacaoPercentual'] = (df_envios['variacaoAbsoluta'] / df_envios.groupby(coluna_unidade)['envios'].shift(1) * 100)
#     df_envios['variacaoPercentual'].replace([np.inf, -np.inf], np.nan, inplace=True)  # Lida com divisões por 0
    
#     # Sinaliza unidades com 3 dias consecutivos de 0 envios, sem 0 nos últimos `dias_recentes`
#     df_envios['sinalizacao'] = np.where(
#         (df_envios['inatividade_consecutiva'] == dias_consecutivos) & (df_envios['atividade_recente'] > 0), 'Inatividade', 'Estável')

#     # df_envios['sinalizacao'] = np.where(
#     # (df_envios['inatividade_consecutiva'] == dias_consecutivos) & (df_envios['atividade_recente'] > 0),'Inatividade com Atividade Recente',
#     # np.where((df_envios['inatividade_consecutiva'] == dias_consecutivos) & (df_envios['atividade_recente'] == 0), 'Inatividade Sem Atividade Recente', 'Estável'))

#     # Filtra apenas os dias relevantes para sinalização
#     df_resultado = df_envios[df_envios['sinalizacao'] == 'Inatividade']
    
#     return df_resultado






def identificar_inatividade_consecutiva(df, coluna_data, coluna_unidade, dias_consecutivos=3, dias_recentes=15):
    # Converte a coluna de data para datetime
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    
    # Filtra apenas os dados do ano corrente
    ano_corrente = pd.Timestamp.now().year
    df = df[df[coluna_data].dt.year == ano_corrente]
    
    # Garante que cada combinação de unidade e data tenha um único valor de envios
    df_contado = (df.groupby([coluna_unidade, coluna_data]).size().reset_index(name='envios'))
    
    # Obtém todas as combinações possíveis de unidade e data
    todas_datas = pd.date_range(df[coluna_data].min(), df[coluna_data].max())  # datas corridas sem lacunas
    unidades = df[coluna_unidade].unique()  # lista de unidades
    # todas combinações possíveis de data com unidade
    index_continuo = pd.MultiIndex.from_product([unidades, todas_datas], names=[coluna_unidade, coluna_data])
    
    # Reindexa o DataFrame para criar as datas faltantes com envios = 0
    df_envios = (df_contado.set_index([coluna_unidade, coluna_data]).reindex(index_continuo, fill_value=0).reset_index())
    
    # Cria uma coluna de flag indicando dias com 0 envios
    df_envios['inatividade'] = (df_envios['envios'] == 0).astype(int)
    
    # Verifica inatividade consecutiva
    df_envios['inatividade_consecutiva'] = (
        df_envios.groupby(coluna_unidade)['inatividade']
        .transform(lambda x: x.rolling(dias_consecutivos, min_periods=dias_consecutivos).sum())
    )
    
    # Cria uma flag para os últimos `dias_recentes`
    df_envios['atividade_recente'] = (
        df_envios.groupby(coluna_unidade)['envios']
        .transform(lambda x: x.rolling(dias_recentes, min_periods=1).sum())
    )
    
    # Calcula variação absoluta em relação ao dia anterior
    df_envios['variacaoAbsolutaAnterior'] = df_envios.groupby(coluna_unidade)['envios'].diff()
    
    # Calcula a média histórica acumulada de envios para cada unidade (apenas no ano corrente)
    df_envios['mediaAcumulada'] = df_envios.groupby(coluna_unidade)['envios'].expanding().mean().reset_index(level=0, drop=True)
    
    # Calcula variação absoluta com base na média histórica
    df_envios['variacaoAbsoluta'] = df_envios['envios'] - df_envios['mediaAcumulada']
    
    # Calcula variação percentual com base na média histórica
    df_envios['variacaoPercentual'] = (df_envios['variacaoAbsoluta'] / df_envios['mediaAcumulada']) * 100
    df_envios['variacaoPercentual'].replace([np.inf, -np.inf], np.nan, inplace=True)  # Lida com divisões por 0
    
    # Sinaliza unidades com 3 dias consecutivos de 0 envios, sem 0 nos últimos `dias_recentes`
    df_envios['sinalizacao'] = np.where(
        (df_envios['inatividade_consecutiva'] == dias_consecutivos) & (df_envios['atividade_recente'] > 0), 'Inatividade', 'Estável')

    # Filtra apenas os dias relevantes para sinalização
    df_resultado = df_envios[df_envios['sinalizacao'] == 'Inatividade']
    
    return df_resultado

# COMMAND ----------

# MAGIC %md
# MAGIC #Apontamentos inativos

# COMMAND ----------

inatividade = identificar_inatividade_consecutiva(
    data, 
    coluna_data='dataExecucaoModelo', 
    coluna_unidade='nomeHospital', 
    dias_consecutivos=3, 
    dias_recentes=15
)

inatividade.shape

# COMMAND ----------

# data_limite = datetime.now() - timedelta(days=2)
data_limite = data_execucao_modelo_dtm - timedelta(days=2)

print(f"data_limite: {data_limite}")

# COMMAND ----------

inatividade = inatividade[inatividade['dataExecucaoModelo'] >= data_limite]

if origem == 'email':
    inatividade = inatividade[inatividade['atividade_recente'] >= 30]
else:
    inatividade = inatividade[inatividade['atividade_recente'] >= 10]

inatividade.shape

# COMMAND ----------

inatividade

# COMMAND ----------

inatividade['origem'] = origem
inatividade['birads'] = 'Geral'
inatividade.rename(columns={'dataExecucaoModelo':'dataApontamento', 'envios':'enviosDiario'}, inplace=True)
inatividade = inatividade[['nomeHospital', 'dataApontamento', 'birads', 'enviosDiario', 'variacaoAbsoluta', 'variacaoPercentual', 'mediaAcumulada', 'inatividade_consecutiva', 'atividade_recente', 'sinalizacao', 'origem']]

#inatividade = inatividade.merge(df_auxiliar[['dataApontamento', 'diaInicial']], on='dataApontamento', how='left')
inatividade = extrair_dia_do_ano(inatividade, 'dataApontamento')
inatividade = extrair_dia_da_semana(inatividade, 'dataApontamento')

inatividade.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Apontamentos abruptos

# COMMAND ----------

resultado = sinalizar_pico_continuo_corrigido_diario(
    df=data,
    coluna_data="dataExecucaoModelo",
    coluna_unidade="nomeHospital",
    coluna_birads="birads",
    aumento_percentual=100,
    aumento_absoluto_min=9,
    queda_percentual=100,
    queda_absoluta_min=9
)

monitoramento = resultado[resultado['sinalizacao'] != 'Estável']

monitoramento.shape

# COMMAND ----------

df_auxiliar = data[['ano', 'diaInicial', 'dataExecucaoModelo']].drop_duplicates()
df_auxiliar.rename(columns={'dataExecucaoModelo': 'dataApontamento'}, inplace=True)

monitoramento = monitoramento.merge(df_auxiliar, on=['ano', 'diaInicial'], how='left')

monitoramento = monitoramento[['nomeHospital', 'dataApontamento', 'diaInicial', 'birads', 'enviosDiario', 'variacaoAbsoluta', 'mediaAcumulada', 'variacaoPercentual', 'sinalizacao', 'origem']]

# COMMAND ----------

# data_limite = datetime.now() - timedelta(days=2)

data_limite = data_execucao_modelo_dtm - timedelta(days=2)

df_filtrado = monitoramento[monitoramento['dataApontamento'] >= data_limite]

df_filtrado.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #Concatenando tabelas

# COMMAND ----------

df_filtrado = pd.concat([df_filtrado, inatividade], ignore_index=True)

df_filtrado

# COMMAND ----------

# hoje = datetime.now()
# data_hoje = hoje.strftime('%Y-%m-%d')  # Formato aaaa-mm-dd
# dia_corrido = hoje.timetuple().tm_yday  # numero do dia no ano
# dia_da_semana = hoje.strftime('%A')  # Nome do dia da semana
# ano_corrente = hoje.year

# data_api_ano_corrente = data[pd.to_datetime(data['dataExecucaoModelo']).dt.year == ano_corrente]

# total_envios_api = data[data['dataExecucaoModelo'] == data_hoje].shape[0]

# media_envios_api = data_api_ano_corrente.groupby('dataExecucaoModelo').size().mean()

# variacao_absoluta_api = total_envios_api - media_envios_api
# variacao_percentual_api = (variacao_absoluta_api / media_envios_api) * 100 if media_envios_api > 0 else 0

# sinalizacao_api = "Aumento" if variacao_percentual_api > 0 else "Queda" if variacao_percentual_api < 0 else "Estável"

# linha_api = pd.DataFrame([{
#     'nomeHospital': origem.upper(),
#     'dataApontamento': data_hoje,
#     'diaInicial': dia_corrido,
#     #'diaSemana': dia_da_semana,
#     'birads': 'Geral',
#     'enviosDiario': total_envios_api,
#     'mediaAcumulada': media_envios_api,
#     'variacaoPercentual': variacao_percentual_api,
#     'variacaoAbsoluta': variacao_absoluta_api,
#     'sinalizacao': sinalizacao_api,
#     'origem': origem
# }])

# colunas_01 = ['variacaoAbsoluta', 'variacaoPercentual', 'mediaAcumulada']
# def arredondar_float(df, casas_decimais=2):
#     for coluna in colunas_01:
#         df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
#         df[coluna] = df[coluna].round(casas_decimais)
#         df[coluna] = df[coluna].fillna("não se aplica")
#     return df

# colunas_02 = ['inatividade_consecutiva', 'atividade_recente']
# def transformar_para_int(df, colunas):
#     for coluna in colunas_02:
#         df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
#         df[coluna] = df[coluna].fillna("não se aplica")
#         df[coluna] = df[coluna].apply(lambda x: int(x) if isinstance(x, (float, int)) else x)
#     return df

# df_atualizado = pd.concat([linha_api, df_filtrado], ignore_index=True)
# df_atualizado['dataExecucao'] = datetime.now().date()

# df_atualizado = extrair_dia_da_semana(df_atualizado, 'dataApontamento')
# df_atualizado['dataApontamento'] = pd.to_datetime(df_atualizado['dataApontamento'])
# df_atualizado = arredondar_float(df_atualizado)
# df_atualizado = transformar_para_int(df_atualizado, colunas_02)

# COMMAND ----------

# hoje = datetime.now()
hoje = data_execucao_modelo_dtm
data_hoje = hoje.strftime('%Y-%m-%d')  # Formato aaaa-mm-dd
dia_corrido = hoje.timetuple().tm_yday  # número do dia no ano
dia_da_semana = hoje.strftime('%A')  # Nome do dia da semana
ano_corrente = hoje.year

data_api_ano_corrente = data[pd.to_datetime(data['dataExecucaoModelo']).dt.year == ano_corrente]

total_envios_api = data[data['dataExecucaoModelo'] == data_hoje].shape[0]

media_envios_api = data_api_ano_corrente.groupby('dataExecucaoModelo').size().mean()

# Lógica de variação absoluta e percentual substituída pelo código 2:
variacao_absoluta_alta = total_envios_api - media_envios_api
if media_envios_api > 0:
    variacao_percentual_alta = ((total_envios_api / media_envios_api) - 1) * 100
else:
    variacao_percentual_alta = -100

# Lógica para baixa
variacao_absoluta_baixa = media_envios_api - total_envios_api
if total_envios_api > 0:
    variacao_percentual_baixa = ((media_envios_api / total_envios_api) - 1) * 100
else:
    variacao_percentual_baixa = -100

# Determinação da sinalização (Aumento ou Queda)
if variacao_percentual_alta > 0:
    sinalizacao_api = "Aumento"
elif variacao_percentual_baixa > 0:
    sinalizacao_api = "Queda"
else:
    sinalizacao_api = "Estável"

linha_api = pd.DataFrame([{
    'nomeHospital': origem.upper(),
    'dataApontamento': data_hoje,
    'diaInicial': dia_corrido,
    'birads': 'Geral',
    'enviosDiario': total_envios_api,
    'mediaAcumulada': media_envios_api,
    'variacaoPercentual': variacao_percentual_alta if variacao_percentual_alta > 0 else variacao_percentual_baixa,
    'variacaoAbsoluta': variacao_absoluta_alta if variacao_absoluta_alta > 0 else variacao_absoluta_baixa,
    'sinalizacao': sinalizacao_api,
    'origem': origem
}])

colunas_01 = ['variacaoPercentual', 'mediaAcumulada', 'variacaoAbsoluta']
def arredondar_float(df, casas_decimais=2):
    for coluna in colunas_01:
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
        df[coluna] = df[coluna].round(casas_decimais)
        df[coluna] = df[coluna].fillna("não se aplica")
    return df

colunas_02 = ['inatividade_consecutiva', 'atividade_recente']
def transformar_para_int(df, colunas):
    for coluna in colunas_02:
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
        df[coluna] = df[coluna].fillna("não se aplica")
        df[coluna] = df[coluna].apply(lambda x: int(x) if isinstance(x, (float, int)) else x)
    return df

df_atualizado = pd.concat([linha_api, df_filtrado], ignore_index=True)
df_atualizado['dataExecucao'] = datetime.now().date()

df_atualizado = extrair_dia_da_semana(df_atualizado, 'dataApontamento')
df_atualizado['dataApontamento'] = pd.to_datetime(df_atualizado['dataApontamento'])
df_atualizado = arredondar_float(df_atualizado)
df_atualizado = transformar_para_int(df_atualizado, colunas_02)

# COMMAND ----------

df_atualizado

# COMMAND ----------

# MAGIC %md
# MAGIC #Salvando tabelas

# COMMAND ----------

columns_to_cast = ["mediaAcumulada", "variacaoAbsoluta", "variacaoPercentual"]
df_atualizado[columns_to_cast] = df_atualizado[columns_to_cast].astype(str)

# COMMAND ----------

df_atualizado.rename(
    columns={
        "dataExecucao": "dt_execucao",
        "nomeHospital": "emp_nome_unidade",
        "dataApontamento": "dt_apontamento",
        "birads": "vl_proced_birads",
        "enviosDiario": "cnt_envio_diario",
        "mediaAcumulada": "num_media_acumulada",
        "variacaoAbsoluta": "num_variacao_absoluta",
        "variacaoPercentual": "num_variacao_percentual",
        "sinalizacao": "cod_sinalizacao",
        "origem": "cod_origem",
    },
    inplace=True,
)

# COMMAND ----------

schema = StructType(
    [
        StructField("dt_execucao", DateType(), True),
        StructField("emp_nome_unidade", StringType(), True),
        StructField("dt_apontamento", DateType(), True),
        StructField("vl_proced_birads", StringType(), True),
        StructField("cnt_envio_diario", IntegerType(), True),
        StructField("num_media_acumulada", StringType(), True),
        StructField("num_variacao_absoluta", StringType(), True),
        StructField("num_variacao_percentual", StringType(), True),
        StructField("cod_sinalizacao", StringType(), True),
        StructField("cod_origem", StringType(), True),
    ]
)

spark_df = spark.createDataFrame(df_atualizado[schema.fieldNames()], schema)
spark_df.display()

# COMMAND ----------

tb_diamond_mod_monitoramento

# COMMAND ----------

table_exists = spark.catalog.tableExists(tb_diamond_mod_monitoramento)

if table_exists:
    spark.sql(f"""
        DELETE FROM {tb_diamond_mod_monitoramento}
        WHERE dt_apontamento = '{data_execucao_modelo}'
        AND cod_origem = '{origem}'
    """).display()

# COMMAND ----------

spark_df.write.mode("append").option("overwriteSchema", "true").option("path", table_location(tb_diamond_mod_monitoramento)).saveAsTable(tb_diamond_mod_monitoramento)

# COMMAND ----------

dbutils.notebook.exit("Fim da execução!")

# COMMAND ----------

