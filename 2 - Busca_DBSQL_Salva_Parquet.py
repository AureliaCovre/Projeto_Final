import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from conector_mysql import Interface_db_mysql

"""
    descrição:
        Faz a leitura dos dados no database SQL
        Realiza a criação de dataframes no framework Spark
        Salva os dados lidos em arquivos Parquet
    @author:
        Aurelia Covre
        João Victor Guimarães
        Kely Fernandes Rodrigues
        Ricardo Rowedder
        Robson Motta
"""

# INTERFACE COM O MYSQL WORKBENCH:

# Cria uma interface do usuário com o banco MySQL
caminho = "gs://arquivos_parquet_bucket/arquivos_parquet/"
try:
    interface_mysql = Interface_db_mysql("root","projeto-final","35.198.38.124","projeto_final")
except Exception as e:
    print('Erro na conexao ao MySQL: ', e)

# ----------------------------------------------LEITURA DOS DADOS NO DATABASE MYSQL---------------------------------------------------------
try:
    print('Leitura dos dados da tabela geracaoDistribuida')    
    dados_geracao_distribuida = interface_mysql.select("*","geracaoDistribuida","")
    df_geracao_distribuida = pd.DataFrame(dados_geracao_distribuida)
except Exception as e:
    print('Erro na leitura da tabela geracaoDistribuida: ', e)

try:    
    print('Leitura dos dados da tabela tarifaMediaFornecimento')    
    dados_tarifa_media = interface_mysql.select("*","tarifaMediaFornecimento","")
    df_tarifa_media = pd.DataFrame(dados_tarifa_media)
except Exception as e:
    print('Erro na leitura da tabela tarifaMediaFornecimento: ', e)

try:    
    print('Leitura dos dados da tabela tarifaResidencial...')    
    dados_tarifa_residencial = interface_mysql.select("*","tarifaResidencial","")
    df_tarifa_residencial = pd.DataFrame(dados_tarifa_residencial)
except Exception as e:
    print('Erro na leitura da tabela tarifaResidencial: ', e)

try:    
    print('Leitura dos dados da tabela empreendimentosGD') 
    dados_empreendimentos = interface_mysql.select("*","empreendimentosGD","")
    df_empreendimentos_gd = pd.DataFrame(dados_empreendimentos)
except Exception as e:
    print('Erro na leitura da tabela empreendimentosGD: ', e)
    
# ----------------------------------------------INICIA SESSÃO DO SPARK---------------------------------------------------------
try:
    spark = SparkSession.builder.appName("projeto_final").config("spark.sql.caseSensitive", "True").config("spark.sql.debug.maxToStringFields",100).getOrCreate()
except Exception as e:
    print('Erro ao iniciar sessão do Spark: ', e)
# ----------------------------------------------CRIAÇÃO DOS DATAFRAMES SPARK---------------------------------------------------------
try:
    print('Inicia criação dos dataframes Spark...') 
    spk_geracao_distribuida = spark.createDataFrame(df_geracao_distribuida)
    spk_tarifa_residencial = spark.createDataFrame(df_tarifa_residencial)
    spk_tarifa_media = spark.createDataFrame(df_tarifa_media)
    spk_empreendimentos_gd = spark.createDataFrame(df_empreendimentos_gd)
except Exception as e:
    print('Erro na criação dos dataframes Spark: ', e)
    
# ----------------------------------------------CRIAÇÃO DOS ARQUIVOS PARQUET---------------------------------------------------------
try:
    print('Criando arquivos parquet') 
    spk_empreendimentos_gd.write.parquet(caminho + "empreendimentos_gd")
    spk_geracao_distribuida.write.parquet(caminho + "geracaodistribuida")
    spk_tarifa_residencial.write.parquet(caminho + "tarifamediaresidencial")
    spk_tarifa_media.write.parquet(caminho + "tarifamediafornecimento")
except Exception as e:
    print('Erro ao criar arquivos Parquet: ', e)
print('Fim da execução')