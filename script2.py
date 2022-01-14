import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from conector_mysql import Interface_db_mysql


# INTERFACE COM O MYSQL WORKBENCH:

# Cria uma interface do usuÃ¡rio com o banco MySQL
caminho = "gs://arquivos_parquet_bucket/arquivos_parquet/"
try:
    interface_mysql = Interface_db_mysql("root","projeto-final","35.198.38.124","projeto_final")
except Exception as e:
    print('Erro na conexao ao MySQL: ', e)

print('Leitura geracao distribuida...')    
dados_geracao_distribuida = interface_mysql.select("*","geracaoDistribuida","")
df_geracao_distribuida = pd.DataFrame(dados_geracao_distribuida)

print('Leitura tarifamediafornecimento...')    
dados_tarifa_media = interface_mysql.select("*","tarifaMediaFornecimento","")
df_tarifa_media = pd.DataFrame(dados_tarifa_media)

print('Leitura tarifaresidencial...')    
dados_tarifa_residencial = interface_mysql.select("*","tarifaResidencial","")
df_tarifa_residencial = pd.DataFrame(dados_tarifa_residencial)

print('Leitura empreendimentos...') 
dados_empreendimentos = interface_mysql.select("*","empreendimentosGD","")
df_empreendimentos_gd = pd.DataFrame(dados_empreendimentos)

spark = SparkSession.builder.appName("projeto_final").config("spark.sql.caseSensitive", "True").config("spark.sql.debug.maxToStringFields",100).getOrCreate()

print('Inicia criação df spark...') 
spk_geracao_distribuida = spark.createDataFrame(df_geracao_distribuida)
spk_tarifa_residencial = spark.createDataFrame(df_tarifa_residencial)
spk_tarifa_media = spark.createDataFrame(df_tarifa_media)
spk_empreendimentos_gd = spark.createDataFrame(df_empreendimentos_gd)

print('Criando parquet...') 
spk_empreendimentos_gd.write.parquet(caminho + "empreendimentos_gd")
spk_geracao_distribuida.write.parquet(caminho + "geracaodistribuida")
spk_tarifa_residencial.write.parquet(caminho + "tarifamediaresidencial")
spk_tarifa_media.write.parquet(caminho + "tarifamediafornecimento")

print('Fim da execução')