from modules.conector_mysql import Interface_db_mysql
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# INTERFACE COM O MYSQL WORKBENCH:

# Cria uma interface do usuário com o banco MySQL

try:
    interface_mysql = Interface_db_mysql("joao","M1necraft","127.0.0.1","projeto_final")
except Exception as e:
    print('Erro na conexao ao MySQL: ', e)

# print('Leitura geracao distribuida...')    
# dados_geracao_distribuida = interface_mysql.select("*", "geracaoDistribuida", "")
# df_geracao_distribuida = pd.DataFrame(dados_geracao_distribuida)

# print('Leitura tarifamediafornecimento...')    
# dados_tarifa_media = interface_mysql.select("*", "tarifaMediaFornecimento", "")
# df_tarifa_media = pd.DataFrame(dados_tarifa_media)

# print('Leitura tarifaresidencial...')    
# dados_tarifa_residencial = interface_mysql.select("*", "tarifaResidencial", "")
# df_tarifa_residencial = pd.DataFrame(dados_tarifa_residencial)

print('Leitura empreendimentos...') 
dados_empreendimentos = interface_mysql.select("*", "empreendimentosGD", "")
df_empreendimentos_gd = pd.DataFrame(dados_empreendimentos)

spark = SparkSession.builder.appName("projeto_final").config("spark.sql.caseSensitive", "True")\
                                                     .config("spark.sql.debug.maxToStringFields", 100)\
                                                     .config("spark.driver.memory", "5g").getOrCreate()

print('Inicia criação df sparks...') 
# spk_geracao_distribuida = spark.createDataFrame(df_geracao_distribuida)
# spk_tarifa_residencial = spark.createDataFrame(df_tarifa_residencial)
# spk_tarifa_media = spark.createDataFrame(df_tarifa_media)
tiposColunas = StructType([StructField("id", IntegerType())\
                    ,StructField("NomeConjunto", StringType())\
                    ,StructField("DataGeracaoConjunto", StringType())\
                    ,StructField("PeriodoReferencia", StringType())\
                    ,StructField("CNPJ_Distribuidora", StringType())\
                    ,StructField("SigAgente", StringType())\
                    ,StructField("NomAgente", StringType())\
                    ,StructField("CodClasseConsumo", IntegerType())\
                    ,StructField("ClasseClasseConsumo", StringType())\
                    ,StructField("CodigoSubgrupoTarifario", IntegerType())\
                    ,StructField("GrupoSubgrupoTarifario", StringType())\
                    ,StructField("codUFibge", StringType())\
                    ,StructField("SigUF", StringType())\
                    ,StructField("codRegiao", StringType())\
                    ,StructField("NomRegiao", StringType())\
                    ,StructField("CodMunicipioIbge", IntegerType())\
                    ,StructField("NomMunicipio", StringType())\
                    ,StructField("CodCEP", StringType())\
                    ,StructField("TipoConsumidor", StringType())\
                    ,StructField("NumCPFCNPJ", StringType())\
                    ,StructField("NomTitularUC", StringType())\
                    ,StructField("CodGD", StringType())\
                    ,StructField("DthConexao", StringType())\
                    ,StructField("CodModalidade", StringType())\
                    ,StructField("DscModalidade", StringType())\
                    ,StructField("QtdUCRecebeCredito", IntegerType())\
                    ,StructField("TipoGeracao", StringType())\
                    ,StructField("FonteGeracao", StringType())\
                    ,StructField("Porte", StringType())\
                    ,StructField("PotenciaInstaladaKW", FloatType())\
                    ,StructField("MdaLatitude", FloatType())\
                    ,StructField("MdaLongitude", FloatType())] )

spk_empreendimentos_gd = spark.createDataFrame(df_empreendimentos_gd, schema=tiposColunas)

spk_empreendimentos_gd = spk_empreendimentos_gd.select(col("SigAgente"),col("NomAgente"),col("CodClasseConsumo"),\
                                                       col("ClasseClasseConsumo"),col("CodigoSubgrupoTarifario"),col("GrupoSubgrupoTarifario"),col("codUFibge"),\
                                                       col("SigUF"),col("codRegiao"),col("NomRegiao"),col("CodMunicipioIbge"),\
                                                       col("NomMunicipio"),col("CodCEP"),col("TipoConsumidor"),col("NomTitularUC"),\
                                                       col("CodGD"),col("DthConexao"),col("CodModalidade"),col("DscModalidade"),\
                                                       col("QtdUCRecebeCredito"),col("TipoGeracao"),col("FonteGeracao"),col("Porte"), col("PotenciaInstaladaKW") )

print('Criando parquet...')
spk_empreendimentos_gd.write.parquet(r"C:\Users\joaov\Desktop\Soulcode\Python\ProjetoFinal\parquet\empreendimentos_gd")
# spk_geracao_distribuida.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\geracaodistribuida")
# spk_tarifa_residencial.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\tarifamediaresidencial")
# spk_tarifa_media.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\tarifamediafornecimento")

print('Fim da execução')
