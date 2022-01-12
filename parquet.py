

from modules.conector_mysql import Interface_db_mysql
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession

import mysql.connector




# INTERFACE COM O MYSQL WORKBENCH:

# Cria uma interface do usuário com o banco MySQL

print(datetime.now())

# try:
#     interface_mysql = Interface_db_mysql("robson","R0350njose*123","127.0.0.1","projeto_final")
# except Exception as e:
#     print('Erro na conexao ao MySQL: ', e)

# print('Leitura geracao distribuida...')    
# dados_geracao_distribuida = interface_mysql.select("*", "geracaodistribuida", "")

# df_geracao_distribuida = pd.DataFrame(dados_geracao_distribuida)

# print(df_geracao_distribuida)

# print('Leitura tarifamediafornecimento...')    
# dados_tarifa_media = interface_mysql.select("*", "tarifamediafornecimento", "")

# df_tarifa_media = pd.DataFrame(dados_tarifa_media)

# print(df_tarifa_media)

# print('Leitura tarifaresidencial...')    
# dados_tarifa_residencial = interface_mysql.select("*", "tarifaresidencial", "")

# df_tarifa_residencial = pd.DataFrame(dados_tarifa_residencial)

# print(df_tarifa_residencial)

con = mysql.connector.connect(user='robson', password='R0350njose*123', host='localhost', database='projeto_final')
cursor = con.cursor()

query = "SELECT * FROM empreendimentosgd;"

print('Leitura empreendimentosgd...')
print(datetime.now())

dados_empreendimentos_gd = cursor.execute(query)
dados_empreendimentos_gd = cursor.fetchall()
df_empreendimentos_gd = pd.DataFrame(dados_empreendimentos_gd)

print(datetime.now())

cursor.close()
con.commit()
con.close()

# print(df_empreendimentos_gd)



spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()

print('Inicia criação df sparks...')    
# spk_geracao_distribuida = spark.createDataFrame(df_geracao_distribuida)
# spk_tarifa_residencial = spark.createDataFrame(df_tarifa_residencial)
# spk_tarifa_media = spark.createDataFrame(df_tarifa_media)
spk_empreendimentos_gd = spark.createDataFrame(df_empreendimentos_gd)

print('Criando parquet...')
spk_empreendimentos_gd.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\empreendimentos_gd")
# spk_geracao_distribuida.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\geracaodistribuida")
# spk_tarifa_residencial.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\tarifamediaresidencial")
# spk_tarifa_media.write.parquet(r"D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\tarifamediafornecimento")


print('Fim da execução')
print(datetime.now())