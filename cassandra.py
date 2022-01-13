# from modules.conector_cassandra import Interface_db_cassandra
from pyspark.sql import SparkSession
import pandas as pd
from cassandra.cluster import Cluster

spark = SparkSession.builder.appName("projeto_final").config("spark.sql.caseSensitive", "True")\
                                                     .config("spark.sql.debug.maxToStringFields",100).getOrCreate()


try:           
    geracao_distribuida = spark.read.parquet(r'D:\Dropbox\cursos\soul-code\3-tarefas\atividades\Projeto_Final-main\parquet\geracaodistribuida', engine='fastparquet')
    geracao_distribuida.show()
    
    cluster = Cluster()
    session = cluster.connect('projeto_final')
    
    print("abc")
    
except Exception as e:
    print(e)
    
    
    
    