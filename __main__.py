import csv

from numpy import NaN
from modules.conector_mysql import Interface_db_mysql
from modules.conector_cassandra import Interface_db_cassandra
from modules.conector_mongo import Interface_db_mongo
from modules.conector_postgree import Interface_db_postgree
import pandas as pd
from pyspark.sql import SparkSession


if __name__ == "__main__":
    try:
            
        # Fazendo a leitura pela biblioteca Pandas
        # dados_empreendimento = pd.read_csv('empreendimento-geracao-distribuida.csv', sep = "\t", encoding = 'utf-16')

        # for column, row in dados_empreendimento.iterrows():
        #     print(row)
            
        # Fazendo a leitura pela biblioteca Pandas
        # dados_geracao = pd.read_csv('GeracaoDistribuida.csv', sep = ";", encoding = 'latin-1')

        # for column, row in dados_geracao.iterrows():
        #     print(row)  
            
        # Fazendo a leitura pela biblioteca Pandas
        # dados_tarifa_residencial = pd.read_csv('TarifaFornecimentoResidencial.csv', sep = ";", encoding = 'latin-1')

        # for column, row in dados_tarifa_residencial.iterrows():
        #     print(row) 
            
        # # Fazendo a leitura pela biblioteca Pandas
        dados_tarifa_media = pd.read_csv('TarifaMediaFornecimento.csv', sep = ";", encoding = 'latin-1')

        #17;Comercial Serviços e Outras;Brasil;448.07;3;2003;13/09/2017 00:00
        #1639;Serviço Público (água esgoto e saneamento);Brasil;336.07;6;2017;10/10/2017 00:00        
        for column, row in dados_tarifa_media.iterrows():
            if(row[2] == " Serviços e Outras" or row[2] == " esgoto e saneamento)"):
                row[1] = row[1] + row[2]
                row[2] = row[3]
                row[3] = row[4]
                row[4] = row[5]
                row[5] = row[6]
                row[6] = row[7]
                row[7] = NaN

        #conexao com o banco 
        #insere os dados 
            
        print("Fim da execução!")
    except Exception as e:
        print(str(e))