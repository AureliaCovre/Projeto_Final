from time import time
from modules.conector_mysql import Interface_db_mysql
from modules.conector_cassandra import Interface_db_cassandra
from modules.conector_mongo import Interface_db_mongo
from modules.conector_postgree import Interface_db_postgree
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession

if __name__ == "__main__":
    try:
        
        # Cria uma interface do usuário com o banco MySQL
        interface_mysql = Interface_db_mysql("joao","M1necraft","127.0.0.1","projeto_final")
        
        # Fazendo a leitura pela biblioteca Pandas
        # dados_empreendimento = pd.read_csv('empreendimento-geracao-distribuida.csv', sep = "\t", encoding = 'utf-16')
        
        # Inserindo no banco MySQL      
        # for linha in dados_empreendimento:
        #     #insere os dados limpos na tabela empreendimentosGD
        #     valores = f'("{linha[0]}", {linha[1]}, "{linha[2]}","{linha[3]}","{linha[4]}","{linha[5]}",{linha[6]},"{linha[7]}",{linha[8]},"{linha[9]}", "{linha[10]}", "{linha[11]}","{linha[12]}","{linha[13]}",{linha[14]},"{linha[15]}","{linha[16]}","{linha[17]}","{linha[18]}","{linha[19]}","{linha[20]}",{linha[21]},"{linha[22]}","{linha[23]}",{linha[24]},"{linha[25]}","{linha[26]}","{linha[27]}",{linha[28]},{linha[29]},{linha[30]})'
        #     interface_mysql.insert("empreendimentosGD","(NomeConjunto ,DataGeracaoConjunto ,PeriodoReferencia ,CNPJ_Distribuidora ,SigAgente ,NomAgente ,CodClasseConsumo,ClasseClasseConsumo ,CodigoSubgrupoTarifario ,GrupoSubgrupoTarifario ,codUFibge ,SigUF ,codRegiao ,NomRegiao ,CodMunicipioIbge ,NomMunicipio  ,CodCEP ,TipoConsumidor ,NumCPFCNPJ ,NomTitularUC ,CodGD ,DthConexao ,CodModalidade ,DscModalidade ,QtdUCRecebeCredito ,TipoGeracao ,FonteGeracao ,Porte ,PotenciaInstaladaKW ,MdaLatitude ,MdaLongitude)",valores)
        
        # Fazendo a leitura pela biblioteca Pandas
        # dados_geracao = pd.read_csv('GeracaoDistribuida.csv', sep = ";", encoding = 'latin-1')
        
        # Inserindo no banco MySQL      
        # for linha in dados_geracao:
        #     #insere os dados limpos na tabela geracaoDistribuida
        #     valores = f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},{linha[6]},{linha[7]})"
        #     interface_mysql.insert("geracaoDistribuida","(ideGeracaoDistribuida,nomGeracaoDistribuida ,sigGeracaoDistribuida ,qtdUsina ,mdaPotenciaInstaladakW ,mesReferencia ,anoReferencia ,dthProcessamento )",valores) 
            
        # Fazendo a leitura pela biblioteca Pandas
        # dados_tarifa_residencial = pd.read_csv('TarifaFornecimentoResidencial.csv', sep = ";", encoding = 'latin-1')

        # Inserindo no banco MySQL      
        # for linha in dados_tarifa_residencial:
        #     #insere os dados limpos na tabela tarifaResidencial
        #     valores = f"({linha[0]},'{linha[1]}','{linha[2]}','{linha[3]}',{linha[4]},{linha[5]},{linha[6]},{linha[7]},{linha[8]},{linha[9]},'{linha[10]}',{linha[11]},{linha[12]})"
        #     interface_mysql.insert("tarifaResidencial","(ideTarifaFornecimento ,nomConcessao ,SigDistribuidora ,SigRegiao ,VlrTUSDConvencional ,VlrTEConvencional ,VlrTotaTRFConvencional ,VlrTRFBrancaPonta,VlrTRFBrancaIntermediaria ,VlrTRFBrancaForaPonta,NumResolucao,DthInicioVigencia ,DthProcessamento)",valores)
         
        # Fazendo a leitura pela biblioteca Pandas
        dados_tarifa_media = pd.read_csv('TarifaMediaFornecimento.csv', sep = ";", encoding = 'latin-1')
        
        dados_tarifa_media.dropna(axis=0, subset=["ideTarifaMediaFornecimento", "nomClasseConsumo", "nomRegiao", "vlrConsumoMWh", "mesReferencia", "anoReferencia", "dthProcessamento"], inplace=True)
        valores = ""
        contador = 0
        
        # Inserindo no banco MySQL       
        for coluna, linha in dados_tarifa_media.iterrows():
            if(linha[2] == " Serviços e Outras" or linha[2] == " esgoto e saneamento)"):
                linha[1] = linha[1] + linha[2]
                linha[2] = linha[3]
                linha[3] = linha[4]
                linha[4] = linha[5]
                linha[5] = linha[6]
                linha[6] = linha[7]
                linha[7] = np.NaN
            linha[6] = datetime.combine(datetime.strptime(linha[6], '%d/%m/%Y %H:%M').date(), datetime.strptime(linha[6], '%d/%m/%Y %H:%M').time()) #altera o formato da data                    
            contador = contador + 1
                    
            if (contador == len(dados_tarifa_media) ): 
                valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},'{linha[6]}')"
            else:
                valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},'{linha[6]}')" + ","
        #interface_mysql.insert("tarifaMediaFornecimento","(ideTarifaMediaFornecimento, nomClasseConsumo, nomRegiao, vlrConsumoMWh, mesReferencia, anoReferencia, dthProcessamento)",valores)
        
        print("Fim da execução!")
    except Exception as e:
        print(str(e))    
        
        
        
        
 