from time import time

from dns.exception import ExceptionWrapper
from modules.conector_mysql import Interface_db_mysql
from modules.conector_cassandra import Interface_db_cassandra
from modules.conector_mongo import Interface_db_mongo
from modules.conector_postgree import Interface_db_postgree
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession

"""
    descrição:
        Faz a leitura dos CSVs
        Faz o tratamento dos dados
        Faz a inserção no banco de dados MySQL (projeto_final)
    @author:
        Aurelia Covre
        João Victor Guimarães
        Kely Fernandes Rodrigues
        Ricardo Rowedder
        Robson Motta
"""

if __name__ == "__main__":
    try:

# INTERFACE COM O MYSQL WORKBENCH:

        # Cria uma interface do usuário com o banco MySQL
        try:
            interface_mysql = Interface_db_mysql("robson","R0350njose*123","127.0.0.1","projeto_final")
        except Exception as e:
            print('Erro na conexao ao MySQL: ', e)

# ----------------------------------------------LEITURA DE CSV, TRATAMENTO E INSERÇÃO DE DADOS (empreendimento-geracao-distribuida) ---------------------------------------------------------
        
        # Fazendo a leitura pela biblioteca Pandas
        try:
            dados_empreendimento = pd.read_csv('empreendimento-geracao-distribuida.csv', sep = "\t", encoding = 'utf-16')
        except Exception as e:
            print('Erro ao ler empreendimento-geracao-distribuida: ', e)
            
        # Preenchendo colunas vazias:
        try:
            values = {"CNPJ_Distribuidora": 'ND', "SigAgente": 'ND', "NomAgente": 'ND', "MdaLatitude": 0, "MdaLongitude": 0}
            dados_empreendimento.fillna(value=values, inplace= True)
        except Exception as e:
            print('Erro ao preencher colunas vazias - empreendimento-geracao-distribuida: ', e)
            
        # Dropando linhas em que as colunas não possuem dados
        try:
            dados_empreendimento.dropna(axis=0, subset=["NomeConjunto", "DataGeracaoConjunto", "PeriodoReferencia", "CNPJ_Distribuidora", "SigAgente", "NomAgente", "CodClasseConsumo", "ClasseClasseConsumo", "CodigoSubgrupoTarifario", "GrupoSubgrupoTarifario", "codUFibge", "SigUF", "codRegiao", "NomRegiao", "CodMunicipioIbge", "NomMunicipio", "CodCEP", "TipoConsumidor", "NumCPFCNPJ", "NomTitularUC", "CodGD", "DthConexao", "CodModalidade", "DscModalidade", "QtdUCRecebeCredito", "TipoGeracao", "FonteGeracao", "Porte", "PotenciaInstaladaKW", "MdaLatitude","MdaLongitude"], inplace=True)
        except Exception as e:
            print('Erro ao dropar linhas com dados em branco - empreendimento-geracao-distribuida:', e)
            
        # Substituindo ',' por '.' na coluna de Potencia instalada
        try:
            dados_empreendimento['PotenciaInstaladaKW'] = dados_empreendimento['PotenciaInstaladaKW'].str.replace(",", ".")
        except Exception as e:
            print('Erro ao substituir virgula por ponto - empreendimento-geracao-distribuida:', e)
        
        # Con, cursor (exclusiva para este csv)
        try:
            con, cursor = interface_mysql.conectar()
        except Exception as e:
            print('Erro em con, cursor - empreendimento-geracao-distribuida:', e)
        
        contador = 0 #contador para acompanhamento
        
        # Inserindo no banco MySQL
        for coluna, linha in dados_empreendimento.iterrows():        
            contador = contador + 1   
            
            # Transforma data da coluna 1 e 21 em DateTime
            try:                     
                linha[1] = datetime.combine(datetime.strptime(linha[1], '%d/%m/%Y %H:%M').date(), datetime.strptime(linha[1], '%d/%m/%Y %H:%M').time()) #altera o formato da data (coluna 1)
                linha[21] = datetime.strptime(linha[21], '%d/%m/%Y').date()  #altera o formato da data (coluna 21)
            except Exception as e:
                print('Erro ao transformar em datetime - empreendimento-geracao-distribuida:', e)
                         
        # Insere os dados tratados na tabela empreendimentosGD
            try:
                valores = f'("{linha[0]}", "{linha[1]}", "{linha[2]}", "{linha[3]}", "{linha[4]}", "{linha[5]}", {linha[6]}, "{linha[7]}", {linha[8]}, "{linha[9]}", "{linha[10]}", "{linha[11]}", "{linha[12]}", "{linha[13]}", {linha[14]}, "{linha[15]}", "{linha[16]}", "{linha[17]}", "{linha[18]}", "{linha[19]}", "{linha[20]}", "{linha[21]}", "{linha[22]}", "{linha[23]}", {linha[24]}, "{linha[25]}", "{linha[26]}", "{linha[27]}", {linha[28]}, {linha[29]}, {linha[30]})'
            except Exception as e:
                print('Erro ao inserir dados tratados no banco - empreendimento-geracao-distribuida:', e)
                
            # Querry de inserção dos dados
            try:
                interface_mysql.insert2("empreendimentosgd","(NomeConjunto, DataGeracaoConjunto, PeriodoReferencia, CNPJ_Distribuidora, SigAgente, NomAgente, CodClasseConsumo, ClasseClasseConsumo, CodigoSubgrupoTarifario, GrupoSubgrupoTarifario, codUFibge, SigUF, codRegiao, NomRegiao, CodMunicipioIbge, NomMunicipio, CodCEP, TipoConsumidor, NumCPFCNPJ, NomTitularUC, CodGD, DthConexao, CodModalidade, DscModalidade, QtdUCRecebeCredito, TipoGeracao, FonteGeracao, Porte, PotenciaInstaladaKW, MdaLatitude, MdaLongitude)",valores, cursor, con)
            except Exception as e:
                print('Erro na querry - empreendimento-geracao-distribuida:', e)
            
            print(contador) #contador para acompanhamento
            
        interface_mysql.desconectar(con, cursor)
        

# -----------------------------------------------LEITURA DE CSV, TRATAMENTO E INSERÇÃO DE DADOS (GeracaoDistribuida) ---------------------------------------------------------

        # Fazendo a leitura pela biblioteca Pandas
        try:
            dados_geracao = pd.read_csv('GeracaoDistribuida.csv', sep = ";", encoding = 'latin-1')
        except Exception as e:
            print('Erro ao ler GeracaoDistribuida:', e)
            
        # Dropando linhas em que as colunas não possuem dados
        try:
            dados_geracao.dropna(axis=0, subset=["ideGeracaoDistribuida", "nomGeracaoDistribuida" ,"sigGeracaoDistribuida" ,"qtdUsina", "mdaPotenciaInstaladakW", "mesReferencia", "anoReferencia", "dthProcessamento"], inplace=True)
        except Exception as e:
            print('Erro ao dropar linhas com dados em branco - GeracaoDistribuida')
            
        # Cria variável que receberá valores a serem inseridos
        valores = ""
        
        contador = 0 #contador para acompanhamento
        
        # Inserindo no banco MySQL      
        for coluna, linha in dados_geracao.iterrows():        

            # Transforma data da coluna 7 em DateTime
            try:
                linha[7] = datetime.combine(datetime.strptime(linha[7], '%d/%m/%Y %H:%M').date(), datetime.strptime(linha[7], '%d/%m/%Y %H:%M').time()) #altera o formato da data
            except Exception as e:
                print('Erro ao transformar data em datetime - GeracaoDistribuida', e)
                
            contador = contador + 1 #contador para acompanhamento

        # Insere os dados tratados na tabela geracaoDistribuida
            try:
                if (contador == len(dados_geracao) ): 
                    valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},{linha[6]},'{linha[7]}')"
                else:
                    valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},{linha[6]},'{linha[7]}')" + ","
            except Exception as e:
                print('Erro ao inserir dados tratados - GeracaoDistribuida', e)
                
        # Querry de inserção dos dados  
        try:      
            interface_mysql.insert("geracaoDistribuida","(ideGeracaoDistribuida, nomGeracaoDistribuida, sigGeracaoDistribuida, qtdUsina, mdaPotenciaInstaladakW, mesReferencia, anoReferencia, dthProcessamento)",valores) 
        except Exception as e:
            print('Erro na querry de insercao:', e)

# -------------------------------------------------LEITURA DE CSV, TRATAMENTO E INSERÇÃO DE DADOS (TarifaFornecimentoResidencial) ---------------------------------------------------------
            
        # Fazendo a leitura pela biblioteca Pandas
        try:
            dados_tarifa_residencial = pd.read_csv('TarifaFornecimentoResidencial.csv', sep = ";", encoding = 'latin-1')
        except Exception as e:
            print('Erro ao ler TarifaFornecimentoResidencial', e)
        
        # Dropando linhas em que as colunas não possuem dados
        try:
            dados_tarifa_residencial.dropna(axis=0, subset=["ideTarifaFornecimento", "nomConcessao", "SigDistribuidora", "SigRegiao", "VlrTUSDConvencional", "VlrTEConvencional", "VlrTotaTRFConvencional", "VlrTRFBrancaPonta", "VlrTRFBrancaIntermediaria", "VlrTRFBrancaForaPonta", "NumResolucao", "DthInicioVigencia",	"DthProcessamento"], inplace=True)
        except Exception as e:
            print('Erro ao dropar linhas com dados em branco', e)       
       
        # Cria variável que receberá valores a serem inseridos 
        valores = ""
        
        contador = 0 #contador para acompanhamento

        # Inserindo no banco MySQL      
        for coluna, linha in dados_tarifa_residencial.iterrows():
            
            # Transforma data da coluna 11 e 12 em DateTime
            try:        
                linha[11] = datetime.combine(datetime.strptime(linha[11], '%d/%m/%Y %H:%M').date(), datetime.strptime(linha[11], '%d/%m/%Y %H:%M').time()) #altera o formato da data
                linha[12] = datetime.combine(datetime.strptime(linha[12], '%d/%m/%Y %H:%M').date(), datetime.strptime(linha[12], '%d/%m/%Y %H:%M').time()) #altera o formato da data                                                                
            except Exception as e:
                print('Erro ao transformar data em datetime - TarifaFornecimentoResidencial', e)
             
            contador = contador + 1 #contador para acompanhamento
            
            # Insere os dados tratados na tabela tarifaResidencial   
            try:          
                if (contador == len(dados_tarifa_residencial) ): 
                    valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}','{linha[3]}',{linha[4]},{linha[5]},{linha[6]},{linha[7]},{linha[8]},{linha[9]},'{linha[10]}','{linha[11]}','{linha[12]}')"
                else:
                    valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}','{linha[3]}',{linha[4]},{linha[5]},{linha[6]},{linha[7]},{linha[8]},{linha[9]},'{linha[10]}','{linha[11]}','{linha[12]}')" + ","
            except Exception as e:
                print('Erro ao inserir dados tratados - TarifaFornecimentoResidencial', e)
                
        # Querry de inserção dos dados  
        try:      
            interface_mysql.insert("tarifaResidencial","(ideTarifaFornecimento, nomConcessao, SigDistribuidora, SigRegiao, VlrTUSDConvencional, VlrTEConvencional, VlrTotaTRFConvencional, VlrTRFBrancaPonta, VlrTRFBrancaIntermediaria, VlrTRFBrancaForaPonta, NumResolucao, DthInicioVigencia, DthProcessamento)",valores)  
        except Exception as e:
            print('Erro na querry de insercao - TarifaFornecimentoResidencial', e)

# ------------------------------------------LEITURA DE CSV, TRATAMENTO E INSERÇÃO DE DADOS (TarifaMediaFornecimento) ---------------------------------------------------------

        # Fazendo a leitura pela biblioteca Pandas
        try:
            dados_tarifa_media = pd.read_csv('TarifaMediaFornecimento.csv', sep = ";", encoding = 'latin-1')
        except Exception as e:
            print('Erro ao ler TarifaMediaFornecimento', e)
        
        # Dropando linhas em que as colunas não possuem dados  
        try:      
            dados_tarifa_media.dropna(axis=0, subset=["ideTarifaMediaFornecimento", "nomClasseConsumo", "nomRegiao", "vlrConsumoMWh", "mesReferencia", "anoReferencia", "dthProcessamento"], inplace=True)
        except Exception as e:
            print('Erro ao dropar linhas com dados em branco - TarifaMediaFornecimento', e)
            
        # Cria variável que receberá valores a serem inseridos        
        valores = ""
        
        contador = 0 #contador para acompanhamento
        
        # Inserindo no banco MySQL 
        # Laço for para correção de anomalia encontrada na linha 14 do CSV 
        try:     
            for coluna, linha in dados_tarifa_media.iterrows():
                if(linha[2] == " Serviços e Outras" or linha[2] == " esgoto e saneamento)"):
                    linha[1] = linha[1] + linha[2]
                    linha[2] = linha[3]
                    linha[3] = linha[4]
                    linha[4] = linha[5]
                    linha[5] = linha[6]
                    linha[6] = linha[7]
                    linha[7] = np.NaN
        except Exception as e:
            print('Erro no laco de correcao de colunas - TarifaMediaFornecimento', e)     
               
            # Transforma data da coluna 6 em DateTime
            try: 
                linha[6] = datetime.combine(datetime.strptime(linha[6], '%d/%m/%Y %H:%M').date(), datetime.strptime(linha[6], '%d/%m/%Y %H:%M').time()) #altera o formato da data                    
            except Exception as e:
                print('Erro ao transformar data em datetime - TarifaMediaFornecimento', e)
            
            contador = contador + 1 #contador para acompanhamento
            
            # Insere os dados tratados na tabela tarifaResidencial    
            try:                
                if (contador == len(dados_tarifa_media) ): 
                    valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},'{linha[6]}')"
                else:
                    valores = valores + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},'{linha[6]}')" + ","
            except Exception as e:
                print('Erro ao inserir dados tratados - TarifaMediaFornecimento', e)
                
        # Querry de inserção dos dados    
        try:         
            interface_mysql.insert("tarifaMediaFornecimento","(ideTarifaMediaFornecimento, nomClasseConsumo, nomRegiao, vlrConsumoMWh, mesReferencia, anoReferencia, dthProcessamento)",valores)
        except Exception as e:
            print('Erro na querry de insercao - TarifaMediaFornecimento', e)
            
        print("Fim da execução!")
    except Exception as e:
        print(str(e))    
        