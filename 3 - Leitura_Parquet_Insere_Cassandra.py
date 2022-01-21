from pyspark.sql import SparkSession
from conector_cassandra import Interface_db_cassandra

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
caminho = "gs://arquivos_parquet_bucket/arquivos_parquet/"

# ----------------------------------------------INTERFACE COM O CASSANDRA ---------------------------------------------------------
try: 
    interface_cassandra = Interface_db_cassandra(host='34.95.216.194', porta=9042, database='projeto_final',)
except Exception as e:
    print('Erro ao criar interface com o Cassandra: ', e)

# ----------------------------------------------INICIA SESSÃO DO SPARK---------------------------------------------------------    
try: 
    spark = SparkSession.builder.appName("projeto_final").config("spark.sql.caseSensitive", "True").config("spark.sql.debug.maxToStringFields",100).getOrCreate()
except Exception as e:
    print('Erro ao iniciar sessão do Spark: ', e)

# ----------------------------------------------LEITURA DOS ARQUIVOS PARQUET---------------------------------------------------------
try:                                               
    geracao_distribuida = spark.read.parquet(caminho + "geracaodistribuida")
except Exception as e:
    print('Erro ao ler o arquivo parquet geracaodistribuida: ', e)

try:                                               
    empreendimentosGD = spark.read.parquet(caminho + "empreendimentos_gd")
except Exception as e:
    print('Erro ao ler o arquivo parquet empreendimentos_gd: ', e)
    
try:                                               
    tarefa_media_fornecimento = spark.read.parquet(caminho + "tarifamediafornecimento")
except Exception as e:
    print('Erro ao ler o arquivo parquet tarifamediafornecimento: ', e)
    
try:                                               
    tarifa_media_residencial = spark.read.parquet(caminho + "tarifamediaresidencial") 
except Exception as e:
    print('Erro ao ler o arquivo parquet tarifamediaresidencial: ', e)
    
# ----------------------------------------------INSERE NO BANCO DE DADOS NOSQL---------------------------------------------------------
   
try:
    dados_geracao = geracao_distribuida.collect() #Transforma todos os registros em uma lista de linhas
    print("Inserindo dados na tabela geracaoDistribuida..")
    for linha in dados_geracao:
        # Insere os dados tratados na tabela geracaoDistribuida
        try:
            query = "insert into geracaoDistribuida (ideGeracaoDistribuida, nomGeracaoDistribuida, sigGeracaoDistribuida, qtdUsina, mdaPotenciaInstaladakW, mesReferencia, anoReferencia, dthProcessamento) values " + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},{linha[6]},'{linha[7]}')" + ";"
            interface_cassandra.inserir(query)
        except Exception as e:
            print('Erro ao inserir dados tratados - GeracaoDistribuida', e)
except Exception as e:
    print('Erro ao inserir dados no banco NoSQL - geracaoDistribuida: ', e)

try:
    dados_empreendimentosGD = empreendimentosGD.collect() #Transforma todos os registros em uma lista de linhas
    contador = 0
    print("Inserindo dados na tabela empreendimentosGD..")
    for linha in dados_empreendimentosGD:
        # Insere os dados tratados na tabela empreendimentosGD
        try:
            query = "insert into empreendimentosGD (id, NomeConjunto, DataGeracaoConjunto, PeriodoReferencia, CNPJ_Distribuidora, SigAgente, NomAgente, CodClasseConsumo, ClasseClasseConsumo, CodigoSubgrupoTarifario, GrupoSubgrupoTarifario, codUFibge, SigUF, codRegiao, NomRegiao, CodMunicipioIbge, NomMunicipio, CodCEP, TipoConsumidor, NumCPFCNPJ, NomTitularUC, CodGD, DthConexao, CodModalidade, DscModalidade, QtdUCRecebeCredito, TipoGeracao, FonteGeracao, Porte, PotenciaInstaladaKW, MdaLatitude, MdaLongitude) values " + f"({linha[0]},'{linha[1]}', '{linha[2]}', '{linha[3]}', '{linha[4]}', '{linha[5]}', '{linha[6]}', {linha[7]}, '{linha[8]}', {linha[9]}, '{linha[10]}', '{linha[11]}', '{linha[12]}', '{linha[13]}', '{linha[14]}', {linha[15]}, '{linha[16]}', '{linha[17]}', '{linha[18]}', '{linha[19]}', '{linha[20]}', '{linha[21]}', '{linha[22]}', '{linha[23]}', '{linha[24]}', {linha[25]}, '{linha[26]}', '{linha[27]}', '{linha[28]}', {linha[29]}, {linha[30]}, {linha[31]})" + ";"
            interface_cassandra.inserir(query)
        except Exception as e:
            print('Erro ao inserir dados tratados - empreendimentosGD', e)  
except Exception as e:
    print('Erro ao inserir dados no banco NoSQL - empreendimentosGD: ', e)

try:
    dados_tarefa_media_fornecimento = tarefa_media_fornecimento.collect() #Transforma todos os registros em uma lista de linhas
    print("Inserindo dados na tabela tarifaMediaFornecimento..") 
    for linha in dados_tarefa_media_fornecimento:
        # Insere os dados tratados na tabela tarifaMediaFornecimento
        try:
            query = "insert into tarifaMediaFornecimento (ideTarifaMediaFornecimento, nomClasseConsumo, nomRegiao, vlrConsumoMWh, mesReferencia, anoReferencia, dthProcessamento) values " + f"({linha[0]},'{linha[1]}','{linha[2]}',{linha[3]},{linha[4]},{linha[5]},'{linha[6]}')" + ";"
            interface_cassandra.inserir(query)        
        except Exception as e:
            print('Erro ao inserir dados tratados - tarifaMediaFornecimento', e)      
except Exception as e:
    print('Erro ao inserir dados no banco NoSQL - tarifaMediaFornecimento: ', e)    
   
try:
    dados_tarifa_media_residencial = tarifa_media_residencial.collect() #Transforma todos os registros em uma lista de linhas
    print("Inserindo dados na tabela tarifaResidencial..")
    for linha in dados_tarifa_media_residencial:
        # Insere os dados tratados na tabela tarifaResidencial
        try:
            query = "insert into tarifaResidencial (ideTarifaFornecimento, nomConcessao, SigDistribuidora, SigRegiao, VlrTUSDConvencional, VlrTEConvencional, VlrTotaTRFConvencional, VlrTRFBrancaPonta, VlrTRFBrancaIntermediaria, VlrTRFBrancaForaPonta, NumResolucao, DthInicioVigencia, DthProcessamento) values " + f"({linha[0]},'{linha[1]}','{linha[2]}','{linha[3]}',{linha[4]},{linha[5]},{linha[6]},{linha[7]},{linha[8]},{linha[9]},'{linha[10]}','{linha[11]}','{linha[12]}')" + ";"
            interface_cassandra.inserir(query)   
        except Exception as e:
            print('Erro ao inserir dados tratados - tarifaResidencial', e)
except Exception as e:
    print('Erro ao inserir dados no banco NoSQL - tarifaResidencial: ', e)
    
print("Fim da execução!")