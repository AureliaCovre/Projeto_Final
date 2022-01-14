from cassandra.cluster import Cluster

class Interface_db_cassandra():

    cluster = ""
    session = ""
    
    #TODO: FAZER TRATAMENTO DE DADOS
    def __init__(self, host, porta, database = "projeto_final"):
        try:
            self.cluster = Cluster([host], port = porta)
            self.set_session(database)
        except Exception as e:
            print("Erro:", e)
    
    def set_session(self, database):
        try:
            self.session=self.cluster.connect(database)
        except Exception as e:
            print("Erro:", e)
            
    def fetchall(self, dados):
        try:
            lista = []
            for d in dados:
                lista.append(d)
            return lista
        except Exception as e:
            print("Erro:", e)
        
    def buscar(self, query):
        try:
            dados = self.session.execute(query)
            lista = self.fetchall(dados)
            return lista
        except Exception as e:
            print("Erro na busca:", e)
            
    def inserir(self, query):
        try:
            self.session.execute(query)
        except Exception as e:
            print("Erro na inserção:", e)
     
    def atualizar(self, query):
        try:
            self.session.execute(query)   
        except Exception as e:
            print("Erro na atualização:", e)
                
    def deletar(self, query):
        try:
            self.session.execute(query)  
        except Exception as e:
            print("Erro na deleção:", e)
                    