from cassandra.cluster import Cluster

class Interface_db_cassandra():

    cluster = ""
    session = ""
    
    #TODO: FAZER TRATAMENTO DE DADOS
    def __init__(self, database = "soulcode"):
        self.cluster = Cluster()
        self.set_session(database)
        
    
    def set_session(self, database):
        self.session=self.cluster.connect(database)
        
    def fetchall(self, dados):
        lista = []
        for d in dados:
            lista.append(d)
        return lista
        
    def buscar(self, query):
        dados = self.session.execute(query)
        lista = self.fetchall(dados)
        return lista
            
    def inserir(self, query):
        self.session.execute(query)
        
     
    def atualizar(self, query):
        self.session.execute(query)   
    
                
    def deletar(self, query):
        self.session.execute(query)  
        
    def get_matricula(self, nome):
        query = "SELECT matricula FROM alunos WHERE nome = '"+ str(nome) +"' allow filtering;" 
        resultado_busca = self.buscar(query)
        return resultado_busca[0][0]
        
    def deletar_por_nome(self, nome):
        matricula = self.get_matricula(nome)
        query2 = "DELETE FROM alunos WHERE matricula = "+ str(matricula) +";"
        self.deletar(query2)        
        
    def atualizar_por_nome(self,nome, novo_dado):
        matricula = self.get_matricula(nome)
        query2 = "UPDATE alunos SET "+ str(novo_dado) +" WHERE matricula = "+ str(matricula) +";"
        self.atualizar(query2) 
        