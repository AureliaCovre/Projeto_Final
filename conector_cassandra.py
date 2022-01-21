from cassandra.cluster import Cluster

class Interface_db_cassandra():

    cluster = ""
    session = ""
    
    def __init__(self, host, porta, database = "projeto_final"):
        """Construtor da classe Interface_db_cassandra

        Args:
            host (string): ip de acesso ao banco
            porta (string): porta de acesso ao banco
            database (string): nome do banco, default = projeto_final
        """
        try:
            self.cluster = Cluster([host], port = porta)
            self.set_session(database)
        except Exception as e:
            print("Erro:", e)
    
    def set_session(self, database):
        """Função que seta uma nova sessão

        Args:
            database (string): nome do banco
        """
        try:
            self.session=self.cluster.connect(database)
        except Exception as e:
            print("Erro:", e)
            
    def fetchall(self, dados):
        """Função auxiliar para a função de busca

        Args:
            dados (list): dados brutos buscados no banco

        Returns:
            list: lista com os dados 
        """
        try:
            lista = []
            for d in dados:
                lista.append(d)
            return lista
        except Exception as e:
            print("Erro:", e)
        
    def buscar(self, query):
        """Função genérica para um select no banco de dados

        Args:
            query (string): query de busca

        Returns:
            list: lista com os dados
        """
        try:
            dados = self.session.execute(query)
            lista = self.fetchall(dados)
            return lista
        except Exception as e:
            print("Erro na busca:", e)
            
    def inserir(self, query):
        """Função genérica para um insert no banco de dados

        Args:
            query (string): query de inserção
        """
        try:
            self.session.execute(query)
        except Exception as e:
            print("Erro na inserção:", e)
     
    def atualizar(self, query):
        """Função genérica para um update no banco de dados

        Args:
            query (string): query de atualização
        """
        try:
            self.session.execute(query)   
        except Exception as e:
            print("Erro na atualização:", e)
                
    def deletar(self, query):
        """Função genérica para um delete no banco de dados

        Args:
            query (string): query de deleção
        """
        try:
            self.session.execute(query)  
        except Exception as e:
            print("Erro na deleção:", e)
                    