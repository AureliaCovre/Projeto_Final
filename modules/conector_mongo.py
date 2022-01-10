from pymongo import MongoClient

class Interface_db_mongo():
    client = ""
    database = ""
    collection = ""
    
    def __init__(self, host = "mongodb://127.0.0.1:27017"):
        """Construtor da classe Interface_db

        Args:
            host (str, optional): Caminho de conexão com o banco mongoDB, defaults to "mongodb://127.0.0.1:27017".
        """
        try:
            self.cliente = MongoClient(host)
            self.setDatabase()
            self.setCollection()
        except Exception as e:
            print(str(e))
                
    def setDatabase(self, database = "soulcode"):
        """ Muda a database na qual o usuário está usando

        Args:
            database (str, optional): Nome da Database. Defaults to "soulcode".
        """
        try:
            self.database = self.cliente[database]
        except Exception as e:
            print(str(e))
        
    def setCollection(self, collection = "professores"):
        """ Muda a collection que o usuário está usando

        Args:
            collection (str, optional): Nome da collection. Defaults to "professores".
        """
        try:
            self.collection = self.database[collection]
        except Exception as e:
            print(str(e))
        
    def search(self):
        """Função que faz a busca de todos os dados de uma collection

        Returns:
            list: Lista com todos os dados da collection
        """
        try:
            lista = []
            dados = self.collection.find()
            for dado in dados:
                lista.append(dado)
            return lista
        except Exception as e:
            print(str(e))
               
    def insert_one(self, dado):
        """Função para inserir um dado em uma collection

        Args:
            dado (dict): Dado a ser inserido
        """
        try:
            self.collection.insert_one(dado)
        except Exception as e:
            print(str(e))
            
    def insert_many(self, dados):
        """Função para inserir vários dados em uma collection

        Args:
            dados (list(dict)): Lista com os dados a serem inseridos 
        """
        try:
            self.collection.insert_many(dados)
        except Exception as e:
            print(str(e))
    
    def delete_one(self, dado):
        """ Função que deleta um dado de uma collection

        Args:
            dado (dict): Dado a ser removido
        """
        try:
            self.collection.delete_one(dado)
        except Exception as e:
            print(str(e))
            
    def delete_many(self, dados):
        """ Função que deleta varios dados de uma collection

        Args:
            dados (dict): Lista com os dados a serem removidos 
        """
        try:
            self.collection.delete_many(dados)
        except Exception as e:
            print(str(e))
            
    def update_one(self, dado, novoDado):
        """ Função que atualiza um dado de uma collection

        Args:
            dado (dict): Dado a ser atualizado 
            novoDado (dict) : novo valor do dado
        """
        try:
            self.collection.update_one(dado, novoDado)
        except Exception as e:
            print(str(e))
            
    def update_many(self, dados, novoDado):
        """ Função que atualiza vários dados de uma collection

        Args:
            dados (dict): Dados a ser atualizado 
            novoDado (dict) : novo valor do dado
        """
        try:
            self.collection.update_many(dados, novoDado)
        except Exception as e:
            print(str(e))