import psycopg2

class Interface_db_postgree:

    user, password, host, database = "", "", "", ""
    
    def __init__(self, user, password, host, database):
        """
        Construtor da classe InterfaceDB

        Args:
            user (string): usuario para conexao ao banco
            senha (string): senha para acesso ao banco
            host (string): string contendo o endereco do host
            banco (string): string contendo o nome do banco
        """
        try:
            self.user = user
            self.password = password
            self.host = host
            self.database = database
        except Exception as e:
            print(str(e))

    def conectar(self):
        """Função genérica para conectar ao banco

        Returns:
            con : conector mysql
            cursor : cursor para leitura do banco
        """
        try:
            con = psycopg2.connect(user=self.user, password=self.password, host=self.host, database=self.database)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))
    
    def desconectar(self, con, cursor):
        """Função genérica para desconectar do banco

        Args:
            con : conector mysql
            cursor : cursor para leitura do banco
        """
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e))

    def select(self, query):
        """Função genérica para um select no banco de dados

        Args:
            query (string): query pronta para buscar dados no banco
            
        Returns:
            cursor.fetchall(): retorna tudo que for encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
            return cursor.fetchall() #Retorna tudo o que for encontrado pelo cursor na busca realizada no banco de dados
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)

    def execute(self, query):
        """Função genérica para inserir, alterar ou deletar um dado no banco de dados

        Args:
            query (string): query pronta para buscar dados no banco
             
        Returns:
            result: retorna o resultado da operação
        """
        try:
            con, cursor = self.conectar()
            result = cursor.execute(query)
            con.commit()
            return result
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def get_user(self):
        try:
            return self.user
        except Exception as e:
            print(str(e))
    
    def get_password(self):
        try:
            return self.password
        except Exception as e:
            print(str(e))
            
    def get_host(self):
        try:
            return self.host
        except Exception as e:
            print(str(e))
            
    def get_database(self):
        try:
            return self.database
        except Exception as e:
            print(str(e))

    def get_info_db(self):
        """ Função com as informacões necessárias para o acesso ao banco de dados
        
        Returns:
            user: retorna o usuário conectado
            password: retorna a senha do usuário conectado
            host: retorna o endereço do host conectado
            database: retorna qual a databse em uso
        """
        try:
            return self.get_user(), self.get_password(), self.get_host(), self.get_database()
        except Exception as e:
            print(str(e))