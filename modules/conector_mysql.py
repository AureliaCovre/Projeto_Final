import mysql.connector

class Interface_db_mysql():
    usuario, senha, host, banco = "","","",""

    def __init__(self, usuario, senha, host, banco):
        """Construtor da classe interface_db

        Args:
            usuario (string): usuario do banco
            senha (string): senha de acesso ao banco
            host (string): ip de acesso ao banco
            banco (string): nome do banco
        """
        try:
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.banco = banco
        except Exception as e:
            print(str(e))
    
    def conectar(self):
        """Função genérica para conectar ao banco

        Returns:
            con : conector mysql
            cursor : cursor para leitura do banco
        """
        try:
            con = mysql.connector.connect(user = self.usuario, password = self.senha, host = self.host, database=self.banco)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))

    def desconectar(self,con,cursor):
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

    def select(self, o_que, de_onde, argumentos):
        """Função genérica para um select no banco de dados

        Args:
            o_que (string): o que você quer buscar/selecionar
            de_onde (string): de qual tabela
            argumentos (string): quais os argumentos ? (dentro do where)

        Returns:
            cursor.fetchall(): retorna tudo que for encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            query = "select " + o_que + " from " + de_onde + " "+ argumentos + ";"
            cursor.execute(query)
            return cursor.fetchall() #Retorna tudo o que for encontrado pelo cursor na busca realizada no banco de dados
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
    
    def insert(self, de_onde, argumentos, valores):
        """Função genérica para inserir um dado no banco de dados

        Args:
            de_onde (string): de qual tabela
            argumentos (string): colunas da tabela a ser inserida ? (dentro do where)
            valores (string): valores dos dados a ser inserido 
        Returns:
            cursor.fetchall(): retorna tudo que for encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            query = "insert into " + de_onde + argumentos + " values " + valores + ";"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
            
    def update(self, de_onde, coluna, novo_dado, outros_dados):
        """Função genérica para alterar um dado no banco de dados

        Args:
            de_onde (string): de qual tabela
            coluna (string): coluna a ser alterada
            novo_dado (string): novo valor a ser alterado
            outros_dados (string): quais os argumentos ? (dentro do where) 
        Returns:
            cursor.fetchall(): retorna tudo que for encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            query = "UPDATE " + de_onde + " SET "+ coluna +" = "+ novo_dado + " WHERE "+ outros_dados+";"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)

    def delete(self, de_onde, argumentos):
        """Função genérica para um delete de algum dado no banco de dados

        Args:
            de_onde (string): de qual tabela
            argumentos (string): quais os argumentos ? (dentro do where)

        Returns:
            cursor.fetchall(): retorna tudo que for encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            query = "delete from " + de_onde + " where " + argumentos + ";"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
    
