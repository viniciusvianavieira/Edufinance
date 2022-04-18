import pymysql
from sqlalchemy import create_engine

class conexao_aws:

    def __init__(self, senha, usuario, nome_do_banco):
        self.user = usuario
        self.senha = senha
        self.nome_do_banco = nome_do_banco
        self.cursor = None
        self.conexao = None
        self.engine = None

    def iniciar_conexao(self):

        self.estabelecendo_conexao()
        self.utilizando_um_banco_de_dados()
        self.estabelecendo_conexao_sqlalchemy()

    def estabelecendo_conexao(self):
        conexao_amazon = pymysql.connect(host='dados-rede-social.c2cxonpshmlt.us-east-1.rds.amazonaws.com', user=self.user, passwd=self.senha)
        self.conexao = conexao_amazon
        self.cursor = conexao_amazon.cursor()
    
    def utilizando_um_banco_de_dados(self):
        conexao_com_o_banco = self.cursor
        sql_query = f'''use {self.nome_do_banco}'''
        conexao_com_o_banco.execute(sql_query)
    
    def estabelecendo_conexao_sqlalchemy(self):
        conn = f'mysql+mysqlconnector://{self.user}:{self.senha}@dados-rede-social.c2cxonpshmlt.us-east-1.rds.amazonaws.com/redes_sociais'.format(username=self.user, 
                                                                                                                            password=self.senha, 
                                                                                                                            hostname='dados-rede-social.c2cxonpshmlt.us-east-1.rds.amazonaws.com', 
                                                                                                                            database=self.nome_do_banco)
        self.engine = create_engine(conn)


#teste
if __name__ == "__main__":

    import os
    
    usuario_sql = os.getenv('usuario_sql')
    senha_sql = os.getenv('senha_sql')
    
    aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='redes_sociais')
    aws.iniciar_conexao()

    import pandas as pd

    a = pd.read_sql('''SELECT * from informacoes_midias''', con = aws.engine) 

    print(a)