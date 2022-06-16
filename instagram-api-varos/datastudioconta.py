import boto3
import pandas as pd
import os
from conexao_banco import conexao_aws
import mysql.connector
from datetime import datetime, timedelta

os.system('cls' if os.name == 'nt' else 'clear')
print()


def informacoesconta(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_conta_instagram')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    Data_do_dia = []
    Alcance = []
    Cliques_no_site = []
    Impressoes = []
    Numero_de_seguidores = []
    Visualizacoes_do_perfil = []
    Midias = []
    Seguidores = []
    Seguindo = []

    for i,item in enumerate(data):
        Data_do_dia.append(item['Data_do_dia'])

        try:
            Alcance.append(item['Dados']['Alcance'])
        except:
            Alcance.append('---')

        try:
            Cliques_no_site.append(item['Dados']['Cliques_no_site'])
        except:
            Cliques_no_site.append('---')

        try:
            Impressoes.append(item['Dados']['Impressoes'])
        except:
            Impressoes.append('---')

        try:
            Numero_de_seguidores.append(item['Dados']['Numero_de_seguidores'])
        except:
            Numero_de_seguidores.append('---')

        try:
            Visualizacoes_do_perfil.append(item['Dados']['Visualizacoes_do_perfil'])
        except:
            Visualizacoes_do_perfil.append('---')

        try:
            Midias.append(item['Metricas']['Midias'])
        except:
            Midias.append('---')
        
        try:
            Seguidores.append(item['Metricas']['Seguidores'])
        except:
            Seguidores.append('---')

        try:
            Seguindo.append(item['Metricas']['Informacoes']['Seguindo'])
        except:
            Seguindo.append('---')


    
    dfconta = pd.DataFrame(list(zip(Data_do_dia, Impressoes, Alcance, Cliques_no_site, Numero_de_seguidores, Visualizacoes_do_perfil, Midias, Seguidores, Seguindo)),columns=['Data_do_dia','Impressoes','Alcance','Cliques_no_site','Numero_de_seguidores','Visualizacoes_do_perfil','Midias','Seguidores','Seguindo'])

    print(dfconta)


    hoje = datetime.today()
    hoje = hoje.strftime("%Y-%m-%d")
    ontem = datetime.today() - timedelta(days=1)
    ontem = ontem.strftime("%Y-%m-%d")


    for i,data in enumerate(dfconta['Data_do_dia']):
        if str(data) == str(hoje):
            print('Achou hoje',i)
            dfconta.drop(i,axis=0, inplace=True)

        if str(data) == str(ontem):
            print('Achou ontem',i)
            dfconta.drop(i,axis=0, inplace=True)


    usuario_sql = os.getenv('usuario_sql')
    senha_sql = os.getenv('senha_sql')
    host_sql = os.getenv('host_sql')
    database_sql = os.getenv('database_sql')

    aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
    aws.iniciar_conexao()

    conn = mysql.connector.connect(
    user=usuario_sql, password=senha_sql, host=host_sql, database=database_sql
    )   

    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS `informacoes_conta_instagram`;""")
    conn.commit()
    cursor.execute("""  create table informacoes_conta_instagram(
                        Indice int NOT NULL AUTO_INCREMENT primary key,
                        Data_do_dia date,
                        Impressoes bigint(20),
						Alcance bigint(20),
                        Cliques_no_site bigint(20),
                        Numero_de_seguidores bigint(20),
                        Visualizacoes_do_perfil bigint(20),
                        Midias bigint(20),
                        Seguidores bigint(20),
                        Seguindo bigint(20));""") 

    dfconta.to_sql(
        name='informacoes_conta_instagram',
        con=aws.engine,
        schema='edu_db',
        if_exists='append',
        index=False)
        

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}

    #Criar um endpoint dentro da API