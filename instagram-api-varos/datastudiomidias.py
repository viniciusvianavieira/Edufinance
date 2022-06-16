import boto3
import pandas as pd
import os
from conexao_banco import conexao_aws
import mysql.connector

os.system('cls' if os.name == 'nt' else 'clear')
print()


def informacoesmidias(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_midias_instagram')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    Data_de_extracao = []
    Id = []
    Engajamento = []
    Alcance = []
    Comentarios = []
    Impressoes = []
    Likes = []
    Salvos = []
    Visualizacoes_dos_videos = []
    Tipo_da_midia = []
    Local_da_midia = []
    UTC_da_postagem = []
    Link = []
    Url = []


    for i,item in enumerate(data):

        Data_de_extracao.append(item['Data_de_extracao'])
        Id.append(item['Id'])

        try:
            Alcance.append(item['Metricas']['Dados']['Alcance'])
        except:
            Alcance.append('---')

        try:
            Comentarios.append(item['Metricas']['Dados']['Comentarios'])
        except:
            Comentarios.append('---')

        try:
            Engajamento.append(item['Metricas']['Dados']['Engajamento'])
        except:
            Engajamento.append('---')
            
        try:
            Impressoes.append(item['Metricas']['Dados']['Impressoes'])
        except:
            Impressoes.append('---')

        try:
            Likes.append(item['Metricas']['Dados']['Likes'])
        except:
            Likes.append('---')

        try:
            Salvos.append(item['Metricas']['Dados']['Salvos'])
        except:
            Salvos.append('---')

        try:
            Visualizacoes_dos_videos.append(item['Metricas']['Dados']['Visualizacoes_dos_videos'])
        except:
            Visualizacoes_dos_videos.append('---')
        
        try:
            Tipo_da_midia.append(item['Metricas']['Informacoes']['Tipo_da_midia'])
        except:
            Tipo_da_midia.append('---')

        try:
            UTC_da_postagem.append(item['Metricas']['Informacoes']['UTC_da_postagem'])
        except:
            UTC_da_postagem.append('---')
        try:
            Local_da_midia.append(item['Metricas']['Informacoes']['Local_da_midia'])
        except:
            Local_da_midia.append('---')
        try:
            Link.append(item['Metricas']['Informacoes']['Link'])
        except:
            Link.append('---')
        try:
            Url.append(item['Metricas']['Informacoes']['URL'])
        except:
            Url.append('---')



    
    dfmidias = pd.DataFrame(list(zip(Data_de_extracao, Id, Engajamento, Alcance, Comentarios, Impressoes, Likes, Salvos, Visualizacoes_dos_videos, Tipo_da_midia, Local_da_midia, UTC_da_postagem,Link, Url)),columns=['Data_de_extracao','Id','Engajamento','Alcance','Comentarios','Impressoes','Likes','Salvos','Visualizacoes_dos_videos','Tipo_da_midia','Local_da_midia','UTC_da_postagem', 'Link', 'URL'])
   
    print(dfmidias)

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
    cursor.execute("""DROP TABLE IF EXISTS `informacoes_midias_instagram`;""")
    conn.commit()
    cursor.execute("""  create table informacoes_midias_instagram(
                        Indice int NOT NULL AUTO_INCREMENT primary key,
                        Data_de_extracao datetime,
                        Id varchar(20),
                        Engajamento bigint(20),
                        Alcance bigint(20),
                        Comentarios bigint(20),
                        Impressoes bigint(20),
                        Likes bigint(20),
                        Salvos bigint(20),
                        Visualizacoes_dos_videos bigint(20),
                        Tipo_da_midia varchar(25),
                        Local_da_midia varchar(25),
                        UTC_da_postagem datetime,
                        Link varchar(350),
                        URL varchar(350));;""") 


    dfmidias.to_sql(

        name='informacoes_midias_instagram',
        con=aws.engine,
        schema='edu_db',
        if_exists='append',
        index=False)
        

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}

    #Criar um endpoint dentro da API

