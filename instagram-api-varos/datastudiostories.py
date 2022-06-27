import mysql.connector
import boto3
import pandas as pd
import os
from conexao_banco import conexao_aws
from dateutil.relativedelta import relativedelta
from boto3 import client
import datetime
from datetime import date, datetime


os.system('cls' if os.name == 'nt' else 'clear')
print()


def informacoesstories(event, context):

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('informacoes_stories_instagram')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    Data_de_extracao = []
    Id = []
    Alcance = []
    Impressoes = []
    Likes = []
    Respostas = []
    Saidas = []
    Toques_para_avancar = []
    Toques_para_voltar = []
    Tipo_da_midia = []
    Local_da_midia = []
    UTC_da_postagem = []
    Thumbnail = []
    Media_url = []
    awsurl = []

    tempo_agora = datetime.strptime(str((datetime.utcnow())), '%Y-%m-%d %H:%M:%S.%f').date()

    for i, item in enumerate(data):
        Data_de_extracao.append(item['Data_de_extracao'])
        Id.append(item['Id'])
        try:
            Alcance.append(item['Metricas']['Dados']['Alcance'])
        except:
            Alcance.append('---')

        try:
            Respostas.append(item['Metricas']['Dados']['Respostas'])
        except:
            Respostas.append('---')

        try:
            Saidas.append(item['Metricas']['Dados']['Saidas'])
        except:
            Saidas.append('---')

        try:
            Impressoes.append(item['Metricas']['Dados']['Impressoes'])
        except:
            Impressoes.append('---')

        try:
            Likes.append(item['Metricas']['Dados']['Likes'])
        except:
            Likes.append('---')

        try:
            Toques_para_avancar.append(item['Metricas']['Dados']['Toques_para_avancar'])
        except:
            Toques_para_avancar.append('---')

        try:
            Toques_para_voltar.append(item['Metricas']['Dados']['Toques_para_voltar'])
        except:
            Toques_para_voltar.append('---')

        try:
            Tipo_da_midia.append(
                item['Metricas']['Informacoes']['Tipo_da_midia'])
        except:
            Tipo_da_midia.append('---')

        try:
            UTC_da_postagem.append(
                item['Metricas']['Informacoes']['UTC_da_postagem'])
        except:
            UTC_da_postagem.append('---')
        try:
            Local_da_midia.append(
                item['Metricas']['Informacoes']['Local_da_midia'])
        except:
            Local_da_midia.append('---')
        try:
            Media_url.append(item['Metricas']['Informacoes']['Media_url'])
        except:
            Media_url.append('---')
        try:
            Thumbnail.append(item['Metricas']['Informacoes']['Thumbnail'])
        except:
            Thumbnail.append('---')

        try:
            awsurl.append(item['Metricas']['Informacoes']['AWS_URL'])
        except:
            awsurl.append('---')

     
    
    dfstories = pd.DataFrame(list(zip(Data_de_extracao, Id, Saidas, Alcance, Respostas, Impressoes, Likes, Toques_para_avancar, Toques_para_voltar, Tipo_da_midia, Local_da_midia, UTC_da_postagem, Media_url, Thumbnail, awsurl)), columns=[
                             'Data_de_extracao', 'Id', 'Saidas', 'Alcance', 'Respostas', 'Impressoes', 'Likes', 'Toques_para_avancar', 'Toques_para_voltar', 'Tipo_da_midia', 'Local_da_midia', 'UTC_da_postagem', 'Media_url', 'Thumbnail', 'AWS_URL'])

    print(dfstories)

    usuario_sql = os.getenv('usuario_sql')
    senha_sql = os.getenv('senha_sql')
    host_sql = os.getenv('host_sql')
    database_sql = os.getenv('database_sql')

    aws = conexao_aws(senha=senha_sql, usuario=usuario_sql,
                      nome_do_banco='edu_db')
    aws.iniciar_conexao()

    conn = mysql.connector.connect(
        user=usuario_sql, password=senha_sql, host=host_sql, database=database_sql
    )

    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS `informacoes_stories_instagram`;""")
    conn.commit()
    cursor.execute("""  create table informacoes_stories_instagram(
                        Indice int NOT NULL AUTO_INCREMENT primary key,
                        Data_de_extracao datetime,
                        Id varchar(20),
                        Saidas bigint(20),
                        Alcance bigint(20),
                        Respostas bigint(20),
                        Impressoes bigint(20),
                        Likes bigint(20),
                        Toques_para_avancar bigint(20),
                        Toques_para_voltar bigint(20),
                        Tipo_da_midia varchar(25),
                        Local_da_midia varchar(25),
                        UTC_da_postagem datetime,
                        Media_url varchar(5000),
                        Thumbnail varchar(5000),
                        AWS_URL varchar(5000));""")

    dfstories.to_sql(
        name='informacoes_stories_instagram',
        con=aws.engine,
        schema='edu_db',
        if_exists='append',
        index=False)

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}
