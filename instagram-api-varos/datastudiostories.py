import boto3
import pandas as pd
import os
from conexao_banco import conexao_aws
os.system('cls' if os.name == 'nt' else 'clear')
print()
import mysql.connector


def informacoesstories(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_stories_instagram')

    response = table.scan()['Items']

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

    for i,item in enumerate(response):
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
            UTC_da_postagem.append('---')        

    
    dfstories = pd.DataFrame(list(zip(Data_de_extracao, Id, Saidas, Alcance, Respostas, Impressoes, Likes, Toques_para_avancar, Toques_para_voltar, Tipo_da_midia, Local_da_midia, UTC_da_postagem)),columns=['Data_de_extracao','Id','Saidas','Alcance','Respostas','Impressoes','Likes','Toques_para_avancar','Toques_para_voltar','Tipo_da_midia','Local_da_midia','UTC_da_postagem'])
    

    print(dfstories)

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
    cursor.execute("DROP TABLE informacoes_stories_instagram") 

    dfstories.to_sql(
        name='informacoes_stories_instagram',
        con=aws.engine,
        schema='edu_db',
        if_exists='replace',
        index='false'
    )

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}