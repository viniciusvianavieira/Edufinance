import boto3
import pandas as pd
import os
from conexao_banco import conexao_aws

os.system('cls' if os.name == 'nt' else 'clear')
print()


def informacoesmidias(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_midias_instagram')

    response = table.scan()['Items']

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

    for i,item in enumerate(response):
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
            UTC_da_postagem.append('---')

    
    dfmidias = pd.DataFrame(list(zip(Data_de_extracao, Id, Engajamento, Alcance, Comentarios, Impressoes, Likes, Salvos, Visualizacoes_dos_videos, Tipo_da_midia, Local_da_midia, UTC_da_postagem)),columns=['Data_de_extracao','Id','Engajamento','Alcance','Comentarios','Impressoes','Likes','Salvos','Visualizacoes_dos_videos','Tipo_da_midia','Local_da_midia','UTC_da_postagem'])
   
    print(dfmidias)

    usuario_sql = os.getenv('usuario_sql')
    senha_sql = os.getenv('senha_sql')


    aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
    aws.iniciar_conexao()

    dfmidias.to_sql(

        name='informacoes_midias_instagram',
        con=aws.engine,
        schema='edu_db',
        if_exists='replace',
        index='false'
    )
    print('botou')

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}

    #Criar um endpoint dentro da API

