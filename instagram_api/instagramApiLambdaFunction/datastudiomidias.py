import boto3
import pandas as pd
import os

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

    
    df = pd.DataFrame(list(zip(Data_de_extracao, Id, Engajamento, Alcance, Comentarios, Impressoes, Likes, Salvos, Visualizacoes_dos_videos, Tipo_da_midia, Local_da_midia, UTC_da_postagem)),columns=['Data_de_extracao','Id','Engajamento','Alcance','Comentarios','Impressoes','Likes','Salvos','Visualizacoes_dos_videos','Tipo_da_midia','Local_da_midia','UTC_da_postagem'])
   
    print(df)

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}

    #Criar um endpoint dentro da API