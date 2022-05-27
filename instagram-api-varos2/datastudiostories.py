import boto3
import pandas as pd
import os

os.system('cls' if os.name == 'nt' else 'clear')
print()


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

    
    df = pd.DataFrame(list(zip(Data_de_extracao, Id, Saidas, Alcance, Respostas, Impressoes, Likes, Toques_para_avancar, Toques_para_voltar, Tipo_da_midia, Local_da_midia, UTC_da_postagem)),columns=['Data_de_extracao','Id','Saidas','Alcance','Respostas','Impressoes','Likes','Toques_para_avancar','Toques_para_voltar','Tipo_da_midia','Local_da_midia','UTC_da_postagem'])
    

    print(df)

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}