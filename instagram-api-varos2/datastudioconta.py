import boto3
import pandas as pd
import os
from conexao_banco import conexao_aws


os.system('cls' if os.name == 'nt' else 'clear')
print()


def informacoesconta(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_conta_instagram')

    response = table.scan()['Items']

    Data_do_dia = []
    Alcance = []
    Cliques_no_site = []
    Impressoes = []
    Numero_de_seguidores = []
    Visualizacoes_do_perfil = []
    Midias = []
    Seguidores = []
    Seguindo = []

    for i,item in enumerate(response):
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

    usuario_sql = os.getenv('usuario_sql')
    senha_sql = os.getenv('senha_sql')



    aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
    aws.iniciar_conexao()

    a = pd.read_sql('''SELECT * from edu_db.alunos_curso''', con = aws.engine)
    print(a) 

    dfconta.to_sql('edu_db.informacoes_conta_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')
    

    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}

    #Criar um endpoint dentro da API