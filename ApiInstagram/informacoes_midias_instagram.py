# Import Libraries
from mimetypes import init
from time import process_time_ns
from traceback import print_tb
from typing_extensions import Self
import requests
import json
import datetime
import pandas as pd
import datetime
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from sqlalchemy import true
from conexao_banco import conexao_aws
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
import pytz
utc=pytz.UTC

os.system('cls' if os.name == 'nt' else 'clear')
print()

class Informacoes_midia:
    
    def __init__(self):

        self.params = dict()
        self.params['access_token'] = os.getenv('access_token')#Primeiro coloca o access_token do facebook developers, e o programa te redirecionará um mais longo
        self.params['client_id'] = os.getenv('client_id')#facebook_developers --> settings               
        self.params['client_secret'] = os.getenv('client_secret')#facebook_developers --> settings
        self.params['graph_domain'] = 'https://graph.facebook.com'
        self.params['graph_version'] = 'v12.0'
        self.params['endpoint_base'] = self.params['graph_domain'] + '/' + self.params['graph_version'] + '/'
        self.params['page_id'] = os.getenv('page_id') #Id da página do fb vinculada ao instagram
        self.params['instagram_account_id'] = os.getenv('instagram_account_id') #Id do instagram que o Facebook Developers dá ao conectar
        self.params['ig_username'] = 'edufinance' #Usuario


   

    def pegar_informacoes_midia(self):

        parametros = Parametros()

        # Define URL
        url = parametros.params['endpoint_base'] + parametros.params['instagram_account_id'] + '/media'

        # Define Endpoint Parameters
        endpointParams = dict()
        endpointParams['fields'] = 'id,caption,media_product_type,media_type,permalink,timestamp,username,like_count,comments_count'
        endpointParams['access_token'] = parametros.params['access_token']

        # Requests Data
        data = requests.get(url, endpointParams )
        basic_insight = json.loads(data.content)
        basic_insight1 = basic_insight
        all_basics_insights = pd.DataFrame(basic_insight['data'])

        lista_df_insights = []
        lista_df_insights.append(all_basics_insights)

        fotos_ids = []
        for i in range(0,len(basic_insight['data'])):
            fotos_ids.append(basic_insight['data'][i]['id']) #pegando os id's das primeiras paginas
            

        Existe_proxima_pagina = 0
        while Existe_proxima_pagina < 1:
            
            try:
                Existe_proxima_pagina = Existe_proxima_pagina + 1
                url_next = basic_insight['paging']['next']
                data_next = requests.get(url_next)
                basic_insight = json.loads(data_next.content)
                for i in range(0,len(basic_insight['data'])):
                    fotos_ids.append(basic_insight['data'][i]['id']) #pegando os id's das nextpages

            except Exception as e:
                Existe_proxima_pagina = False #QUANDO QUERO PEGAR A BASE DE DADOS TODA
                break
            lista_df_insights.append(pd.DataFrame(basic_insight['data']))


        all_basics_insights = pd.concat(lista_df_insights, ignore_index=True)
        all_basics_insights.columns = ['Id', 'Legenda', 'Local_da_midia','Tipo_da_midia', 'Link', 'UTC_da_postagem', 'Username', 'Likes', 'Comentarios']

        all_basics_insights['UTC_da_postagem'] = pd.to_datetime(all_basics_insights['UTC_da_postagem'], format='%Y-%m-%d')
        
        media_insight = []

        # Loop Over 'Media ID'
        cont = 0
        for id in all_basics_insights['Id'].to_list():

            cont = cont + 1
            parametros.params['latest_media_id'] = id
            # Define URL
            url = parametros.params['endpoint_base'] + parametros.params['latest_media_id'] + '/insights'

            # Define Endpoint Parameters
            endpointParams = dict() 
            endpointParams['metric'] = 'engagement,impressions,reach,saved,video_views'
            endpointParams['access_token'] = parametros.params['access_token'] 
            
            # Requests Data
            data = requests.get(url, endpointParams )
            json_data_temp = json.loads(data.content)
        
            try:
                media_insight.append(list(json_data_temp['data']))

            except:
                pass

       
        self.data_hoje = datetime.datetime.now()

        # Initialize Empty Container
        engagement_list = []
        impressions_list = []
        reach_list = []
        saved_list = []
        video_views = []
        date_time = []
        numero = []

        # Loop Over Insights to Fill Container
        cont = 0
        for i,insight in enumerate(media_insight):
            cont = cont + 1
            numero.append(i)
            engagement_list.append(insight[0]['values'][0]['value'])
            impressions_list.append(insight[1]['values'][0]['value'])
            reach_list.append(insight[2]['values'][0]['value'])
            saved_list.append(insight[3]['values'][0]['value'])
            video_views.append(insight[4]['values'][0]['value'])
            date_time.append(self.data_hoje)

                     # Create DataFrame
        df_media_insight = pd.DataFrame(list(zip(numero,date_time,fotos_ids, engagement_list, impressions_list, reach_list, saved_list,video_views)), columns =['Numero','Data_de_extracao','Id' , 'Engajamento', 'Impressoes', 'Alcance', 'Salvos','Visualizacoes_dos_videos'])

        self.all_insights = all_basics_insights.merge(df_media_insight)

        self.all_insights = self.all_insights[['Id','Data_de_extracao','UTC_da_postagem','Likes','Comentarios',"Engajamento",'Impressoes','Alcance','Salvos','Visualizacoes_dos_videos','Tipo_da_midia','Username','Link','Legenda','Local_da_midia']]

        print(self.all_insights)
        print()

iniciar = Informacoes_midia()
iniciar.pegar_informacoes_midia()



dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('informacoes_midias_instagram')

# for i,data in enumerate(iniciar.all_insights['Data_de_extracao']):
#     table.put_item(
#     Item={
#             'Data_de_extracao' : str(iniciar.data_hoje),
#             'Id': int(iniciar.all_insights['Id'][i]),
#             'Metricas':{
#                 'Dados':{
#                     'Likes': int(iniciar.all_insights['Likes'][i]),
#                     'Comentarios': int(iniciar.all_insights['Comentarios'][i]),
#                     'Engajamento': int(iniciar.all_insights['Engajamento'][i]),
#                     'Impressoes': int(iniciar.all_insights['Impressoes'][i]),
#                     'Alcance': int(iniciar.all_insights['Alcance'][i]),
#                     'Salvos':int(iniciar.all_insights['Salvos'][i]),
#                     'Visualizacoes_dos_videos': int(iniciar.all_insights['Visualizacoes_dos_videos'][i]),

#                 },

#                 'Informacoes':{
#                     'UTC_da_postagem': str(iniciar.all_insights['UTC_da_postagem'][i]),
#                     'Tipo_da_midia': str(iniciar.all_insights['Tipo_da_midia'][i]),
#                     'Username': str(iniciar.all_insights['Username'][i]),
#                     'Link':str(iniciar.all_insights['Link'][i]),
#                     'Legenda': str(iniciar.all_insights['UTC_da_postagem'][i]),
#                     'Local_da_midia': str(iniciar.all_insights['Local_da_midia'][i]),
#                 },
#             }
            
#         }
#     )

#//////////////////////////////////////////////////////////////////////

#chamar os ids na hora de rodar o programa

# for i,id_foto in enumerate(iniciar.all_insights['Id']): #ids pegos pelo request na api
    
#     response = table.scan(
#     FilterExpression= Attr('Id').eq(int(id_foto)))
#     print(response['Items'])

#     if len(response['Items']) == 0:#se id não estiver na base
#         print()
#         print('Id',id_foto,' não estava na base, foi adicionado')
#         print()
#         table.put_item(
#                    Item={
#                     'Data_de_extracao' : str(iniciar.data_hoje),
#                     'Id': int(iniciar.all_insights['Id'][i]),
#                     'Metricas':{
#                     'Dados':{
#                             'Likes': int(iniciar.all_insights['Likes'][i]),
#                             'Comentarios': int(iniciar.all_insights['Comentarios'][i]),
#                             'Engajamento': int(iniciar.all_insights['Engajamento'][i]),
#                             'Impressoes': int(iniciar.all_insights['Impressoes'][i]),
#                             'Alcance': int(iniciar.all_insights['Alcance'][i]),
#                             'Salvos':int(iniciar.all_insights['Salvos'][i]),
#                             'Visualizacoes_dos_videos': int(iniciar.all_insights['Visualizacoes_dos_videos'][i]),
#                             },
#                     'Informacoes':{
#                             'UTC_da_postagem': str(iniciar.all_insights['UTC_da_postagem'][i]),
#                             'Tipo_da_midia': str(iniciar.all_insights['Tipo_da_midia'][i]),
#                             'Username': str(iniciar.all_insights['Username'][i]),
#                             'Link':str(iniciar.all_insights['Link'][i]),
#                             'Legenda': str(iniciar.all_insights['UTC_da_postagem'][i]),
#                             'Local_da_midia': str(iniciar.all_insights['Local_da_midia'][i]),
#                  },
#              }
            
#          }
#      )
#     else:
#         print()
#         print('Id',id_foto,'Já estava na base, PARA DE ME ENCHER')
#         print()
    


tempo_agora = datetime.datetime.utcnow()  #tranformando na mesma formatação do pandas
df_estatico = pd.DataFrame()



for i, id_foto in enumerate(iniciar.all_insights['Id']): #COMPARAR OS IDS COM OS IDS DA BASE
    response = table.scan(FilterExpression= Attr('Id').eq(int(id_foto)))
    response = json.loads(str(response['Items']))
    print(response['Items'])
    print(type(response['Items']))
    #TRANSFORMAR EM JSON <<<<<<<<<<<<<<<<<<<<<<

    # if tempo_agora < (ids_midias_na_base.iloc[i, 2] + relativedelta(hours=24)): #o tempo de agora for maior que o tempo da publicação + 15 minutos então
    #     print("Postou a foto ",id_foto," menos de 24 horas, foi adicionado")



        # adicionar = ids_midias_na_base[ids_midias_na_base.Id == id_foto] 
        # df_estatico = df_estatico.append(adicionar)
        # print(df_estatico)
        # df_estatico.to_sql('informacoes_midias_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')




