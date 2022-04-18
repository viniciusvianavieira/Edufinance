# Import Libraries
from traceback import print_tb
from httplib2 import ProxiesUnavailableError
import requests
import json
import datetime
import pandas as pd
import datetime
import os
from conexao_banco import conexao_aws
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
import pytz
utc=pytz.UTC

os.system('cls' if os.name == 'nt' else 'clear')
print()

parametros = Parametros()



url = parametros.params['endpoint_base'] + parametros.params['instagram_account_id'] + '/stories'

# Define Endpoint Parameters
endpointParams = dict()
endpointParams['access_token'] = parametros.params['access_token']

# Requests Data
data = requests.get(url, endpointParams )
json_stories_id = json.loads(data.content)
json_stories_id = pd.DataFrame(json_stories_id['data'])

names_insights_stories = []
values_insights_stories = []
ids_insights_stories = []

try:
    for id in json_stories_id['id'].to_list():
        url = parametros.params['endpoint_base'] + id + '/insights'

        # Define Endpoint Parameters
        endpointParams = dict()
        endpointParams['metric'] = 'impressions, reach, taps_forward, taps_back, exits, replies'
        endpointParams['access_token'] = parametros.params['access_token']

        # Requests Data
        data = requests.get(url, endpointParams)
        json_stories_insights = json.loads(data.content)
        for insights in json_stories_insights['data']:
            
            names_insights_stories.append( insights['name'])
            values_insights_stories.append( insights['values'][0]['value'])
            valor_id_tratado = insights['id']
            ids_insights_stories.append(valor_id_tratado[0:17])

        

        df_insights_stories = pd.DataFrame(list(zip(ids_insights_stories,names_insights_stories,values_insights_stories)), columns =['Id','Nome','Valor'])


    df_insights_stories = df_insights_stories.pivot(index='Id',columns='Nome',values='Valor') 

    df_insights_stories['Data_de_extracao'] = [datetime.datetime.now()] * len(df_insights_stories)

    df_insights_stories.columns = ['Saidas', 'Impressoes', 'Alcance', 'Respostas', 'Toques_para_voltar', 'Toques_para_avancar', 'Data_de_extracao']

    df_insights_stories = df_insights_stories[['Data_de_extracao','Impressoes','Alcance','Respostas','Saidas','Toques_para_voltar','Toques_para_avancar']]

    id_storie_fields = []
    like_count_storie_fields = []
    media_product_type_fields = []
    media_type_fields = []
    media_url_fields = []
    permalink_fields = []
    shortcode_fields =[]
    timestamp_fields = []
    username_fields = []
    caption_fields = []

    for id in json_stories_id['id'].to_list():
        url = parametros.params['endpoint_base'] + id   

        # Define Endpoint Parameters
        endpointParams = dict()
        endpointParams['fields'] = 'caption,id,like_count,media_product_type,media_type,media_url,owner,permalink,shortcode,thumbnail_url,timestamp,username'
        endpointParams['access_token'] = parametros.params['access_token']

        # Requests Data
        data = requests.get(url, endpointParams)
        json_stories_fields = json.loads(data.content)
    
        id_storie_fields.append(id)
        like_count_storie_fields.append(json_stories_fields['like_count'])
        media_product_type_fields.append(json_stories_fields['media_product_type'])
        media_type_fields.append(json_stories_fields['media_type'])
        media_url_fields.append(json_stories_fields['media_url'])
        permalink_fields.append(json_stories_fields['permalink'])
        shortcode_fields.append(json_stories_fields['shortcode'])
        timestamp_fields.append(json_stories_fields['timestamp'])
        username_fields.append(json_stories_fields['username'])

        try:
            caption_fields.append(json_stories_fields['caption'])
        except:
            caption_fields.append('Nan')
        
        df_fields_stories = pd.DataFrame(list(zip(id_storie_fields, like_count_storie_fields, media_product_type_fields, media_type_fields, media_url_fields, permalink_fields,timestamp_fields, username_fields, caption_fields)), columns =['Id','Likes','Local_da_midia','Tipo_da_midia', 'Thumbnail_url','Link','UTC_da_postagem','Username', 'Legenda'])
 
    all_stories_informations = pd.merge(df_fields_stories,  df_insights_stories, how='left', on = 'Id')

    all_stories_informations = all_stories_informations[['Id','UTC_da_postagem','Data_de_extracao','Likes','Impressoes','Alcance','Respostas','Saidas','Toques_para_voltar','Toques_para_avancar','Tipo_da_midia','Username','Link', 'Thumbnail_url','Local_da_midia','Legenda']]


    all_stories_informations['Id'] = all_stories_informations['Id'].astype(int)

    all_stories_informations['UTC_da_postagem'] = pd.to_datetime(all_stories_informations['UTC_da_postagem'], format='%Y-%m-%d')

    # all_stories_informations['Id']

    usuario_sql = os.getenv('usuario_sql')
    senha_sql = os.getenv('senha_sql')

    aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='redes_sociais')
    aws.iniciar_conexao()


    print(all_stories_informations)

    all_stories_informations.to_sql('informacoes_stories_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')
except:
    print("Nenhum storie encontrado")




