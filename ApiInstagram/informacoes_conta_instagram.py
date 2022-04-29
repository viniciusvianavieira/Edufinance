#https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights

# Import Libraries
from time import process_time_ns
from traceback import print_tb
from matplotlib.pyplot import table
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
import boto3

os.system('cls' if os.name == 'nt' else 'clear')
print()

parametros = Parametros()

url = parametros.params['endpoint_base'] + parametros.params['instagram_account_id'] + '/insights'

# Define Endpoint Parameters
endpointParams = dict()
endpointParams['metric'] = 'impressions, follower_count, reach, email_contacts, phone_call_clicks, text_message_clicks, get_directions_clicks, website_clicks, profile_views'  
#follower_count só conta um periodo de 30 dias

endpointParams['period'] = 'day'
endpointParams['access_token'] = parametros.params['access_token']

# Requests Data
data = requests.get(url, endpointParams )
json_account_metrics = json.loads(data.content)

metrics_name = []
metrics_value = []
metrics_time = []
date_time = []

data = datetime.datetime.now()


metrics_name.append(json_account_metrics['data'][0]['title'])
metrics_value.append(json_account_metrics['data'][0]['values'][0]['value'])
metrics_time.append(json_account_metrics['data'][0]['values'][0]['end_time'])
date_time.append(data)
metrics_name.append(json_account_metrics['data'][0]['title'])
metrics_value.append(json_account_metrics['data'][0]['values'][1]['value'])
metrics_time.append(json_account_metrics['data'][0]['values'][1]['end_time'])
date_time.append(data)

cont = 0
Existe_pagina_anterior = True
while Existe_pagina_anterior:
    cont = cont + 1
    try:
        if cont > 14: #CADA PAGINA TEM 2 DIAS E SÓ PODE 30 DE FOLLOWER_COUNT
            endpointParams['metric'] = 'impressions, reach, email_contacts, phone_call_clicks, text_message_clicks, get_directions_clicks, website_clicks, profile_views'
        url_previous = json_account_metrics['paging']['previous']

        data_previous = requests.get(url_previous,endpointParams)
        json_account_metrics = json.loads(data_previous.content)
        for metrics in json_account_metrics['data']:

            try:
                metrics_name.append(metrics['title'])
                metrics_value.append(metrics['values'][0]['value'])
                metrics_time.append(metrics['values'][0]['end_time'])
                date_time.append(data)
                metrics_name.append(metrics['title'])
                metrics_value.append(metrics['values'][1]['value'])
                metrics_time.append(metrics['values'][1]['end_time'])
                date_time.append(data)
                
            except:
                pass

    except Exception as e:
        Existe_pagina = False
        break
    
    # if cont == 1: #fazendo um contador, pois só precisamos do ultimo dia atualizado(já pegamos a base de dados)
    #     break
    
# Create DataFrame
# date_time = pd.DataFrame(date_time)

df_account_metrics = pd.DataFrame(list(zip(metrics_time,metrics_name,metrics_value)),columns =['UTC_do_dia','Nome' , 'Valor'])

df_account_metrics['UTC_do_dia'] = pd.to_datetime(df_account_metrics['UTC_do_dia'], format='%Y-%m-%d')

df_account_metrics = df_account_metrics.pivot(index="UTC_do_dia",columns="Nome",values="Valor")

date = datetime.datetime.now
df_account_metrics['Data_de_extracao'] = [datetime.datetime.now()] * len(df_account_metrics)


df_account_metrics = df_account_metrics[['Data_de_extracao','Alcance','Impressões','Visualizações do perfil', 'Número de seguidores', 'Cliques no site']]

df_account_metrics = df_account_metrics.rename(columns={'Impressões': 'Impressoes'})
df_account_metrics = df_account_metrics.rename(columns={'Visualizações do perfil': 'Visualizacoes_do_perfil'})
df_account_metrics = df_account_metrics.rename(columns={'Número de seguidores': 'Numero_de_seguidores'})
df_account_metrics = df_account_metrics.rename(columns={'Cliques no site': 'Cliques_no_site'})


# df_account_metrics.dropna(inplace=True) #excluindo linhas com valores vazios

# df_account_metrics = df_account_metrics.tail(1)

df_account_metrics = df_account_metrics.reset_index()

print(df_account_metrics)
print()

utc = df_account_metrics['UTC_do_dia'].to_string() 
utc = utc[4:] #para retirar os vazios
utcNow = df_account_metrics['Data_de_extracao'].to_string()
utcNow = utcNow[4:] #para retirar os vazios

# json = {

#     'Data_de_extracao': str(utcNow),
#     'Utc_do_dia': str(utc),
#     'Alcance': int(df_account_metrics['Alcance']),
#     'Impressoes': int(df_account_metrics['Impressoes']),
#     'Visualizacoes_do_perfil': int(df_account_metrics['Visualizacoes_do_perfil']),
#     'Numero_de_seguidores': int(df_account_metrics['Numero_de_seguidores']),
#     'Cliques_no_site': int(df_account_metrics['Cliques_no_site'])
# }
#print(json)

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('informacoes_conta_instagram')

print(table.creation_date_time)

with table.batch_writer() as batch:
    for i,datas in enumerate(df_account_metrics['Data_de_extracao'].to_list()):
        batch.put_item(
            Item={
                'Data_de_extracao':  str(utcNow),
                'Dados': {
                            'Alcance': df_account_metrics['Alcance'][i],
                            'Impressoes': df_account_metrics['Impressoes'][i],
                            'Numero_de_seguidores': df_account_metrics['Numero_de_seguidores'],
                            'Cliques_no_site': df_account_metrics['Cliques_no_site']

                },
            }
        )

