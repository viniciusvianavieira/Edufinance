#https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights

# Import Libraries
from operator import index
from re import A
import requests
import json
import datetime
import pandas as pd
import datetime
import os
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
import pytz
utc=pytz.UTC
import boto3

os.system('cls' if os.name == 'nt' else 'clear')
print()

class informacoes_backup_conta_instagram:

        def pegando_informacoes_backup(self):

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

            data = datetime.date.today()


            metrics_name.append(json_account_metrics['data'][0]['title'])
            metrics_value.append(json_account_metrics['data'][0]['values'][0]['value'])
            data_postagem1 = json_account_metrics['data'][0]['values'][0]['end_time']
            metrics_time.append(data_postagem1[:10])
            date_time.append(data)
            metrics_name.append(json_account_metrics['data'][0]['title'])
            metrics_value.append(json_account_metrics['data'][0]['values'][1]['value'])
            data_postagem2 = json_account_metrics['data'][0]['values'][1]['end_time']
            metrics_time.append(data_postagem2[:10])

            date_time.append(data)


            cont = 0
            Existe_pagina_anterior = 0
            while Existe_pagina_anterior < 1:
                cont = cont + 1
                try:
                    if cont > 14: #CADA PAGINA TEM 2 DIAS E SÓ PODE 30 DE FOLLOWER_COUNT
                        endpointParams['metric'] = 'impressions, reach, email_contacts, phone_call_clicks, text_message_clicks, get_directions_clicks, website_clicks, profile_views'
                    url_previous = json_account_metrics['paging']['previous']

                    data_previous = requests.get(url_previous,endpointParams)
                    json_account_metrics = json.loads(data_previous.content)
                    Existe_pagina_anterior = Existe_pagina_anterior + 1
                    for metrics in json_account_metrics['data']:

                        try:
                            metrics_name.append(metrics['title'])
                            metrics_value.append(metrics['values'][0]['value'])
                            data_postagem1 = metrics['values'][0]['end_time']
                            data_postagem1 = data_postagem1[:10]
                            metrics_time.append(data_postagem1)
                            date_time.append(data)
                            metrics_name.append(metrics['title'])
                            metrics_value.append(metrics['values'][1]['value'])
                            data_postagem2 = metrics['values'][1]['end_time']
                            data_postagem2 = data_postagem2[:10]
                            metrics_time.append(data_postagem2)
                            date_time.append(data)

                        except:
                            pass

                except Exception as e:
                    Existe_pagina = False
                    break


            self.df_account_metrics = pd.DataFrame(list(zip(metrics_time,metrics_name,metrics_value)),columns =['UTC_do_dia','Nome' , 'Valor'])

            self.df_account_metrics = self.df_account_metrics.pivot(index="UTC_do_dia",columns="Nome",values="Valor")

            date = datetime.date.today()
            self.df_account_metrics['Data_de_extracao'] = [datetime.date.today()] * len(self.df_account_metrics)


            self.df_account_metrics = self.df_account_metrics[['Data_de_extracao','Alcance','Impressões','Visualizações do perfil', 'Número de seguidores', 'Cliques no site']]

            self.df_account_metrics = self.df_account_metrics.rename(columns={'Impressões': 'Impressoes'})
            self.df_account_metrics = self.df_account_metrics.rename(columns={'Visualizações do perfil': 'Visualizacoes_do_perfil'})
            self.df_account_metrics = self.df_account_metrics.rename(columns={'Número de seguidores': 'Numero_de_seguidores'})
            self.df_account_metrics = self.df_account_metrics.rename(columns={'Cliques no site': 'Cliques_no_site'})


            # self.df_account_metrics.dropna(inplace=True) #excluindo linhas com valores vazios
            self.df_account_metrics = self.df_account_metrics.reset_index()

            self.df_account_metrics = self.df_account_metrics.fillna('------')
            
            print(self.df_account_metrics)






