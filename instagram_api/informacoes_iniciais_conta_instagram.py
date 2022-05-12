#https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights

# Import Libraries
from traceback import print_tb
import requests
import json
import datetime
from datetime import date
import pandas as pd
import datetime
import os
import boto3
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
import pytz
utc=pytz.UTC

os.system('cls' if os.name == 'nt' else 'clear')
print()

class informacoes_iniciais_conta_instagram:

        def pegando_informacoes_iniciais(self):

                parametros = Parametros()

                url = parametros.params['endpoint_base'] + parametros.params['instagram_account_id'] 

                # Define Endpoint Parameters
                endpointParams = dict()
                endpointParams['fields'] = 'id,ig_id,name,username,followers_count,follows_count,media_count'
                endpointParams['access_token'] = parametros.params['access_token']

                data = requests.get(url, endpointParams )
                json_account_fields= json.loads(data.content)

                date_time = []
                generic_followers = []
                generic_follows = []
                generic_midia_count = []
                generic_account_id = []


                self.data_do_dia = datetime.date.today()

                date_time.append(self.data_do_dia)
                generic_account_id.append(json_account_fields['id'])
                generic_followers.append(json_account_fields['followers_count'])
                generic_follows.append(json_account_fields['follows_count'])
                generic_midia_count.append(json_account_fields['media_count'])

                self.df_metrics_accounts_generics = pd.DataFrame(list(zip(date_time,generic_followers, generic_follows, generic_midia_count,)), columns =['Data_do_dia','Seguidores','Seguindo','Midias'])


                self.df_metrics_accounts_generics = self.df_metrics_accounts_generics[['Data_do_dia','Seguidores','Seguindo','Midias']]

                print()
                print(self.df_metrics_accounts_generics)
















