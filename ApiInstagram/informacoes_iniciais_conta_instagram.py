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

        def pegando_informacoes_iniciais():

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


                data = datetime.date.today()

                date_time.append(data)
                generic_account_id.append(json_account_fields['id'])
                generic_followers.append(json_account_fields['followers_count'])
                generic_follows.append(json_account_fields['follows_count'])
                generic_midia_count.append(json_account_fields['media_count'])

                df_metrics_accounts_generics = pd.DataFrame(list(zip(date_time,generic_followers, generic_follows, generic_midia_count,)), columns =['Data_do_dia','Seguidores','Seguindo','Midias'])


                df_metrics_accounts_generics = df_metrics_accounts_generics[['Data_do_dia','Seguidores','Seguindo','Midias']]

                print()
                print(df_metrics_accounts_generics)


                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_conta_instagram')

                table.update_item(
                        Key={
                        'Data_do_dia': str(data),
                         },
                        UpdateExpression='SET Dados = :valor',
                        ExpressionAttributeValues={
                        ':valor': {
                                "Seguidores": int(df_metrics_accounts_generics['Seguidores'][0]),
                                "Seguindo:": int(df_metrics_accounts_generics['Seguindo'][0]),
                                "Midias": int(df_metrics_accounts_generics['Midias'][0]),

                                }
                        }
                
                )


# informacoes_iniciais_conta_instagram.pegando_informacoes_iniciais()















