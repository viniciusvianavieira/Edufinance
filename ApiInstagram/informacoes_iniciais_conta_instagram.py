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
from sqlalchemy import DATE
from conexao_banco import conexao_aws
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
import pytz
utc=pytz.UTC

os.system('cls' if os.name == 'nt' else 'clear')
print()

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


data = datetime.datetime.now()

date_time.append(data)
generic_account_id.append(json_account_fields['id'])
generic_followers.append(json_account_fields['followers_count'])
generic_follows.append(json_account_fields['follows_count'])
generic_midia_count.append(json_account_fields['media_count'])

df_metrics_accounts_generics = pd.DataFrame(list(zip(date_time,generic_followers, generic_follows, generic_midia_count,)), columns =['Data_de_extracao','Seguidores','Seguindo','Midias'])


df_metrics_accounts_generics = df_metrics_accounts_generics[['Data_de_extracao','Seguidores','Seguindo','Midias']]

json = { str(data):{
 "Data_de_extracao" : str(data),
 "Seguidores": df_metrics_accounts_generics['Seguidores'][0],
 "Seguindo:": df_metrics_accounts_generics['Seguindo'][0],
 "Midias": df_metrics_accounts_generics['Midias'][0]
}}

print(json)

import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('informacoes_iniciais_conta_instagram')
print()
print("Data da criação: ",table.creation_date_time)
print()

table.put_item(
   Item={
        "Data_de_extracao" : str(data),
        "Seguidores": int(df_metrics_accounts_generics['Seguidores'][0]),
        "Seguindo:": int(df_metrics_accounts_generics['Seguindo'][0]),
        "Midias": int(df_metrics_accounts_generics['Midias'][0]),
    }
)




















# #CREATING TABLE
# dynamodb = boto3.resource('dynamodb')

# # Create the DynamoDB table.
# table = dynamodb.create_table(
#     TableName='informacoes_iniciais_conta_instagram',
#     KeySchema=[
#         {
#             'AttributeName': 'Data_de_extracao',
#             'KeyType': 'HASH'
#         }
#     ],
#     AttributeDefinitions=[
#         {
#             'AttributeName': 'Data_de_extracao',
#             'AttributeType': 'S'
#         }
#     ],
#     ProvisionedThroughput={
#         'ReadCapacityUnits': 5,
#         'WriteCapacityUnits': 5
#     }
# )

# # Wait until the table exists.
# table.wait_until_exists()

# # Print out some data about the table.
# print(table.item_count)