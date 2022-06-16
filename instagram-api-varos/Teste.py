from h11 import Data
import requests
import json
import datetime
import pandas as pd
import datetime
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from dateutil.relativedelta import relativedelta
from sqlalchemy import true
from accessToken_e_endpoints import Parametros
from botocore.exceptions import ClientError
import pytz
utc=pytz.UTC
from dynamo_comandos import ComandosDynamo
# from conexao_banco import conexao_aws

os.system('cls' if os.name == 'nt' else 'clear')
print()

id_teste = 17957772589477046


parametros = Parametros()

# Define URL
url = parametros.params['endpoint_base'] + parametros.params['instagram_account_id'] + '/media'
# Define Endpoint Parameters

endpointParams = dict()
endpointParams['fields'] = 'id,caption,media_product_type,media_type,permalink,timestamp,username,like_count,comments_count,media_url'
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
while Existe_proxima_pagina < 40:
    
    try:
        Existe_proxima_pagina = Existe_proxima_pagina + 1
        url_next = basic_insight['paging']['next']

        data_next = requests.get(url_next)
        basic_insight = json.loads(data_next.content)
        for i in range(0,len(basic_insight['data'])):
            if basic_insight['data'][i]['id'] == str(id_teste):
                print('Aqui foi')
                print(basic_insight['data'][i]['id'])
                print(basic_insight['data'][i])
                fotos_ids.append(id_teste) #pegando os id's das nextpages

    except Exception as e:
        print("não conseguiu")
        break


url = parametros.params['endpoint_base'] + str(id_teste) + '/insights'
endpointParams = dict() 
endpointParams['metric'] = 'engagement,impressions,reach,saved,video_views'
endpointParams['access_token'] = parametros.params['access_token']


data = requests.get(url, endpointParams )
json_data_temp = json.loads(data.content)
print(json_data_temp)



