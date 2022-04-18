#https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights

# Import Libraries
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

df_metrics_accounts_generics

usuario_sql = os.getenv('usuario_sql')
senha_sql = os.getenv('senha_sql')
    
aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='redes_sociais')
aws.iniciar_conexao()


# df_metrics_accounts_generics.to_sql('informacoes_iniciais_conta_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')