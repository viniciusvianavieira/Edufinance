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

Existe_proxima_pagina = True
while Existe_proxima_pagina:
    try:
        url_next = basic_insight['paging']['next']
        data_next = requests.get(url_next)
        basic_insight = json.loads(data_next.content)

        for i in range(0,len(basic_insight['data'])):
            fotos_ids.append(basic_insight['data'][i]['id']) #pegando os id's das nextpages

    except Exception as e:
        Existe_proxima_pagina = False
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

    if cont > 100:
        break

data = datetime.datetime.now()

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
    date_time.append(data)

    if cont > 7:
        break

# Create DataFrame
df_media_insight = pd.DataFrame(list(zip(numero,date_time,fotos_ids, engagement_list, impressions_list, reach_list, saved_list,video_views)), columns =['Numero','Data_de_extracao','Id' , 'Engajamento', 'Impressoes', 'Alcance', 'Salvos','Visualizacoes_dos_videos'])

all_insights = all_basics_insights.merge(df_media_insight)

all_insights = all_insights[['Id','Data_de_extracao','UTC_da_postagem','Likes','Comentarios',"Engajamento",'Impressoes','Alcance','Salvos','Visualizacoes_dos_videos','Tipo_da_midia','Username','Link','Legenda','Local_da_midia']]

usuario_sql = os.getenv('usuario_sql')
senha_sql = os.getenv('senha_sql')
    
aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='redes_sociais')
aws.iniciar_conexao()

# all_insights.to_sql('informacoes_midias_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')

print(all_insights)
print()

#IDENTIFICA SE TEM MIDIA E SE ESSA MIDIA FOI POSTADA HÁ MENOS DE 15 MINUTOS E ADICIONA AO BANCO DE DADOS
#>>>> BANCO DE DADOS RODANDO A CADA 1 MINUTO <<<<<<<<<<


ids_midias = all_insights['Id']#chamar os ids na hora de rodar o programa
ids_midias_na_base = (pd.read_sql(f'''SELECT * FROM informacoes_midias_instagram ''', con= aws.engine)).tail(10) #pegando os ids do bancos de dados

df_estatico = pd.DataFrame()
for id_foto in ids_midias: #ids pegos pelo request na api
    if int(id_foto) not in ids_midias_na_base['Id'].to_list():#se id não estiver na base
        adicionar = all_insights[all_insights.Id == id_foto]
        df_estatico = pd.concat([df_estatico, adicionar], axis=0, ignore_index=True)
        print('Id',id_foto,' não estava na base, foi adicionado')



tempo_agora = datetime.datetime.utcnow()  #tranformando na mesma formatação do pandas
df_estatico = pd.DataFrame()
for i, id_foto in enumerate(ids_midias_na_base['Id'].to_list()): #COMPARAR OS IDS COM OS IDS DA BASE
    if tempo_agora < (ids_midias_na_base.iloc[i, 3] + relativedelta(weeks=1)): #o tempo de agora for maior que o tempo da publicação + 15 minutos então
        adicionar = ids_midias_na_base[ids_midias_na_base.Id == id_foto] 
        df_estatico = df_estatico.append(adicionar)




print()
print(df_estatico)

#df_estatico.to_sql('informacoes_midias_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')