from dynamo_comandos import ComandosDynamo
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
utc = pytz.UTC
# from conexao_banco import conexao_aws

os.system('cls' if os.name == 'nt' else 'clear')
print()


class Informacoes_midia:

    def pegar_informacoes_midia(self):

        parametros = Parametros()

        # Define URL
        url = parametros.params['endpoint_base'] + \
            parametros.params['instagram_account_id'] + '/media'
        # Define Endpoint Parameters
        endpointParams = dict()
        endpointParams['fields'] = 'id,caption,media_product_type,media_type,permalink,timestamp,username,like_count,comments_count,media_url,thumbnail_url'
        endpointParams['access_token'] = parametros.params['access_token']

        # Requests Data
        data = requests.get(url, endpointParams)
        basic_insight = json.loads(data.content)

        all_basics_insights = pd.DataFrame(basic_insight['data'])

        lista_df_insights = []
        lista_df_insights.append(all_basics_insights)

        fotos_ids = []
        for i in range(0, len(basic_insight['data'])):
            # pegando os id's das primeiras paginas
            fotos_ids.append(basic_insight['data'][i]['id'])

        Existe_proxima_pagina = 0
        while Existe_proxima_pagina < 1:

            try:
                Existe_proxima_pagina = Existe_proxima_pagina + 1
                url_next = basic_insight['paging']['next']

                data_next = requests.get(url_next)
                basic_insight = json.loads(data_next.content)
                for i in range(0, len(basic_insight['data'])):
                    # pegando os id's das nextpages
                    fotos_ids.append(str(basic_insight['data'][i]['id']))

            except Exception as e:
                Existe_proxima_pagina = False  # QUANDO QUERO PEGAR A BASE DE DADOS TODA
                break

            lista_df_insights.append(pd.DataFrame(basic_insight['data']))

        all_basics_insights = pd.concat(
            lista_df_insights, ignore_index=True, sort=False)
        #  'id,  caption, media_product_type,   media_type, permalink, timestamp,        username, like_count, comments_count,     media_url'
        all_basics_insights.columns = ['Id', 'Legenda', 'Local_da_midia', 'Tipo_da_midia',
                                       'Link', 'UTC_da_postagem', 'Username', 'Likes', 'Comentarios', 'URL','Thumbnail']

        all_basics_insights['UTC_da_postagem'] = pd.to_datetime(
            all_basics_insights['UTC_da_postagem'])

        media_insight = []

        # Loop Over 'Media ID'
        cont = 0
        tempo_agora = (datetime.datetime.utcnow()).astimezone(tz=None)
        for i, id in enumerate(all_basics_insights['Id'].to_list()):
            id = str(id)
            # intervalo de 7 dias entre as midias
            if all_basics_insights['UTC_da_postagem'][i] + relativedelta(hours=500) > (tempo_agora):

                cont = cont + 1
                parametros.params['latest_media_id'] = str(id)
                # Define URL
                url = parametros.params['endpoint_base'] + parametros.params['latest_media_id'] + '/insights'

                if all_basics_insights['Local_da_midia'][i] == 'VIDEO':

                    # Define Endpoint Parameters
                    endpointParams = dict()
                    endpointParams['metric'] = 'engagement,impressions,reach,saved,video_views'
                    endpointParams['access_token'] = parametros.params['access_token']

                else:

                    # Define Endpoint Parameters
                    endpointParams = dict()
                    endpointParams['metric'] = 'engagement,impressions,reach,saved'
                    endpointParams['access_token'] = parametros.params['access_token']

                # Requests Data
                data = requests.get(url, endpointParams)
                json_data_temp = json.loads(data.content)

                try:
                    media_insight.append(list(json_data_temp['data']))

                except:
                    print("deu erro")

        self.data_hoje = datetime.datetime.now()

        # Initialize Empty Container
        engagement_list = []
        impressions_list = []
        reach_list = []
        saved_list = []
        video_views = []
        date_time = []
        numero = []
        id_certo = []

        # Loop Over Insights to Fill Container
        cont = 0
        for i, insight in enumerate(media_insight):
            print()
            cont = cont + 1
            numero.append(i)
            id_certo.append(insight[0]['id'][:17])
            engagement_list.append(insight[0]['values'][0]['value'])
            impressions_list.append(insight[1]['values'][0]['value'])
            reach_list.append(insight[2]['values'][0]['value'])
            saved_list.append(insight[3]['values'][0]['value'])
            try:
                video_views.append(insight[4]['values'][0]['value'])
            except:
                video_views.append('---')

            date_time.append(self.data_hoje)

        # Create DataFrame
        df_media_insight = pd.DataFrame(list(zip(numero, date_time, id_certo, engagement_list, impressions_list, reach_list, saved_list, video_views,)), columns=[
                                        'Numero', 'Data_de_extracao', 'Id', 'Engajamento', 'Impressoes', 'Alcance', 'Salvos', 'Visualizacoes_dos_videos'])

        self.all_insights = all_basics_insights.merge(
            df_media_insight, on='Id')

        self.all_insights = self.all_insights[['Id', 'Data_de_extracao', 'UTC_da_postagem', 'Likes', 'Comentarios', "Engajamento", 'Impressoes',
                                               'Alcance', 'Salvos', 'Visualizacoes_dos_videos', 'Tipo_da_midia', 'Username', 'Link', 'Legenda', 'Local_da_midia', 'URL','Thumbnail']]

        print(self.all_insights)




teste = ComandosDynamo()
iniciar = Informacoes_midia()
iniciar.pegar_informacoes_midia()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
# table = dynamodb.Table('informacoes_midias_instagram')
table = dynamodb.Table('informacoes_midias_instagram')


class UtilizandoDynamo:

    def adiciona_midias_na_base(self):
        for i, postagem in enumerate(iniciar.all_insights['UTC_da_postagem']):
            teste.comando_adicionar_midias_ao_dynamo(i, iniciar)

    def adiciona_Teste(self):
        for i, postagem in enumerate(iniciar.all_insights['UTC_da_postagem']):
            teste.comando_adicionar_Teste(i, iniciar)

    def confere_e_adiciona_midias_na_base(self):

        # ids pegos pelo request na api
        for i, id_foto in enumerate(iniciar.all_insights['Id']):
            response = table.scan(FilterExpression=Attr('Id').eq(int(id_foto)))

            if len(response['Items']) == 0:  # se id não estiver na base
                print('Id', id_foto, ' não estava na base, foi adicionado')
                teste.comando_adicionar_midias_ao_dynamo(i, iniciar)
            else:
                print('Id', id_foto, 'Já estava na base, PARA DE ME ENCHER')

    def adicionando_com_repeticao_por_tempo(self):

        tempo_agora = (datetime.datetime.utcnow()).astimezone(
            tz=None)  # tranformando na mesma formatação do pandas

        # COMPARAR OS IDS COM OS IDS DA BASE
        for i, id_foto in enumerate(iniciar.all_insights['Id']):
            # o tempo de agora for maior que o tempo da publicação + 15 minutos então
            if tempo_agora > (iniciar.all_insights['UTC_da_postagem'][i] + relativedelta(hours=2)):
                print("Postou a foto de Id ", id_foto,
                      " menos de 7 dias, foi adicionada")
                teste.comando_adicionar_midias_ao_dynamo(i, iniciar)
