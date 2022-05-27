import requests
import json
import datetime
import pandas as pd
import datetime
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
from botocore.exceptions import ClientError
import pytz
utc=pytz.UTC
from dynamo_comandos import ComandosDynamo
# from conexao_banco import conexao_aws

os.system('cls' if os.name == 'nt' else 'clear')
print()

class Informacoes_midia:

    def pegar_informacoes_midia(self):

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


        Existe_proxima_pagina = 0
        while Existe_proxima_pagina < 1:
            
            try:
                Existe_proxima_pagina = Existe_proxima_pagina + 1
                url_next = basic_insight['paging']['next']
                data_next = requests.get(url_next)
                basic_insight = json.loads(data_next.content)
                for i in range(0,len(basic_insight['data'])):
                    fotos_ids.append(basic_insight['data'][i]['id']) #pegando os id's das nextpages

            except Exception as e:
                Existe_proxima_pagina = False #QUANDO QUERO PEGAR A BASE DE DADOS TODA
                break
        
            lista_df_insights.append(pd.DataFrame(basic_insight['data']))

        all_basics_insights = pd.concat(lista_df_insights, ignore_index=True)
        all_basics_insights.columns = ['Id', 'Legenda', 'Local_da_midia','Tipo_da_midia', 'Link', 'UTC_da_postagem', 'Username', 'Likes', 'Comentarios']

        all_basics_insights['UTC_da_postagem'] = pd.to_datetime(all_basics_insights['UTC_da_postagem'], format='%Y-%m-%d')
        
        media_insight = []


        # Loop Over 'Media ID'
        cont = 0
        tempo_agora = (datetime.datetime.utcnow()).astimezone(tz=None)

        for i,id in enumerate(all_basics_insights['Id'].to_list()):

            if all_basics_insights['UTC_da_postagem'][i] + relativedelta(hours=168) > (tempo_agora): #intervalo de 7 dias entre as midias

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
                data = requests.get(url, endpointParams )
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

        # Loop Over Insights to Fill Container
        cont = 0
        for i,insight in enumerate(media_insight):
            cont = cont + 1
            numero.append(i)
            engagement_list.append(insight[0]['values'][0]['value'])
            impressions_list.append(insight[1]['values'][0]['value'])
            reach_list.append(insight[2]['values'][0]['value'])
            saved_list.append(insight[3]['values'][0]['value'])
            try:
                video_views.append(insight[4]['values'][0]['value'])
            except:
                video_views.append(0)

            date_time.append(self.data_hoje)
        
        
        # Create DataFrame
        df_media_insight = pd.DataFrame(list(zip(numero,date_time,fotos_ids, engagement_list, impressions_list, reach_list, saved_list,video_views)), columns =['Numero','Data_de_extracao','Id' , 'Engajamento', 'Impressoes', 'Alcance', 'Salvos','Visualizacoes_dos_videos'])

        self.all_insights = all_basics_insights.merge(df_media_insight)

        self.all_insights = self.all_insights[['Id','Data_de_extracao','UTC_da_postagem','Likes','Comentarios',"Engajamento",'Impressoes','Alcance','Salvos','Visualizacoes_dos_videos','Tipo_da_midia','Username','Link','Legenda','Local_da_midia']]

        print(self.all_insights)
        


teste = ComandosDynamo()
iniciar = Informacoes_midia()
iniciar.pegar_informacoes_midia()

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('informacoes_midias_instagram')


class UtilizandoDynamo:

    def adiciona_midias_na_base():
        for i,postagem in enumerate(iniciar.all_insights['UTC_da_postagem']):
            teste.comando_adicionar_midias_ao_dynamo(i,iniciar)


    def confere_e_adiciona_midias_na_base(self):

        for i,id_foto in enumerate(iniciar.all_insights['Id']): #ids pegos pelo request na api  
            response = table.scan(FilterExpression= Attr('Id').eq(int(id_foto)))

            if len(response['Items']) == 0:#se id não estiver na base
                print('Id',id_foto,' não estava na base, foi adicionado')
                teste.comando_adicionar_midias_ao_dynamo(i,iniciar)      
            else:
                print('Id',id_foto,'Já estava na base, PARA DE ME ENCHER')
        


    def adicionando_com_repeticao_por_tempo(self):

        tempo_agora = (datetime.datetime.utcnow()).astimezone(tz=None)  #tranformando na mesma formatação do pandas

        for i, id_foto in enumerate(iniciar.all_insights['Id']): #COMPARAR OS IDS COM OS IDS DA BASE
            if tempo_agora < (iniciar.all_insights['UTC_da_postagem'][i] + relativedelta(hours=168)): #o tempo de agora for maior que o tempo da publicação + 15 minutos então
                print("Postou a foto de Id ",id_foto," menos de 7 dias, foi adicionada")
                teste.comando_adicionar_midias_ao_dynamo(i,iniciar)


        

# usuario_sql = os.getenv('usuario_sql')
# senha_sql = os.getenv('senha_sql')

# aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
# aws.iniciar_conexao()


# a = pd.read_sql('''SELECT * from edu_db.informacoes_midias_instagram''', con = aws.engine)
# print(a) 

# iniciar.all_insights.to_sql('informacoes_midias_instagram', aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')
