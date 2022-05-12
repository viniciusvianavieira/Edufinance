# Import Libraries
import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests
import json
import datetime
import pandas as pd
import datetime
import os
from dateutil.relativedelta import relativedelta
from accessToken_e_endpoints import Parametros
from dynamo_comandos import MidiasComandosDynamo
import pytz
utc=pytz.UTC

os.system('cls' if os.name == 'nt' else 'clear')
print()




class Informacoes_Stories:

    def pegando_informacoes_storie(self):

        parametros = Parametros()

        url = parametros.params['endpoint_base'] + parametros.params['instagram_account_id'] + '/stories'

        # Define Endpoint Parameters
        endpointParams = dict()
        endpointParams['access_token'] = parametros.params['access_token']

        # Requests Data
        data = requests.get(url, endpointParams )
        json_stories_id = json.loads(data.content)
        json_stories_id = pd.DataFrame(json_stories_id['data'])

        names_insights_stories = []
        values_insights_stories = []
        ids_insights_stories = []

        try:
            for id in json_stories_id['id'].to_list():
                url = parametros.params['endpoint_base'] + id + '/insights'

                # Define Endpoint Parameters
                endpointParams = dict()
                endpointParams['metric'] = 'impressions, reach, taps_forward, taps_back, exits, replies'
                endpointParams['access_token'] = parametros.params['access_token']

                # Requests Data
                data = requests.get(url, endpointParams)
                json_stories_insights = json.loads(data.content)
                for insights in json_stories_insights['data']:
                    
                    names_insights_stories.append( insights['name'])
                    values_insights_stories.append( insights['values'][0]['value'])
                    valor_id_tratado = insights['id']
                    ids_insights_stories.append(valor_id_tratado[0:17])

                

                df_insights_stories = pd.DataFrame(list(zip(ids_insights_stories,names_insights_stories,values_insights_stories)), columns =['Id','Nome','Valor'])


            df_insights_stories = df_insights_stories.pivot(index='Id',columns='Nome',values='Valor') 

            df_insights_stories['Data_de_extracao'] = [datetime.datetime.now()] * len(df_insights_stories)

            df_insights_stories.columns = ['Saidas', 'Impressoes', 'Alcance', 'Respostas', 'Toques_para_voltar', 'Toques_para_avancar', 'Data_de_extracao']

            df_insights_stories = df_insights_stories[['Data_de_extracao','Impressoes','Alcance','Respostas','Saidas','Toques_para_voltar','Toques_para_avancar']]

            id_storie_fields = []
            like_count_storie_fields = []
            media_product_type_fields = []
            media_type_fields = []
            media_url_fields = []
            permalink_fields = []
            shortcode_fields =[]
            timestamp_fields = []
            username_fields = []
            caption_fields = []

            for id in json_stories_id['id'].to_list():
                url = parametros.params['endpoint_base'] + id   

                # Define Endpoint Parameters
                endpointParams = dict()
                endpointParams['fields'] = 'caption,id,like_count,media_product_type,media_type,media_url,owner,permalink,shortcode,thumbnail_url,timestamp,username'
                endpointParams['access_token'] = parametros.params['access_token']

                # Requests Data
                data = requests.get(url, endpointParams)
                json_stories_fields = json.loads(data.content)
            
                id_storie_fields.append(id)
                like_count_storie_fields.append(json_stories_fields['like_count'])
                media_product_type_fields.append(json_stories_fields['media_product_type'])
                media_type_fields.append(json_stories_fields['media_type'])
                media_url_fields.append(json_stories_fields['media_url'])
                permalink_fields.append(json_stories_fields['permalink'])
                shortcode_fields.append(json_stories_fields['shortcode'])
                timestamp_fields.append(json_stories_fields['timestamp'])
                username_fields.append(json_stories_fields['username'])
                try:
                    caption_fields.append(json_stories_fields['caption'])
                except:
                    caption_fields.append('------')
                
                df_fields_stories = pd.DataFrame(list(zip(id_storie_fields, like_count_storie_fields, media_product_type_fields, media_type_fields, media_url_fields, permalink_fields,timestamp_fields, username_fields, caption_fields)), columns =['Id','Likes','Local_da_midia','Tipo_da_midia', 'Thumbnail_url','Link','UTC_da_postagem','Username', 'Legenda'])
        
            self.all_stories_informations = pd.merge(df_fields_stories,  df_insights_stories, how='left', on = 'Id')

            self.all_stories_informations = self.all_stories_informations[['Id','UTC_da_postagem','Data_de_extracao','Likes','Impressoes','Alcance','Respostas','Saidas','Toques_para_voltar','Toques_para_avancar','Tipo_da_midia','Username','Link', 'Thumbnail_url','Local_da_midia','Legenda']]


            self.all_stories_informations['Id'] = self.all_stories_informations['Id'].astype(int)

            self.all_stories_informations['UTC_da_postagem'] = pd.to_datetime(self.all_stories_informations['UTC_da_postagem'], format='%Y-%m-%d')

            self.all_stories_informations = self.all_stories_informations.fillna('------')

            print(self.all_stories_informations)

        except:
            print("Nenhum storie encontrado")


teste = MidiasComandosDynamo()
iniciar = Informacoes_Stories()
iniciar.pegando_informacoes_storie()

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('informacoes_stories_instagram')


class UtilizandoDynamoStories:

    def adiciona_stories_na_base():
        for i,postagem in enumerate(iniciar.all_stories_informations['UTC_da_postagem']):
            teste.comando_adicionar_stories_ao_dynamo(i,iniciar)


    def confere_e_adiciona_stories_na_base():

        for i,id_foto in enumerate(iniciar.all_stories_informations['Id']): #ids pegos pelo request na api  
            response = table.scan(FilterExpression= Attr('Id').eq(int(id_foto)))

            if len(response['Items']) == 0:#se id não estiver na base
                print('Id',id_foto,' não estava na base, foi adicionado')
                teste.comando_adicionar_stories_ao_dynamo(i,iniciar)      
            else:
                print('Id',id_foto,'Já estava na base, PARA DE ME ENCHER')
        


    def adicionando_com_repeticao_por_tempo():

        tempo_agora = (datetime.datetime.utcnow()).astimezone(tz=None)  #tranformando na mesma formatação do pandas

        for i, id_foto in enumerate(iniciar.all_stories_informations['Id']): #COMPARAR OS IDS COM OS IDS DA BASE
            if tempo_agora < (iniciar.all_stories_informations['UTC_da_postagem'][i] + relativedelta(hours=24)): #o tempo de agora for maior que o tempo da publicação + 15 minutos então
                print("Postou a foto de Id ",id_foto," menos de 24 horas, foi adicionado")
                teste.comando_adicionar_stories_ao_dynamo(i,iniciar)


# UtilizandoDynamoStories.adiciona_stories_na_base()
UtilizandoDynamoStories.confere_e_adiciona_stories_na_base()
UtilizandoDynamoStories.adicionando_com_repeticao_por_tempo()