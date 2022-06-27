# https://binaryguy.tech/aws/s3/quickest-ways-to-list-files-in-s3-bucket/
from ast import Pass
from errno import EEXIST
import os
import boto3
import requests
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import type_coerce
import pandas as pd



def handleruploadfotosstories(event, context):

    session = boto3.Session()
    s3 = session.resource('s3', region_name='us-east-1')
    s3client = boto3.client("s3", region_name='us-east-1')
    bucket_name = "instagram-api-varos-fotos"
    response = s3client.list_objects_v2(Bucket=bucket_name)
    files = response.get("Contents")
    tempo_agora = str((datetime.now()))
    tempo_agora = tempo_agora[0:19]
    tempo_agora = datetime.strptime(tempo_agora, '%Y-%m-%d %H:%M:%S')

    os.system('cls' if os.name == 'nt' else 'clear')
    print()

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_stories_instagram')
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    Thumbnail = []
    Link = []
    Id = []
    Id_s3 = []
    Data = []
    Tipo = []

    for i, item in enumerate(data):

        try:
            Id.append(str(item['Id']))
        except:
            Id.append('---')

        try:
            data = item['Metricas']['Informacoes']['UTC_da_postagem'][:19]
            data = datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
            Data.append(data)
        except:
            Data.append('---')

        try:
            Link.append(item['Metricas']['Informacoes']['Media_url'])
        except:
            Link.append('---')
        try:
            Tipo.append(item['Metricas']['Informacoes']['Tipo_da_midia'])
        except:       
            Tipo.append('---')
        try:
            Thumbnail.append(item['Metricas']['Informacoes']['Thumbnail'])
        except:
            Thumbnail.append('---')

    df_aux = pd.DataFrame(list(zip(Data,Id,Link,Tipo,Thumbnail)),columns =['Data','Id' ,'Link','Tipo','Thumbnail'])
    df_aux = df_aux.drop_duplicates(subset='Id', keep='last', ignore_index=True)

    Id = df_aux['Id'].to_list()
    Data = df_aux['Data'].to_list()
    Link = df_aux['Link'].to_list()
    Tipo = df_aux['Tipo'].to_list()
    Thumbnail = df_aux['Thumbnail'].to_list()

    for file in files:
        Id_s3.append(str(file['Key'])[:17])


    for n, ids in enumerate(Id):


        if Tipo[n] == 'IMAGE':
            if ids not in Id_s3:
                if Link[n] == '---' or Link[n] == '------':
                    pass
                else:
                    r = requests.get(Link[n], stream=True)
                    key = str(ids) + '.jpg'
                    bucket = s3.Bucket(bucket_name)
                    bucket.upload_fileobj(r.raw, key, ExtraArgs={
                        "ContentType": "image/jpeg", 'ACL': 'public-read'})
                    print('Adicionou', ids, 'Image')



        if Tipo[n] == 'VIDEO':
            if ids not in Id_s3:
                if Link[n] == '---' or Link[n] == '------':
                    pass
                else:
                    r = requests.get(Link[n], stream=True)
                    key = str(ids) + '.mp4'
                    bucket = s3.Bucket(bucket_name)
                    bucket.upload_fileobj(r.raw, key, ExtraArgs={
                        "ContentType": "video/mp4", 'ACL': 'public-read'})
                    print('Adicionou', ids, 'Video')

                print(Thumbnail[n])
                if Thumbnail[n] == '---'or Thumbnail[n] == '------':
                    pass
                else:
                    print(Thumbnail[n])
                    r = requests.get(Thumbnail[n], stream=True)
                    key = str(ids) + '.jpg'
                    bucket = s3.Bucket(bucket_name)
                    bucket.upload_fileobj(r.raw, key, ExtraArgs={
                        "ContentType": "image/jpeg", 'ACL': 'public-read'})
                    print('Adicionou', ids, 'Thumbnail')


    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}
