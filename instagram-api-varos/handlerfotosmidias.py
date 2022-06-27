#https://binaryguy.tech/aws/s3/quickest-ways-to-list-files-in-s3-bucket/
import os
from re import I
import boto3
import requests
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import type_coerce
import pandas as pd



def handleruploadfotosmidias(event, context):

    os.system('cls' if os.name == 'nt' else 'clear')
    print()


    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('informacoes_midias_instagram')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    Thumbnail = []
    Link = []
    Id = []
    Id_s3 = []
    Tipo = []


    for i,item in enumerate(data):
        
        try:
            Link.append(item['Metricas']['Informacoes']['URL'])
            Id.append(item['Id'])
            Tipo.append(item['Metricas']['Informacoes']['Tipo_da_midia'])
        except:
            pass

    df_aux = pd.DataFrame(list(zip(Id,Link,Tipo,Thumbnail)),columns =['Id' ,'Link','Tipo','Thumbnail'])
    df_aux = df_aux.drop_duplicates(subset='Id', keep='last', ignore_index=True)

    Id = df_aux['Id'].to_list()
    Link = df_aux['Link'].to_list()
    Tipo = df_aux['Tipo'].to_list()
    Thumbnail = df_aux['Thumbnail'].to_list()



    session = boto3.Session()
    s3 = session.resource('s3', region_name='us-east-1')
    s3 = boto3.client("s3", region_name='us-east-1')
    bucket_name = "instagram-api-varos-fotos"
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = response.get("Contents")

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


