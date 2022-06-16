#https://binaryguy.tech/aws/s3/quickest-ways-to-list-files-in-s3-bucket/
import os
import boto3
import requests


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

    Link = []
    Id = []
    Id_s3 = []


    for i,item in enumerate(data):
        
        try:
            Link.append(item['Metricas']['Informacoes']['URL'])
            Id.append(item['Id'])
        except:
            pass

    s3 = boto3.client("s3")
    bucket_name = "instagram-api-varos-fotos"
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = response.get("Contents")

    for file in files:
        Id_s3.append(str(file['Key'])[:17])

    session = boto3.Session()
    s3 = session.resource('s3')


    for i,ids in enumerate(Id):
        if str(ids) not in Id_s3:
            try:
                r = requests.get(Link[i], stream=True)
                key = str(ids) + '.jpg' 
                bucket = s3.Bucket(bucket_name)
                bucket.upload_fileobj(r.raw, key)
            except:
                pass




    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}


