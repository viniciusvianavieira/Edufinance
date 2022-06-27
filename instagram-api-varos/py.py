from distutils.command.config import config
import logging
import boto3
from botocore.exceptions import ClientError
import os
import datetime
from datetime import date, datetime
from botocore.config import Config

bucket = "instagram-api-varos-fotos"
s3client = boto3.client('s3', region_name='us-east-1', aws_access_key_id= os.getenv('access_key_aws'),
    aws_secret_access_key=os.getenv('secret_key_aws'), config=Config(signature_version = 's3'))

session = boto3.Session()
s3 = session.resource('s3', region_name='us-east-1')
bucket_name = "instagram-api-varos-fotos"
response = s3client.list_objects_v2(Bucket=bucket_name)
files = response.get("Contents")
tempo_agora = str((datetime.now()))
tempo_agora = tempo_agora[0:19]
tempo_agora = datetime.strptime(tempo_agora, '%Y-%m-%d %H:%M:%S')

os.system('cls' if os.name == 'nt' else 'clear')
print()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('informacoes_stories_instagram')
response = table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

Id = []

for i, item in enumerate(data):

    try:
        Id.append(str(item['Id']))
    except:
        Id.append('---')

idfotoapi = '18303088612009350.jpg'
# 18303088612009350
# 17874640958701472
# 17889202427598145
# 17893140926636165
# 17906432444597396
# 17923272098287246
# 17924591300267853
# 17939421314010363
# 17944172402015685
# 17951916880803587
# 17975464153568113
# 18241984294102208
# 18298985896037115
# 17907519239601314
# 17908660763498574
for ide in Id:
    fotos3 = str(ide)+'.jpg'
    if fotos3 == idfotoapi:
        print(fotos3)
        presigned_url = s3client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket, 'Key': fotos3}, ExpiresIn=99999999)
        print(presigned_url)
