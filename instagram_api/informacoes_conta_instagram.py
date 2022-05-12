from turtle import back
from informacoes_iniciais_conta_instagram import informacoes_iniciais_conta_instagram
from informacoes_backup_conta_instagram import informacoes_backup_conta_instagram
from dynamo_comandos import ComandosDynamo
import boto3
import os


os.system('cls' if os.name == 'nt' else 'clear')
print()

dynamo = ComandosDynamo()

backup = informacoes_backup_conta_instagram()
backup.pegando_informacoes_backup()

iniciais = informacoes_iniciais_conta_instagram()
iniciais.pegando_informacoes_iniciais()

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('informacoes_conta_instagram')

table.put_item(
                Item={
                        'Data_do_dia': str(backup.df_account_metrics['UTC_do_dia'][3]),
                        "Dados":{
                                "Alcance": str(backup.df_account_metrics['Alcance'][3]),
                                "Impressoes": str(backup.df_account_metrics['Impressoes'][3]),
                                "Numero_de_seguidores": str(backup.df_account_metrics['Numero_de_seguidores'][3]),
                                "Visualizacoes_do_perfil": str(backup.df_account_metrics['Visualizacoes_do_perfil'][3]),
                                "Cliques_no_site": str(backup.df_account_metrics['Cliques_no_site'][3]),
                                },
                        'Metricas':{
                                "Seguidores": int(iniciais.df_metrics_accounts_generics['Seguidores'][0]),
                                "Seguindo:": int(iniciais.df_metrics_accounts_generics['Seguindo'][0]),
                                "Midias": int(iniciais.df_metrics_accounts_generics['Midias'][0]),
                                        },
                        
                }
        )

for i,data in enumerate(backup.df_account_metrics['UTC_do_dia']):
    
    dynamo.comando_upload_backup_ao_dynamo(i,backup)