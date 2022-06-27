from informacoes_iniciais_conta_instagram import informacoes_iniciais_conta_instagram
from informacoes_backup_conta_instagram import informacoes_backup_conta_instagram
from dynamo_comandos import ComandosDynamo
import boto3
import os


class informacoes_conta_lambda_function:

        def informacoes_conta_lambda_serverless(self):

                os.system('cls' if os.name == 'nt' else 'clear')
                print()

                dynamo = ComandosDynamo()

                backup = informacoes_backup_conta_instagram()
                backup.pegando_informacoes_backup()

                iniciais = informacoes_iniciais_conta_instagram()
                iniciais.pegando_informacoes_iniciais()

                dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
                table = dynamodb.Table('informacoes_conta_instagram')

                table.put_item(
                                Item={
                                        'Data_do_dia': str(backup.df_account_metrics['UTC_do_dia'][5]),
                                        "Dados":{
                                                "Alcance": str(backup.df_account_metrics['Alcance'][5]),
                                                "Impressoes": str(backup.df_account_metrics['Impressoes'][5]),
                                                "Numero_de_seguidores": str(backup.df_account_metrics['Numero_de_seguidores'][5]),
                                                "Visualizacoes_do_perfil": str(backup.df_account_metrics['Visualizacoes_do_perfil'][5]),
                                                "Cliques_no_site": str(backup.df_account_metrics['Cliques_no_site'][5]),
                                                },
                                        'Metricas':{
                                                "Seguidores": str(iniciais.df_metrics_accounts_generics['Seguidores'][0]),
                                                "Seguindo:": str(iniciais.df_metrics_accounts_generics['Seguindo'][0]),
                                                "Midias": str(iniciais.df_metrics_accounts_generics['Midias'][0]),
                                                        },
                                        
                                }
                        )

                for i,data in enumerate(backup.df_account_metrics['UTC_do_dia']):
                
                        dynamo.comando_upload_backup_ao_dynamo(i,backup)

