import boto3
from botocore.exceptions import ClientError

class ComandosDynamo:

        def comando_adicionar_midias_ao_dynamo(self,i,iniciar):

                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_midias_instagram')

                table.put_item(
                                Item={
                                        'Data_de_extracao' : str(iniciar.data_hoje),
                                        'Id': int(iniciar.all_insights['Id'][i]),
                                        'Metricas':{
                                                'Dados':{
                                                        'Likes': str(iniciar.all_insights['Likes'][i]),
                                                        'Comentarios': str(iniciar.all_insights['Comentarios'][i]),
                                                        'Engajamento': str(iniciar.all_insights['Engajamento'][i]),
                                                        'Impressoes': str(iniciar.all_insights['Impressoes'][i]),
                                                        'Alcance': str(iniciar.all_insights['Alcance'][i]),
                                                        'Salvos':str(iniciar.all_insights['Salvos'][i]),
                                                        'Visualizacoes_dos_videos': str(iniciar.all_insights['Visualizacoes_dos_videos'][i]),
                                                        },
                                                'Informacoes':{
                                                        'UTC_da_postagem': str(iniciar.all_insights['UTC_da_postagem'][i]),
                                                        'Tipo_da_midia': str(iniciar.all_insights['Tipo_da_midia'][i]),
                                                        'Username': str(iniciar.all_insights['Username'][i]),
                                                        'Link':str(iniciar.all_insights['Link'][i]),
                                                        'Legenda': str(iniciar.all_insights['Legenda'][i]),
                                                        'Local_da_midia': str(iniciar.all_insights['Local_da_midia'][i]),
                                                        'URL': str(iniciar.all_insights['URL'][i]),
                                                        'Thumbnail': str(iniciar.all_insights['Thumbnail'][i]),
                                                                },
                                                        }
                                        
                                        }
                                )
        
        def comando_adicionar_stories_ao_dynamo(self,i,iniciar):

                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_stories_instagram')

                table.put_item(
                                Item={
                                        'Data_de_extracao' : str(iniciar.all_stories_informations['Data_de_extracao'][i]),
                                        'Id': int(iniciar.all_stories_informations['Id'][i]),
                                        'Metricas':{
                                                'Dados':{
                                                        'Likes': str(iniciar.all_stories_informations['Likes'][i]),
                                                        'Impressoes': str(iniciar.all_stories_informations['Impressoes'][i]),
                                                        'Alcance': str(iniciar.all_stories_informations['Alcance'][i]),
                                                        'Respostas': str(iniciar.all_stories_informations['Respostas'][i]),
                                                        'Saidas': str(iniciar.all_stories_informations['Saidas'][i]),
                                                        'Toques_para_avancar':str(iniciar.all_stories_informations['Toques_para_avancar'][i]),
                                                        'Toques_para_voltar': str(iniciar.all_stories_informations['Toques_para_voltar'][i]),
                                                        },
                                                'Informacoes':{
                                                        'UTC_da_postagem': str(iniciar.all_stories_informations['UTC_da_postagem'][i]),
                                                        'Tipo_da_midia': str(iniciar.all_stories_informations['Tipo_da_midia'][i]),
                                                        'Username': str(iniciar.all_stories_informations['Username'][i]),
                                                        'Link':str(iniciar.all_stories_informations['Link'][i]),
                                                        'Legenda': str(iniciar.all_stories_informations['Legenda'][i]),
                                                        'Local_da_midia': str(iniciar.all_stories_informations['Local_da_midia'][i]),
                                                        'Media_url': str(iniciar.all_stories_informations['Media_url'][i]),
                                                        'Thumbnail': str(iniciar.all_stories_informations['Thumbnail'][i]),
                                                        'AWS_URL': str(iniciar.all_stories_informations['AWS_URL'][i]),                                                     
                                                                },
                                                        }
                                        
                                        }
                                )

        def pegar_dados_na_base(self,Data_de_extracao, Id, dynamodb=None):

                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_midias_instagram')

                try:
                        response = table.get_item(Key={'Data_de_extracao': Data_de_extracao, 'Id': Id})

                except ClientError as e:
                        print(e.response['Error']['Message'])
                else:
                        return response['Item']

        def comando_upload_backup_ao_dynamo(self,i,backup):

                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_conta_instagram')

                table.update_item(
                        Key={
                        'Data_do_dia': str(backup.df_account_metrics['UTC_do_dia'][i]),
                         },
                        UpdateExpression='SET Dados = :valor',
                        ExpressionAttributeValues={
                        ':valor': {
                                "Alcance": str(backup.df_account_metrics['Alcance'][i]),
                                "Impressoes": str(backup.df_account_metrics['Impressoes'][i]),
                                "Numero_de_seguidores": str(backup.df_account_metrics['Numero_de_seguidores'][i]),
                                "Visualizacoes_do_perfil": str(backup.df_account_metrics['Visualizacoes_do_perfil'][i]),
                                "Cliques_no_site": str(backup.df_account_metrics['Cliques_no_site'][i]),

                                }
                        }
                )               

        def comando_upload_iniciais_ao_dynamo(self,iniciais):

                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_conta_instagram')

                table.update_item(
                        Key={
                        'Data_do_dia': str(iniciais.data_do_dia),
                         },
                        UpdateExpression='SET Metricas = :valor',
                        ExpressionAttributeValues={
                        ':valor': {
                                "Seguidores": str(iniciais.df_metrics_accounts_generics['Seguidores'][0]),
                                "Seguindo:": str(iniciais.df_metrics_accounts_generics['Seguindo'][0]),
                                "Midias": str(iniciais.df_metrics_accounts_generics['Midias'][0]),

                                }
                        }
                
                )




                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('informacoes_conta_instagram')

                table.update_item(
                        Key={
                                'Data_de_extracao' : str(iniciar.data_hoje),
                                'Id': int(iniciar.all_insights['Id'][i]),
                         },
                        UpdateExpression='SET Metricas = :valor',
                        ExpressionAttributeValues={
                        ':valor': {
                                'Dados':{
                                        'Likes': str(iniciar.all_insights['Likes'][i]),
                                        'Comentarios': str(iniciar.all_insights['Comentarios'][i]),
                                        'Engajamento': str(iniciar.all_insights['Engajamento'][i]),
                                        'Impressoes': str(iniciar.all_insights['Impressoes'][i]),
                                        'Alcance': str(iniciar.all_insights['Alcance'][i]),
                                        'Salvos':str(iniciar.all_insights['Salvos'][i]),
                                        'Visualizacoes_dos_videos': str(iniciar.all_insights['Visualizacoes_dos_videos'][i]),
                                        },
                                'Informacoes':{
                                        'UTC_da_postagem': str(iniciar.all_insights['UTC_da_postagem'][i]),
                                        'Tipo_da_midia': str(iniciar.all_insights['Tipo_da_midia'][i]),
                                        'Username': str(iniciar.all_insights['Username'][i]),
                                        'Link':str(iniciar.all_insights['Link'][i]),
                                        'Legenda': str(iniciar.all_insights['Legenda'][i]),
                                        'Local_da_midia': str(iniciar.all_insights['Local_da_midia'][i]),
                                                },

                                }
                        }
                
                )



                                











