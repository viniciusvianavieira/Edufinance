from informacoes_conta_instagram import informacoes_conta_lambda_function



def informacoesconta(event, context):

    iniciar = informacoes_conta_lambda_function()
    iniciar.informacoes_conta_lambda_serverless()


    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}