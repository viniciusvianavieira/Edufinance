from informacoes_storie_instagram  import UtilizandoDynamoStories

def informacoesstorie(event, context):

    iniciar = UtilizandoDynamoStories()
    iniciar.confere_e_adiciona_stories_na_base()
    iniciar.adicionando_com_repeticao_por_tempo()


    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}