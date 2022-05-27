from informacoes_midias_instagram import UtilizandoDynamo



def informacoesmidias(event, context):

    iniciar = UtilizandoDynamo()
    iniciar.confere_e_adiciona_midias_na_base()
    iniciar.adicionando_com_repeticao_por_tempo()


    resposta = "Tudo rodou perfeitamente"

    return {"statusCode": 200, "resposta": resposta}