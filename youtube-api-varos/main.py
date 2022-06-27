#https://www.youtube.com/watch?v=5qtC-tsQ-wE (Link do video)


from estatisticas_youtube import estatisticasYoutube #importando a class: estatisticasyoutube do outro arquivo python

key_api = 'AIzaSyDRtc_ubbFC_LnJ1vcBuWsXalYMb_HSGCs' #chave obtida a partir do google developers
id_canal = 'UC7zXCDXJssKjkij2N2az39g' #id do canal obtida pelo youtube

youtube = estatisticasYoutube(key_api, id_canal)
 #variavel que é atribuida a class criada, onde retiramos dela dois parametros
youtube.pegar_estatisticas_youtube()
youtube.pegar_estatisticas_video_canal()
youtube.dump()
#youtube.dump()

#youtube.pegar_estatisticas_video_canal()
