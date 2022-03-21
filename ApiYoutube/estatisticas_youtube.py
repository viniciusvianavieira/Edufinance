from os import PRIO_USER
import statistics
from traceback import print_tb
import requests
import json

from urllib3 import Retry

class estatisticasYoutube:

    def __init__(self,key_api, id_canal):
        self.key_api = key_api
        self.id_canal = id_canal
        self.estatisticas_canal = None
        self.estatisticas_video = None
        

    def pegar_estatisticas_youtube(self):
        url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={self.id_canal}&key={self.key_api}"#O f''(fstring) foi utilizado para incluir variaveis {self.id_canal} no meio da string

        json_url = requests.get(url) #pega o formato json do url que estava na web
        dados = json.loads(json_url.text) #pega o formato json do url e transforma em texto no foramto de PYTHON

        try: #VAI TENTAR RODAR O CÓDIGO SE FALHAR VAI PRO EXCEPT
            dados = dados["items"][0]['statistics'] #filtrando para pegar somente a estatistica. O [0] É A POSIÇÃO DO ITEMS(PRIMEIRO)

        except: #RODA O CÓDIGO CASO A PRIMEIRA PARTE ESTEJA ERRADA
            dados = None

        self.estatisticas_canal = dados #armazazenando as estatisticas do canal

        return dados

    def pegar_estatisticas_video_canal(self):

        videos_canal = self.pegar_videos_canal(limit=50) #só podemos pegar até 50 videos, limite do youtube

        



    def pegar_videos_canal(self, limit=None):

        url = f'//www.googleapis.com/youtube/v3/search?key={self.key_api}&channelId={self.id_canal}&part=id&order=date' #só retorna os 5 ultimos videos por isso...

        if limit is not None and isinstance(limit, int):
            url += '&maxResults=' + str(limit) #... precisamos extender esse limite
        
        print(url)
        
    
    

    def dump(self):
        if self.estatisticas_canal is None:
            return
        
        
        nome_canal = "Brenno Sullivan" #pegar do youtube dados
        nome_canal = nome_canal.replace(" ","_").lower() #substituir os espaços por '_' e deixar tudo minusculo
        nome_arquivo = nome_canal + '.json' #criar o nome do arquivo no vscode formato json

        with open(nome_arquivo,'w') as f: #abriindo o arquivo no "write mode" como fstring (formatação de texto)
            json.dump(self.estatisticas_canal,f, indent=4) # jogando a estatisticas do canal no arquivo .json, formatando ela por f-string, e organizando ela mais "bonito"

        print('Arquivo descarregado')

    
     