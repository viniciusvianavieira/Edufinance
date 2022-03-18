from os import PRIO_USER
import statistics
import requests
import json

from urllib3 import Retry

class estatisticasYoutube:

    def __init__(self,key_api, id_canal):
        self.key_api = key_api
        self.id_canal = id_canal
        self.estatisticas_canal = None

    def pegar_estatisticas_youtube(self):
        url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={self.id_canal}&key={self.key_api}'#O f''(fstring) foi utilizado para incluir variaveis {self.id_canal} no meio da string

        json_url = requests.get(url) #pega o formato json do url que estava na web
        data = json.loads(json_url.text) #pega o formato json do url e transforma em texto

        try:
            data = data["items"][0]['statistics'] #filtrando para pegar somente a estatistica

        except:
            data = None
        
        return data



print("ola")
