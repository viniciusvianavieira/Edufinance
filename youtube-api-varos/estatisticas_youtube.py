from nturl2path import url2pathname
import re
import statistics
from traceback import print_tb
import requests
import json


from urllib3 import Retry


class estatisticasYoutube:

    def __init__(self, key_api, id_canal):
        self.key_api = key_api
        self.id_canal = id_canal
        self.estatisticas_canal = None
        self.estatisticas_video = None

    def pegar_estatisticas_youtube(self):
        # O f''(fstring) foi utilizado para incluir variaveis {self.id_canal} no meio da string
        url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={self.id_canal}&key={self.key_api}"

        # pega o formato json do url que estava na web
        json_url = requests.get(url)
        # pega o formato json do url e transforma em texto no foramto de PYTHON
        dados = json.loads(json_url.text)

        try:  # VAI TENTAR RODAR O CÓDIGO SE FALHAR VAI PRO EXCEPT
            # filtrando para pegar somente a estatistica. O [0] É A POSIÇÃO DO ITEMS(PRIMEIRO)
            dados = dados["items"][0]['statistics']

        except:  # RODA O CÓDIGO CASO A PRIMEIRA PARTE ESTEJA ERRADA
            dados = None

        self.estatisticas_canal = dados  # armazazenando as estatisticas do canal

        return dados

    def pegar_estatisticas_video_canal(self):

        # só podemos pegar até 50 videos, limite do youtube
        videos_canal = self.pegar_videos_canal(limit=50)
        print(videos_canal)
        print(len(videos_canal))

        # existem 3 abas de dados do video, vamos acessar todas
        partes = ["snippet", "statistics", "contentDetails"]
        for id_videos in tqdm(videos_canal):
            for parte in partes:
                dados = self.estatisticas_unico_video(id_videos, parte)
                videos_canal[id_videos].update(dados)

        self.estatisticas_video = videos_canal
        return videos_canal

    def estatisticas_unico_video(self, id_videos, parte):
        url = f"https://www.googleapis.com/youtube/v3/videos?part={parte}&id={id_videos}&key={self.key_api}"
        json_url = requests.get(url)
        dados = json.loads(json_url.text)
        try:
            dados = dados["items"][0][parte]
        except:
            print("error")
            dados = dict()

        return dados

    def pegar_videos_canal(self, limit=None):

        # só retorna os 5 ultimos videos por isso...
        url = f'https://www.googleapis.com/youtube/v3/search?key={self.key_api}&channelId={self.id_canal}&part=id&order=date'

        # isinstance verifica se o limit é da classe int
        if limit is not None and isinstance(limit, int):
            # ... precisamos extender esse limite
            url += '&maxResults=' + str(limit)

        vid, npt = self.pegar_videos_canal_por_pagina(
            url)  # pegando os videos e o next page token
        idx = 0  # 10páginas
        while(npt is not None and idx < 10):
            # esta formando a url da proxima pagina com onext page token
            nexturl = url + "&pageToken=" + npt
            next_vid, npt = self.pegar_videos_canal_por_pagina(nexturl)
            vid.update(next_vid)  # adicionando no dicionario
            idx += 1

        return vid

    def pegar_videos_canal_por_pagina(self, url):
        json_url = requests.get(url)  # pegando os dados no formato json
        dados = json.loads(json_url.text)  # transforma o arquivo json em texto
        videos_canal = dict()  # cria um dicionario vazio
        if 'items' not in dados:
            return videos_canal, None  # caso o item não esteja na lista em json, retornara nulo

        # filtrando apenas os items do json inteiro
        item_dados = dados['items']
        # vaipegar o token da proxima pagina e caso não encontre, retornara vazio
        proximaPagina = dados.get("nextPageToken", None)

        for item in item_dados:
            try:
                kind = item['id']['kind']  # pegando o kind
                if kind == 'youtube#video':  # pegando só os videos
                    # pegando só os ids dos videos
                    id_videos = item['id']['videoId']
                    videos_canal[id_videos] = dict()

            except KeyError:
                print("error")

        return videos_canal, proximaPagina  # retorna dois valores

    def dump(self):
        if self.estatisticas_canal is None or self.estatisticas_video is None:
            print("data is none")
            return

        fused_dados = {self.id_canal: {
            "channel_statistics": self.estatisticas_canal, "video_data": self.estatisticas_video}}

        nome_canal = self.estatisticas_video.popitem()[1].get(
            'channelTitle', self.id_canal)
        # substituir os espaços por '_' e deixar tudo minusculo
        nome_canal = nome_canal.replace(" ", "_").lower()
        # criar o nome do arquivo no vscode formato json
        nome_arquivo = nome_canal + '.json'

        # abriindo o arquivo no "write mode" como fstring (formatação de texto)
        with open(nome_arquivo, 'w') as f:
            # jogando a estatisticas do canal no arquivo .json, formatando ela por f-string, e organizando ela mais "bonito"
            json.dump(fused_dados, f, indent=4)

        print('Arquivo descarregado')
