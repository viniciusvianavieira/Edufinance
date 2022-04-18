#https://towardsdatascience.com/discover-insights-from-your-instagram-business-account-with-facebook-graph-api-and-python-81d20ee2e751
#https://www.powermyanalytics.com/connectors/instagram-insights/fields/

# Import Libraries
import requests
import json
import datetime
import datetime
import os

os.system('cls' if os.name == 'nt' else 'clear')
print()

class Parametros:
    
    def __init__(self):

        self.params = dict()
        self.params['access_token'] = os.getenv('access_token')#Primeiro coloca o access_token do facebook developers, e o programa te redirecionará um mais longo
        self.params['client_id'] = os.getenv('client_id')#facebook_developers --> settings               
        self.params['client_secret'] = os.getenv('client_secret')#facebook_developers --> settings
        self.params['graph_domain'] = 'https://graph.facebook.com'
        self.params['graph_version'] = 'v12.0'
        self.params['endpoint_base'] = self.params['graph_domain'] + '/' + self.params['graph_version'] + '/'
        self.params['page_id'] = os.getenv('page_id') #Id da página do fb vinculada ao instagram
        self.params['instagram_account_id'] = os.getenv('instagram_account_id') #Id do instagram que o Facebook Developers dá ao conectar
        self.params['ig_username'] = 'edufinance' #Usuario

    
    def pegar_validade_access_token(self): #pega a validade do access_token
        
        # Define Endpoint Parameters
        self.endpointparams = dict()
        self.endpointparams['input_token'] = self.params['access_token']
        self.endpointparams['access_token'] = self.params['access_token']

        # Define URL
        url = self.params['graph_domain'] + '/debug_token'

        # Requests Data
        data = requests.get(url, self.endpointparams)
        access_token_data = json.loads(data.content)
        
        #access_token_data
        print("Token Expires: ", datetime.datetime.fromtimestamp(access_token_data['data']['expires_at']))
        print()

    
    def pegar_access_token_longa_duracao(self): #pega o access_token de curta duração(2 horas) e transforma no access_token de longa duração(60 dias)

        # Define URL
        url = self.params['endpoint_base'] + 'oauth/access_token'

        # Define Endpoint Parameters
        self.endpointparams = dict() 
        self.endpointparams['grant_type'] = 'fb_exchange_token'
        self.endpointparams['client_id'] = self.params['client_id']
        self.endpointparams['client_secret'] = self.params['client_secret']
        self.endpointparams['fb_exchange_token'] = self.params['access_token']


        # Requests Data
        data = requests.get(url, self.endpointparams )
        long_lived_token = json.loads(data.content)
        print(long_lived_token['access_token'])
        print()



#2022-06-17 15:30:36

if __name__ == "__main__":

    teste = Parametros()

    teste.pegar_validade_access_token()
    teste.pegar_access_token_longa_duracao()











