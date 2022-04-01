

# Import Libraries
import requests
import json
import datetime
import pandas as pd

# Define Parameters Dictionary
params = dict()
params['access_token'] = 'EAAbdojcuBagBABml0d87C2v5HzgENjMpmtFkzCyu6eoH83YkOfoUnT2BDCovOydpimWRcRA1eLZARkfbbGm903xbJZCJsEbBc9pOHjdDcAgAr6ZB918jV1zihFgZCHimmTF6csfgZCF74OBofNbdNpkWUPpmXFHPparqf4EyQ9o9W6gsjsAXSEYxdYk6tI2PtbgLZAyArryvt1tEZCxY7Xm'        # TOKEN DE ACESSO
params['client_id'] = '4895545683873070'                  # not an actual client id
params['client_secret'] = '44918cdd45355b5c3499bff5a6f174d2'     # not an actual client secret
params['graph_domain'] = 'https://graph.facebook.com'
params['graph_version'] = 'v12.0'
params['endpoint_base'] = params['graph_domain'] + '/' + params['graph_version'] + '/'
params['page_id'] = '527015930816521'                  # not an actual page id
params['instagram_account_id'] = '17841406234741156'        # not an actual instagram business account id
params['ig_username'] = 'edufinance'

endpointParams = dict()
endpointParams['input_token'] = params['access_token']
endpointParams['access_token'] = params['access_token']

# Define URL
url = params['graph_domain'] + '/debug_token'


# Requests Data
data = requests.get(url, endpointParams)
access_token_data = json.loads(data.content)
access_token_data

print("Token Expires: ", datetime.datetime.fromtimestamp(access_token_data['data']['expires_at']))

# Define URL
url = params['endpoint_base'] + 'oauth/access_token'

# Define Endpoint Parameters
endpointParams = dict() 
endpointParams['grant_type'] = 'fb_exchange_token'
endpointParams['client_id'] = params['client_id']
endpointParams['client_secret'] = params['client_secret']
endpointParams['fb_exchange_token'] = params['access_token']
print(endpointParams)

# Requests Data
data = requests.get(url, endpointParams )
long_lived_token = json.loads(data.content)
#long_lived_token


# Define URL
url = params['endpoint_base'] + params['instagram_account_id'] + '/media'

# Define Endpoint Parameters
endpointParams = dict()
endpointParams['fields'] = 'id,caption,media_type,media_url,permalink,thumbnail_url,timestamp,username,like_count,comments_count'
endpointParams['access_token'] = params['access_token']
print(params)


# Requests Data
data = requests.get(url, endpointParams )
basic_insight = json.loads(data.content)
#basic_insight


df = pd.DataFrame(basic_insight['data'])
df.columns = ['id', 'Caption', 'Media_Type', 'Media_URL', 'Permalink', 'Timestamp', 'Username', 'Likes', 'Comments','Nothing']
df

media_insight = []

# Loop Over 'Media ID'
for i in basic_insight['data']:
    params['latest_media_id'] = i['id']
   
    # Define URL
    url = params['endpoint_base'] + params['latest_media_id'] + '/insights'

    # Define Endpoint Parameters
    endpointParams = dict() 
    endpointParams['metric'] = 'engagement,impressions,reach,saved'
    endpointParams['access_token'] = params['access_token'] 
    
    # Requests Data
    data = requests.get(url, endpointParams )
    json_data_temp = json.loads(data.content)
  
    try:
        media_insight.append(list(json_data_temp['data']))

    except:
        pass
    

# Initialize Empty Container
engagement_list = []
impressions_list = []
reach_list = []
saved_list = []

# Loop Over Insights to Fill Container
for insight in media_insight:
    engagement_list.append(insight[0]['values'][0]['value'])
    impressions_list.append(insight[1]['values'][0]['value'])
    reach_list.append(insight[2]['values'][0]['value'])
    saved_list.append(insight[3]['values'][0]['value'])
    
# Create DataFrame
df_media_insight = pd.DataFrame(list(zip(engagement_list, impressions_list, reach_list, saved_list)), columns =['Engagement', 'Impressions', 'Reach', 'Saved'])
df_media_insight.head(24)


-----------------------------------------------------------


# Define URL
url = params['endpoint_base'] + params['instagram_account_id'] + '/media'


# Define Endpoint Parameters
endpointParams = dict()
endpointParams['fields'] = 'id,caption,media_product_type,media_type,permalink,timestamp,username,like_count,comments_count'
endpointParams['access_token'] = params['access_token']

# Requests Data
data = requests.get(url, endpointParams )
basic_insight = json.loads(data.content)
print(basic_insight['data'])


fotos_caption = []
fotos_product_type = []
fotos_type = []
fotos_permalink = []
fotos_timestamp = []
fotos_username = []
fotos_like_count = []
fotos_comments_count = []

fotos_ids = []
for i in range(0,len(basic_insight['data'])):
    fotos_ids.append(basic_insight['data'][i]['id'])
    # fotos_caption.append(basic_insight['data'][i]['caption'])
    # fotos_product_type.append(basic_insight['data'][i]['media_product_type'])
    # fotos_type.append(basic_insight['data'][i]['media_type'])
    # fotos_permalink.append(basic_insight['data'][i]['permalink'])
    # fotos_timestamp.append(basic_insight['data'][i]['timestamp'])
    # fotos_username.append(basic_insight['data'][i]['username'])
    # fotos_like_count.append(basic_insight['data'][i]['like_count'])
    # fotos_comments_count.append(basic_insight['data'][i]['comments_count'])


Existe_proxima_pagina = True
while Existe_proxima_pagina:
    try:
        url_next = basic_insight['paging']['next']
        data_next = requests.get(url_next)
        basic_insight = json.loads(data_next.content)
        for i in range(0,len(basic_insight['data'])):
            fotos_ids.append(basic_insight['data'][i]['id'])
            # try:
            #     fotos_caption.append(basic_insight['data'][i]['caption'])
            # except:
            #     fotos_caption.append('ERROR404')
            # fotos_product_type.append(basic_insight['data'][i]['media_product_type'])
            # fotos_type.append(basic_insight['data'][i]['media_type'])
            # fotos_permalink.append(basic_insight['data'][i]['permalink'])
            # fotos_timestamp.append(basic_insight['data'][i]['timestamp'])
            # fotos_username.append(basic_insight['data'][i]['username'])
            # fotos_like_count.append(basic_insight['data'][i]['like_count'])
            # fotos_comments_count.append(basic_insight['data'][i]['comments_count'])
            
            
    except Exception as e:
        Existe_proxima_pagina = False
        break 


----------------------------------------------------


# Define URL
url = params['endpoint_base'] + params['instagram_account_id'] + '/media'


# Define Endpoint Parameters
endpointParams = dict()
endpointParams['fields'] = 'id,caption,media_product_type,media_type,permalink,timestamp,username,like_count,comments_count'
endpointParams['access_token'] = params['access_token']

# Requests Data
data = requests.get(url, endpointParams )
basic_insight = json.loads(data.content)
all_basics_insights = pd.DataFrame(basic_insight['data'])



Existe_proxima_pagina = True
while Existe_proxima_pagina:
    try:
        url_next = basic_insight['paging']['next']
        data_next = requests.get(url_next)
        basic_insight = json.loads(data_next.content)
        
        
    except Exception as e:
        Existe_proxima_pagina = False
        break
    dataframeaux = pd.DataFrame(basic_insight['data'])
    
dataframeaux.append(all_basics_insights)
