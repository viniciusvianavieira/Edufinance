# Import Libraries
import requests
import json
import datetime
import pandas as pd

# Define Parameters Dictionary
params = dict()
params['access_token'] = 'EAAr1Pr9o9QcBAPzITBpXfoXdxEyyHQZBVBtA7ZAZAnhjZAbJPevJuZBvEeZAHGF3sUoN0LpiDx1Q8RGueuRcXfV56ZATPnkoI4KwnUtFMeoFK8TNTOZAWbnGNwWfQHBVTQX0aZAcvRfFcESbrtJnq0proOaVmsYRr4ZCWU2JyDOq2w1CoqUZAEKZCYngTzMDmk9bc70ZD'        # TOKEN DE ACESSO
params['client_id'] = '3084399615210759'                  # not an actual client id
params['client_secret'] = '199bcd728b98322cec2dd66cfc9ffa5a'     # not an actual client secret
params['graph_domain'] = 'https://graph.facebook.com'
params['graph_version'] = 'v12.0'
params['endpoint_base'] = params['graph_domain'] + '/' + params['graph_version'] + '/'
params['page_id'] = '106152182044947'                  # not an actual page id
params['instagram_account_id'] = '17841401411209088'        # not an actual instagram business account id
params['ig_username'] = 'viniciusvianavieira'

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