from google_auth_oauthlib import flow
from googleapiclient import discovery
import os
import pandas as pd

class API():
    def __init__(self, key):
        api_service_name = "youtube"
        api_version = "v3"
        
        youtube = discovery.build(api_service_name, api_version, developerKey=key)
        self.service = youtube
    
    # Return video stats
    def return_videos_stats(self, videos):
        request = self.service.videos().list(
            part="snippet, statistics",
            id=videos
        )
        response = request.execute()
        return response
        

# Returns the a dict object defining the stats of the two videos: 
def get_videos_stats():
    video_ids = "BpEdIm-IBCk,a-jhSWSjoMA" # Pre-defined, hardcoded videos IDs for this project
    api_key = os.getenv("YOUTUBE_API_KEY")
    
    api = API(api_key) # Instantiate the API class
    response = api.return_videos_stats(video_ids)
    items = response.get("items", [])
    
    data = {'id': [], 'stats': [], 'title': [], 'channel': []}
    
    for item in items:
        id_column = data.get("id")
        id_column.append(item.get("id"))
        
        stats_column = data.get("stats")
        stats_column.append(item.get("statistics"))
        
        snippet = item.get("snippet")
        
        title_column = data.get("title")
        title_column.append(snippet.get("title"))
        
        channel_column = data.get("channel")
        channel_column.append(snippet.get("channelTitle"))
        
    data_df = pd.DataFrame(data=data)    
    
    return data_df

# Transform returned data
def transform_df():
    data_df = get_videos_stats()
    
    transformed_data = {
        'id': data_df['id'],
        'title': data_df['title'],
        'channel': data_df['channel'],
        'views': data_df['stats'].apply(lambda x: int(x.get("viewCount", 0))),
        'comments': data_df['stats'].apply(lambda x: int(x.get("commentCount", 0))),
        'likes': data_df['stats'].apply(lambda x: int(x.get("likeCount", 0))),
    }
    
    transformed_df = pd.DataFrame(data=transformed_data)
    
    return transformed_df
    