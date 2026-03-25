import requests
import json
import os

from dotenv import load_dotenv 

load_dotenv(dotenv_path="./.env")

API_KEY=os.getenv("API_KEY")
CHANNEL_HANDEL="MrBeast"
maxReuslts=50

def get_playlistid():
    try:
        url=f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDEL}&key={API_KEY}"

        response=requests.get(url)

        response.raise_for_status()

        data=response.json()

        # print(json.dumps(data,indent=4)) 
        #data.items[0].contentDetails.relatedPlaylists.uploads*/
        channel_item= data["items"][0]
        channel_playlistid=channel_item["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_playlistid)
        return channel_playlistid
    except requests.exceptions.RequestException as e:
        raise e
playlistId=get_playlistid()

def get_video_ids(playlistid):
    video_ids=[]
    pageToken=None
    base_url=f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxReuslts}&playlistId={playlistId}&key={API_KEY}"
    try:
        while True:
            url=base_url
            if pageToken:
                url +=f"&pageToken={pageToken}"
            
            response=requests.get(url)
            response.raise_for_status()
            data=response.json()

            for item in data.get('items',[]):
                video_id=item['contentDetails']['videoId']
                video_ids.append(video_id)
            
            pageToken=data.get('nextPageToken')

            if not pageToken:
                break

        return video_ids
    except requests.exceptions.RequestException as e:
        raise e
if __name__=="__main__":
     playlistId=get_playlistid()
     get_video_ids(playlistId)