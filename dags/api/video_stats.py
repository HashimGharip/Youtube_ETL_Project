import requests
import json

from datetime import date

#import os
#from dotenv import load_dotenv 
#load_dotenv(dotenv_path="./.env")

from airflow.decorators import task
from airflow.models import Variable

#API_KEY=os.getenv("API_KEY")
#CHANNEL_HANDLE="MrBeast"
API_KEY=Variable.get("API_KEY")
CHANNEL_HANDLE=Variable.get("CHANNEL_HANDLE")
maxReuslts=50

# A Python decorator is a way to modify or extend the behavior of a function (or class) without changing its actual code. example @task
@task
def get_playlistid():
    try:
        url=f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

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

@task
def get_video_ids(playlistId):
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

@task
def extract_video_data(video_ids):

    extracted_data=[]

    def batch_list(video_id_lst,batch_size):
        for video_id in range(0,len(video_id_lst),batch_size):
            yield video_id_lst[video_id: video_id + batch_size]
    

    try:
        for batch in batch_list(video_ids,maxReuslts):
            video_ids_str=",".join(batch)
            url=f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

                        
            response=requests.get(url)
            response.raise_for_status()
            data=response.json()

            for item in data.get("items",[]):
                video_id=item['id']
                snippet=item['snippet']
                contentDetails=item['contentDetails']
                statistics=item['statistics']
            
                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }
                extracted_data.append(video_data)
        return extracted_data
    
    except requests.exceptions.RequestException as e:
        raise e
@task  
def save_to_json(extracted_data):
    file_path=f"./data/YT_data_{date.today()}.json"

    with open(file_path,'w',encoding="utf-8") as json_outfile:
        json.dump(extracted_data,json_outfile,indent=4,ensure_ascii=False)


if __name__=="__main__":
     playlistId=get_playlistid()
     video_ids=get_video_ids(playlistId)
     video_date=extract_video_data(video_ids)
     save_to_json(video_date)