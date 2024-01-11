from enum import Enum
from typing import List, Union
import pandas as pd
import logging
import json
from datetime import datetime, timedelta
from social.hooks.youtube.youtube_base import YoutubeBaseHook

class YoutubeReports(YoutubeBaseHook):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.youtube_analytics = self.get_service()
        self.youtube_data = self.get_data()

    """https://developers.google.com/youtube/analytics/"""
    """https://developers.google.com/youtube/analytics/channel_reports#video-reports"""
    
    def get_overview_reports(
        self, channel_id: str, metrics: List[str], ds: str, dimensions: str = "channel,day"
    ):
        self.refresh_token()
        response = self.youtube_analytics.reports().query(
            ids= f"channel=={channel_id}",
            startDate= ds,
            endDate= ds,
            dimensions= dimensions, 
            metrics= ",".join(metrics),
        ).execute()
        data = pd.DataFrame(response['rows'], columns=[header['name'] for header in response['columnHeaders']])
        data = data.to_dict(orient='records')
        print(data)
        item = self.get_channel_data()[0]
        print(item)
        item_ = [
            {
                "view_count": item["statistics"]["viewCount"],
                "subscriber_count": item["statistics"]["subscriberCount"],
                "hidden_subscriber_count": item["statistics"]["hiddenSubscriberCount"],
                "video_count": item["statistics"]["videoCount"],
                "description": item["snippet"]["description"],
                "custom_url": item["snippet"]["customUrl"],
                "published_at": item["snippet"]["publishedAt"],
                "privacy_status": item["status"]["privacyStatus"],
                "long_uploads_status": item["status"]["longUploadsStatus"],
                "made_for_kids": item["status"]["madeForKids"],
                "self_declared_made_for_kids": item["status"]["selfDeclaredMadeForKids"],
            } ]
        print(item_)
        data[0].update(item_[0])
        print(data)

        return data
    
    def get_traffic_sources(
        self, channel_id: str, metrics: List[str], ds: str, dimensions: str = "day,insightTrafficSourceType",
    ):
        self.refresh_token()
        response = self.youtube_analytics.reports().query(
            ids= f"channel=={channel_id}",
            startDate= ds,
            endDate= ds,
            dimensions= dimensions,
            metrics= ",".join(metrics),
        ).execute()

        data = pd.DataFrame(response['rows'], columns=[header['name'] for header in response['columnHeaders']])
        return data.to_dict(orient='records')
    
    def get_geographic_areas(
        self, channel_id: str, metrics: List[str], ds: str, dimensions: str = "channel,country",
    ):
        self.refresh_token()
        response = self.youtube_analytics.reports().query(
            ids= f"channel=={channel_id}",
            startDate= ds,
            endDate= ds,
            dimensions= dimensions,
            metrics= ",".join(metrics),
        ).execute()

        data = pd.DataFrame(response['rows'], columns=[header['name'] for header in response['columnHeaders']])
        return data.to_dict(orient='records')
    
    def get_viewers_demographics(
        self, channel_id: str, ds: str, metrics: str = "viewerPercentage", dimensions: str = "channel,ageGroup,gender",
    ):
        self.refresh_token()
        response = self.youtube_analytics.reports().query(
            ids= f"channel=={channel_id}",
            startDate= (datetime.strptime(ds.strftime('%Y-%m-%d'), '%Y-%m-%d') - timedelta(days=3)).date(),
            endDate= ds,
            dimensions= dimensions,
            metrics= metrics,
        ).execute()

        data = pd.DataFrame(response['rows'], columns=[header['name'] for header in response['columnHeaders']])
        return data.to_dict(orient='records')

    def get_content(
        self, channel_id: str, metrics: List[str], ds: str, part: List[str], dimensions: str = "day,video",
    ):  
        data = self.get_content_data(channel_id=channel_id, ds=ds, part=part)
        video_ids = ",".join(data["videoId"].tolist())
        self.refresh_token()
        response = self.youtube_analytics.reports().query(
            ids= f"channel=={channel_id}",
            startDate= ds,
            endDate= ds,
            dimensions= dimensions,
            metrics= ",".join(metrics),
            filters= f"video=={video_ids}"
        ).execute()

        content = pd.DataFrame(response['rows'], columns=[header['name'] for header in response['columnHeaders']])
        content = content.rename(columns={'video': 'videoId'})
        result = pd.merge(data, content, on= "videoId", how= "left")
        
        return result.to_dict(orient='records')
    
    def get_content_data(
        self,
        part: List[str],
        channel_id: str,
        ds: str,
        maxResults: int = 50,
    ):
        data = self.get_content_type(channel_id=channel_id, ds=ds)
        video_ids = data["videoId"].tolist()
        logging.info(f"The number of videos in channel_id = {channel_id} is {len(video_ids)}")
        content_data = pd.DataFrame()
        for start_index in range(0, len(video_ids), maxResults):
            video_ids_group = video_ids[start_index:start_index + maxResults]

            response = self.youtube_data.videos().list(
                part= ",".join(part),
                id= ",".join(video_ids_group), 
                maxResults= maxResults,
            ).execute()

            df = pd.json_normalize(response['items'])
            content_data = content_data.append(df, ignore_index=True)
            
        content_data = content_data.rename(columns={'id': 'videoId'})
        results = pd.merge(data, content_data, on= "videoId", how= "left")

        return results

    def get_content_type(
        self, channel_id: str, ds: str, metrics: str = "views", dimensions: str = "video,creatorContentType",

    ):
        data = self.get_content_list()
        video_ids = ",".join(data["videoId"].tolist())
        response = self.youtube_analytics.reports().query(
            ids= f"channel=={channel_id}",
            startDate= "2010-01-01",
            endDate= ds,
            dimensions= dimensions,
            metrics= metrics,
            filters= f"video=={video_ids}"
        ).execute()

        content_type = pd.DataFrame(response['rows'], columns=[header['name'] for header in response['columnHeaders']])
        content_type = content_type.rename(columns={'video': 'videoId'})
        results = pd.merge(data, content_type, on= "videoId", how= "left")

        return results

    def get_content_list(
        self, 
        part: str = "snippet, contentDetails", 
        maxResults: int = 50,
        upload_list: str = "UUs8t-T2D2C2HIXt3VIKHz0A", 
    ):
        data = pd.DataFrame()
        next_page_token = None
        while True:
            response = self.youtube_data.playlistItems().list(
                part= part,
                playlistId= upload_list, 
                maxResults= maxResults,
                pageToken= next_page_token
            ).execute() 
            
            items = response.get("items", [])
            data = data.append([
                {
                    "videoId": item["contentDetails"]["videoId"],
                    "position": item["snippet"]["position"],
                }
                for item in items
                ], ignore_index=True)
            
            # Check if there is a next page
            next_page_token = response.get('nextPageToken')
            # Break the loop if there are no more pages
            if not next_page_token:
                break

        return data

    def get_channel_data(
        self,
        part: str = "snippet,statistics,status",
        channel_id: str = "UCs8t-T2D2C2HIXt3VIKHz0A",
    ):
        response = self.youtube_data.channels().list(
                part= part,
                id = channel_id,
        ).execute() 

        return response.get("items", [])