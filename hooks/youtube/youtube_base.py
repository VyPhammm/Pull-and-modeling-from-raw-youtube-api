
import datetime, traceback, requests
from googleapiclient.discovery      import build
from google.oauth2.credentials      import Credentials
from airflow.models import Variable
import logging

class YoutubeBaseHook():
    def __init__(self, *args, **kwargs):
        self.access = Variable.get("youtube_token", deserialize_json=True)
        self.scopes = ["https://www.googleapis.com/auth/youtube.readonly",
                    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
                    "https://www.googleapis.com/auth/yt-analytics.readonly",
                ]
        self.yt_api_service = 'youtubeAnalytics'
        self.yt_api_service_version = "v2"
        self.yt_api_data = "youtube"
        self.yt_api_data_version = "v3"
        self.access['scopes'] = self.scopes

    def get_service(self):
        try:
            credentials = Credentials.from_authorized_user_info(self.access)
            return build(self.yt_api_service, self.yt_api_service_version, credentials=credentials)
        except: logging.info(f'Failed to build service:\n{traceback.format_exc()}')
    
    def get_data(self):
        try:
            credentials = Credentials.from_authorized_user_info(self.access)
            return build(self.yt_api_data, self.yt_api_data_version, credentials=credentials)
        except: logging.info(f'Failed to build service:\n{traceback.format_exc()}')

    def refresh_token (self,token=None):
        data = {
            'client_id': self.access['client_id'],
            'client_secret': self.access['client_secret'],
            'refresh_token': self.access['refresh_token'],
            'grant_type': 'refresh_token'
        }
        response = requests.post('https://accounts.google.com/o/oauth2/token', data=data)
        if response.status_code == 200:
            response_json = response.json()
            self.access['access_token'] = response_json['access_token']
            self.access['expires_in'] = response_json['expires_in']
            now = datetime.datetime.now()
            self.access['token_expiry'] = (now + datetime.timedelta(seconds=response_json['expires_in'])).isoformat()

            message = f"{response.status_code}:\tSuccessfully refreshed access token\n{datetime.datetime.now()}\n"
        else:
            message = f"{response.status_code}:\tFalied to refresh access token\t{datetime.datetime.now()}\n{response.text}"
        return logging.info(message)
    