import logging

from airflow.models import Variable
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackApp:
    def __init__(self, channel_id: str):
        authen = Variable.get(f"slack", deserialize_json=True)
        self._client = WebClient(token=authen.get("bot_user_oauth_token"))
        self.channel_id = channel_id

    def post_message(self, text: str = None, blocks: list = None, attachments: list = None, **kwargs):
        """
            Wrapper of Slack's chat_postMessage
            https://api.slack.com/methods/chat.postMessage#formatting
        """
        try:
            response = self._client.chat_postMessage(
                channel=self.channel_id,
                text=text,
                blocks=blocks,
                attachments=attachments,
                **kwargs,
            )
        except SlackApiError as e:
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            logging.error(f"Got an error: {e.response['error']}")
