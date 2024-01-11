import time
import uuid
from typing import Any, List, Sequence, Union
from airflow.models.baseoperator import BaseOperator
from social.hooks.youtube.youtube_reports import YoutubeReports
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from datetime import datetime, timedelta
import logging

class YoutubeDemographicsOperator(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
    )


    def __init__(
        self,
        channel_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.hook = YoutubeReports()
        self.channel_id = channel_id


    def execute(self, context: Any):
        ds = context["ds"]
        self._get_data_and_upload(ds)

    def _get_data_and_upload(self,ds):
        partition_prefix = f"channel_id={self.channel_id}/snapshot_date={ds}"
        collected_ts = round(time.time() * 1000)

        response= self.hook.get_viewers_demographics(channel_id=self.channel_id, ds= (datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=3)).date())
        logging.info(response)
        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=response,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )