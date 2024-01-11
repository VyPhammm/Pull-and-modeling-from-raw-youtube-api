import time
import uuid
from typing import Any, List, Sequence, Union
from airflow.models.baseoperator import BaseOperator
from social.hooks.youtube.youtube_reports import YoutubeReports
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from datetime import datetime, timedelta
import logging

class YoutubeContentOperator(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
    )

    def __init__(
        self,
        channel_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        metrics: List[str],
        part: List[str],
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.hook = YoutubeReports()
        self.channel_id = channel_id
        self.metrics = metrics
        self.part = part

    def execute(self, context: Any):
        ds = context["ds"]
        self._get_data_and_upload(ds)

    def _get_data_and_upload(self,ds):
        partition_prefix = f"channel_id={self.channel_id}/snapshot_date={ds}"
        collected_ts = round(time.time() * 1000)

        raw_response= self.hook.get_content(channel_id=self.channel_id, metrics=self.metrics, part=self.part, ds= (datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=3)).date())
        response = self.convert_keys(raw_response)
        logging.info(response)
        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=response,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )

    def convert_keys(self,data):
        if isinstance(data, dict):
            return {k.replace(".", "_"): self.convert_keys(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.convert_keys(item) for item in data]
        else:
            return data