import logging
from typing import Dict, Callable, Union, List, Any
from airflow.hooks.base_hook import BaseHook
from google.cloud import storage
from enum import Enum
import pandas as pd


class SupportedGcsFileFormat(Enum):
    JSON = 1
    PARQUET = 2


class FileFormatExtension(Enum):
    JSON = "json"
    PARQUET = "parquet"


class GCSDataUpload:
    def __init__(
        self,
        gcs_bucket: str,
        gcs_prefix: str,
        service_account_json_path: str = None,
        gcp_conn_id: str = "sipher_gcp",
        **kwargs,
    ):
        if service_account_json_path:
            self.storage_client = storage.Client.from_service_account_json(
                service_account_json_path
            )
        else:
            self.storage_client = storage.Client.from_service_account_json(
                BaseHook.get_connection(gcp_conn_id).extra_dejson[
                    "key_path"
                ]
            )

        self.bucket = self.storage_client.get_bucket(gcs_bucket)
        self.gcs_prefix = gcs_prefix

    def upload(
        self,
        object_name: str,
        data: Union[Dict, List, pd.DataFrame, Any],
        gcs_file_format: SupportedGcsFileFormat = SupportedGcsFileFormat.JSON,
        pre_upload_callable: Callable[[pd.DataFrame], pd.DataFrame] = lambda x: x,
        **kwargs,
    ):

        df = None

        if isinstance(data, List):
            df = pd.DataFrame.from_records(data)
        elif isinstance(data, Dict):
            df = pd.json_normalize(data, max_level=0)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise Exception("Unsupported data type")

        if df.empty:
            logging.info("No data, skip uploading")
            return

        pre_upload_df = pre_upload_callable(df)

        object_name_with_ext = "".join(
            [
                self.gcs_prefix,
                self._norm_path_part(object_name),
                ".",
                FileFormatExtension[gcs_file_format.name].value,
            ]
        )

        {
            SupportedGcsFileFormat.PARQUET: self.upload_parquet(
                df=pre_upload_df, object_name=object_name_with_ext
            )
        }.get(gcs_file_format)

    @staticmethod
    def _prepare_before_upload(collected_ts):
        def call(df: pd.DataFrame):
            if df.empty:
                return df
            transformed_df = df.copy()

            transformed_df["__collected_ts"] = collected_ts
            transformed_df.info(memory_usage="deep")
            return transformed_df

        return call

    def upload_parquet(self, df, object_name: str):
        blob = self.bucket.blob(object_name)
        blob.upload_from_string(df.to_parquet())
        logging.info("Data stream upload to `%s` in %s", object_name, self.bucket)

    @staticmethod
    def _norm_path_part(sub_path: str):
        if sub_path[0] == "/":
            return sub_path
        else:
            return f"/{sub_path}"
