import logging

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference


class BigQueryDataUpload:
    def __init__(
        self,
        service_account_json_path: str,
        dataset_name: str,
        table_name: str,
        project_id: str = None,
        **kwargs,
    ):
        self.bq_client = bigquery.Client.from_service_account_json(
            service_account_json_path
        )

        self.bq_dataset = self.bq_client.dataset(dataset_name, project=project_id)
        self.bq_table = self.bq_dataset.table(table_name)

        self.job_config = bigquery.LoadJobConfig()
        self.job_config.write_disposition = "WRITE_TRUNCATE"
        self.job_config.autodetect = True

    def load_file(self, file_out):
        self.bq_client.load_table_from_file(
            file_out, self.bq_table, job_config=self.job_config
        ).result()
        logging.info(
            f"Upload to `{self.bq_table.project}.{self.bq_table.dataset_id}.{self.bq_table.table_id}`: done"
        )

    def load_dataframe(self, df):
        if df.empty:
            logging.info("No data")
            return

        self.bq_client.load_table_from_dataframe(
            df, self.bq_table, job_config=self.job_config
        ).result()

        logging.info(
            f"Upload to `{self.bq_table.project}.{self.bq_table.dataset_id}.{self.bq_table.table_id}`: done"
        )

    def load_gcs_uri(self, source_uri):
        self.bq_client.load_table_from_uri(
            source_uris=source_uri,
            destination=self.table_name,
        ).result()

        logging.info(
            f"Upload {source_uri} to `{self.table.project}.{self.table.dataset_id}.{self.table.table_id}`: done"
        )


def create_external_bq_table_to_gcs(
    gcp_conn_id: str,
    bq_project: str,
    bq_dataset: str,
    bq_table: str,
    gcs_bucket: str,
    gcs_object_prefix: str,
    gcs_partition_expr: str,
    gcs_source_format: str = "PARQUET",
    **kwargs,
):
    """
    Create external table `{bq_project}.{bq_dataset}.{bq_table}`
    which will point to `gs://{gcs_bucket}/{gcs_object_prefix}/{gcs_partition_expr}`

    :param gcp_conn_id: The GCP connection ID with the service account details
    :param bq_project: BigQuery project id of destination table
    :param bq_dataset: BigQuery dataset name of destination table
    :param bq_table: BigQuery table name id of destination table
    :param gcs_bucket: Cloud Storage bucket that contains the data for the table,
    :param gcs_object_prefix: Cloud Storage URI prefix, not including bucket and partition schema
    :param gcs_partition_expr: the partition key schema, in format {key1:type1}/{key2:type2}
    :param gcs_source_format: the format of the external data source. For example: CSV, JSON, PARQUET
    """

    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

    destination_table_name = f"{bq_project}.{bq_dataset}.{bq_table}"
    destination_table_ref = TableReference.from_string(destination_table_name)
    source_uri_prefix = f"gs://{gcs_bucket}/{gcs_object_prefix}"

    bq_hook.delete_table(table_id=str(destination_table_ref))
    logging.info("Drop existing external table %s successfully", destination_table_ref)

    external_table = bq_hook.create_empty_table(
        table_resource={
            "tableReference": destination_table_ref.to_api_repr(),
            "type": "EXTERNAL",
            "externalDataConfiguration": {
                "sourceUris": [f"{source_uri_prefix}/*"],
                "sourceFormat": gcs_source_format,
                "ignoreUnknownValues": True,
                "hivePartitioningOptions": {
                    "mode": "CUSTOM",
                    "sourceUriPrefix": f"{source_uri_prefix}/{gcs_partition_expr}",
                    "fields": True,
                },
            },
        },
    )

    logging.info("Create table %s successfully", external_table)
