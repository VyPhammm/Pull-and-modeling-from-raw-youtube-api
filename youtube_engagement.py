from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator

from operators.youtube.overviews_reports import YoutubeOverviewReportsOperator
from operators.youtube.traffic_sources import YoutubeTrafficSourcesOperator
from operators.youtube.content import YoutubeContentOperator
from operators.youtube.geographic_areas import YoutubeGeographicOperator
from operators.youtube.demographics import YoutubeDemographicsOperator
from scripts.youtube_utils import Channels, Params_yt
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.dbt import default_args_for_dbt_operators

DAG_START_DATE = set_env_value(
    production=datetime(2023, 11, 1), dev=datetime(2023, 12, 25)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")

GCP_CONN_ID = "vypham_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_youtube"

BUCKET = Variable.get("vypham_ingestion_gcs_bucket", "vypham-test")
GCS_PREFIX = "youtube_data"

default_args = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}
default_args.update(default_args_for_dbt_operators)

TWITTER_TOKEN = Variable.get(f"twitter_token", deserialize_json=True)


@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket=BUCKET,
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
    )

with DAG(
    dag_id="youtube_engagement",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    concurrency= 5,
    max_active_runs= 1,
    tags=["social", "bigquery"],
    catchup=True,
) as dag:
    
    with TaskGroup(group_id="youtube") as youtube:

        with TaskGroup(group_id="ingest") as youtube_ingest_group:
            report_types = Params_yt.REPORT_TYPES
            for report in report_types.keys():
                with TaskGroup(group_id=report) as facebook_report_type_group:
                    gcs_prefix = f"{GCS_PREFIX}/youtube/{report}"
                    bq_table_name = f"youtube_{report}"
                    dbt_stg_model_name = f"stg_youtube_{report}"

                    # =======Common operators =============
                    create_big_lake_table = create_big_lake_table_task(
                        bq_table_name=bq_table_name,
                        gcs_prefix=gcs_prefix,
                        gcs_partition_expr=report_types.get(report).get("partition_expr"),
                    )

                    populate_report_to_staging = DbtRunOperator(
                        task_id="populate_report_to_staging",
                        models=dbt_stg_model_name,
                        vars={"snapshot_date": "{{ ds }}"},
                    )
                    # =======================
                    if report == "overview":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeOverviewReportsOperator(
                                    task_id="overview",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.DIMENSIONS,
                                )
                        pull_data
                    
                    if report == "traffic_sources":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeTrafficSourcesOperator(
                                    task_id="traffic_sources",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.TRAFFIC_SOURCES,
                                )
                        pull_data
                    
                    if report == "contents":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeContentOperator(
                                    task_id="contents",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.CONTENTS,
                                    part= Params_yt.PART_CONTENT,
                                )
                        pull_data 
                    
                    if report == "geographic_areas":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeGeographicOperator(
                                    task_id="geographic_areas",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.DIMENSIONS,
                                )
                        pull_data

                    if report == "demographics":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeDemographicsOperator(
                                    task_id="demographics",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                )
                        pull_data
                    
                pull_data >> create_big_lake_table >> populate_report_to_staging
        with TaskGroup(group_id="model") as youtube_model_group:

            dim_youtube_contents = DbtRunOperator(
                task_id="dim_youtube_contents",
                models= "dim_youtube_contents",
            )

            fct_youtube_channel = DbtRunOperator(
                task_id="fct_youtube_channel",
                models= "fct_youtube_channel",
            )

            fct_youtube_contents = DbtRunOperator(
                task_id="fct_youtube_contents",
                models= "fct_youtube_contents",
            )

        youtube_ingest_group  >> youtube_model_group 
       
 
