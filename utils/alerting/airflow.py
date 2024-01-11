import logging
import re
from datetime import datetime
from typing import Dict, List, Union
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery

from utils.alerting.slack import SlackApp
from utils.common import set_env_value
from utils.constants import SipherSlackChannelID
from utils.data_extract.clickup import ClickUp
from utils.data_upload.bigquery_upload import BigQueryDataUpload

CHANNEL_ID = set_env_value(
    production=SipherSlackChannelID.AIRFLOW, dev=SipherSlackChannelID.TEST
)
DATASET_NAME = set_env_value(production="airflow_monitoring", dev="tmp3")

BIGQUERY_PROJECT = Variable.get("bigquery_project")
BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")

PRIORITY = {"urgent": 1, "high": 2, "normal": 3, "low": 5}

clickup = ClickUp()


"""
    Logs: 2024-01-02
    Clickup Docs: https://clickup.com/api/
"""

def airflow_callback(context):
    """Use in `on_failure_callback` Airflow arguments"""
    error_info = get_dag_info(context)

    #Use this to disable creating ClickUp task to avoid contaminating backlog, uncomment if you need to test the feature below
    if Variable.get("execution_environment", default_var="dev") != "production":
        error_info.update({"clickup_task_url": "http://localhost:8080/"})
        send_error_data(error_info=error_info)
        return

    error_log = check_airflow_error_log(error_info)
    if not error_log:
        logging.info("New error!!!")
        new_task = create_click_up_task(error_info).json()
        update_clickup_info_to_error_info(new_task, error_info)
        send_error_data(error_info)
        return

    (clickup_task_id, last_noti_time) = error_log
    timedelta = datetime.now().astimezone() - last_noti_time
    if timedelta.seconds < 7200:  # no send notification for same error within 2 hours
        logging.info(
            "There is the same notification in the last 2 hours, snoozing this."
        )
        return

    clickup_task = clickup.get_task(clickup_task_id)

    if clickup_task["status"]["status"].lower() in ["done", "resolved", "skip"]:
        logging.info("Error was resolved in the past but occuring again")
        new_task = create_click_up_task(error_info).json()
        update_clickup_info_to_error_info(new_task, error_info)
        send_error_data(error_info)

    elif clickup_task["status"]["status"].lower() == "to do":
        if not clickup_task["assignees"]:
            msg = "No assignee for this bug!!!!"
            logging.info(msg)
            update_clickup_info_to_error_info(clickup_task, error_info)
            send_error_data(error_info=error_info, text=msg)
            return

        timedelta = datetime.now() - datetime.fromtimestamp(
            int(clickup_task["date_updated"]) / 1000
        )
        waiting_time = (
            PRIORITY[clickup_task["priority"]["priority"]]
            if clickup_task["priority"] is not None
            else 3
        )

        if timedelta.days > waiting_time:
            msg = f"This bug was from {timedelta} days ago, please fix!!!!"
            logging.info(msg)
            update_clickup_info_to_error_info(clickup_task, error_info)
            send_error_data(error_info=error_info, text=msg)
        else:
            logging.info("Ticket is in queue, snoozing error notification")

    else:
        logging.info("Ticket is being processing, snoozing error notification")


def get_dag_info(context) -> Dict:
    """Extract DAGs info from airflow arguments"""
    dag_info = {}
    dag_info["task_execution_date"] = context["data_interval_start"]
    dag_info["task_id"] = context["task"].task_id
    dag_info["dag_id"] = context["dag"].dag_id
    dag_info["owner"] = context["dag"].default_args.get("owner")
    dag_info["task_log_url"] = context.get("task_instance").log_url
    dag_info["airflow_url"] = re.search("^(.+?)/log", dag_info["task_log_url"]).group(
        1
    )  # Get airflow base url from log url because there is no existing method for it
    dag_info[
        "dag_tree_url"
    ] = f"{dag_info['airflow_url']}/tree?dag_id={dag_info['dag_id']}"
    return dag_info


def get_assignee_run_dag(error_info) -> Union[List[int], None]:
    owner = error_info.get('owner')
    members = clickup.get_list_members(clickup.bug_list_id).get('members')

    if not members:
        return None
    
    return [
        member.get('id') 
            for member in members 
                if member.get('email').split('@')[0] == owner
    ]

def create_click_up_task(error_info: Dict):
    """Create ClickUp ticket bug"""
    assignees: Union[List[int], None] = get_assignee_run_dag(error_info)
    payload = {
        "name": f"`{error_info.get('task_id')}` task in `{error_info.get('dag_id')}` DAG failed",
        "description": "----- Auto-generated ticket from bot -----",
        "tags": ["auto-generated"],
        "status": "TO DO",
        "notify_all": True,
        "assignees": assignees
    }

    return clickup.create_tasks(clickup.bug_list_id, payload=payload)


def update_clickup_info_to_error_info(clickup_task, error_info):
    error_info.update(
        {"clickup_task_id": clickup_task["id"], "clickup_task_url": clickup_task["url"]}
    )


def check_airflow_error_log(error_info):

    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
    )
    task_id = error_info.get("task_id")
    dag_id = error_info.get("dag_id")
    logging.info("Check log of this error id")
    query = f"""
        SELECT
            dag_id, task_id, clickup_task_id, created_at
        FROM `sipher-data-platform.{DATASET_NAME}.airflow_task_error_log`
        WHERE task_id = "{task_id}" AND dag_id = "{dag_id}"
        QUALIFY created_at = MAX(created_at) OVER(PARTITION BY task_id, dag_id)

    """
    print(query)
    res = client.query(query, project=BIGQUERY_BILLING_PROJECT).result()
    if res.total_rows == 0:
        return None
    row = next(res)
    return (row.clickup_task_id, row.created_at)


def send_error_data(error_info: Dict, text: str = None):
    logging.info("Post error message to Slack")
    slack = SlackApp(channel_id=CHANNEL_ID)
    attachments = create_airflow_attachment(error_info)
    slack.post_message(text=text, attachments=attachments)

    logging.info("Send error data to BigQuery")
    send_error_info_to_bq(error_info)


def create_airflow_attachment(error_info: Dict):
    slack_attachment = [
        {
            "fallback": "Task failed",
            "color": "#dd4b39",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*TASK FAILED!*"},
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Task*: {error_info['task_id']}\n *DAG*: {error_info['dag_id']} \n *Execution Time*: {error_info['task_execution_date']} \n *Owner*: @{error_info['owner']}",
                    },
                },
                {"type": "divider"},
                # The right way to use Slack button is to send payload to url set in https://api.slack.com/apps/A042GLMAW5R/interactive-messages
                # But here we just wanna make a fancy link clicks, but Slack doesn't have disable option for sending POST request
                # So work around by adding public address https://httpstat.us/200 to request URL in Interactivity
                # Read more: https://github.com/slackapi/node-slack-sdk/issues/869
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View Task Logs",
                                "emoji": True,
                            },
                            "style": "primary",
                            "url": error_info["task_log_url"],
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View DAG Tree"},
                            "style": "primary",
                            "url": error_info["dag_tree_url"],
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View ClickUp Ticket",
                            },
                            "style": "primary",
                            "url": error_info["clickup_task_url"],
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View Documentation",
                            },
                            "style": "primary",
                            "url": "https://www.notion.so/sipherhq/Airflow-Slack-notification-9623c76428a1454aab9c3535cdf469af",
                        },
                    ],
                },
            ],
        }
    ]
    return slack_attachment


def send_error_info_to_bq(error_info: Dict):
    error_info["created_at"] = datetime.now()
    df = pd.DataFrame(error_info, index=[0])
    service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson[
        "key_path"
    ]
    uploader = BigQueryDataUpload(
        service_account_json_path=service_account_json_path,
        dataset_name=DATASET_NAME,
        table_name="airflow_task_error_log",
        project_id=BIGQUERY_PROJECT,
    )
    uploader.job_config.write_disposition = "WRITE_APPEND"
    uploader.load_dataframe(df)