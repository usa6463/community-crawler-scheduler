from datetime import datetime

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import V1EnvVar
from kubernetes.client import models as k8s_models
from slack_sdk import WebClient
from airflow.models import Variable


def success_msg(context):
    channel, client = _get_slack_info()

    text = f'''
    {context.get('task_instance').dag_id} {context.get('task_instance').task_id} success
    date: {context.get('execution_time')}
    '''
    client.chat_postMessage(channel=channel, text=text)


def failure_msg(context):
    channel, client = _get_slack_info()

    text = f'''
    {context.get('task_instance').dag_id} {context.get('task_instance').task_id} fail
    date: {context.get('execution_time')}
    exception: {context.get('exception')} 
    '''
    client.chat_postMessage(channel=channel, text=text)


def _get_slack_info():
    channel = Variable.get("channel")
    client = WebClient(token=Variable.get("slack_app_token"))
    return channel, client


default_args = {
    'owner': 'airflow',
    'on_success_callback': success_msg,
    'on_failure_callback': failure_msg,
}


@dag(default_args=default_args, schedule_interval="@daily", start_date=datetime(2023, 1, 2), max_active_runs=1)
def dc_scrapping():
    man_fashion_gall = KubernetesPodOperator(
        name="scrap-dc-man-fashion",  # pod name
        namespace="airflow",
        env_vars=[
            V1EnvVar(name="ES_HOST", value="elasticsearch-master.elasticsearch.svc.cluster.local"),
            V1EnvVar(name="ES_PORT", value="9200"),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="LOGGING_LEVEL", value="info"),
            V1EnvVar(name="BOARD_BASE_URL", value="https://gall.dcinside.com/mgallery/board/lists?id=mf"),
            V1EnvVar(name="ES_INDEX_NAME", value="dc-content-mgallery-man-fashion"),
            V1EnvVar(name="WEB_DRIVER_PATH", value="/chromedriver"),
            V1EnvVar(name="POLITENESS", value="1500"),
        ],
        image="usa6463/community-crawler:2.2.16",
        task_id="scrap-dc-man-fashion",
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "2G", "cpu": "2000m"},
        ),
        retries=1
    )

    man_fashion_gall.dry_run()

dag = dc_scrapping()