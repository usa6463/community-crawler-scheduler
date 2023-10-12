from datetime import datetime

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import V1EnvVar
from kubernetes.client import models as k8s_models
from slack_sdk import WebClient

ES_URL = "elasticsearch-master.elasticsearch.svc.cluster.local"
PGSQL_URL = "postgresql://{{ conn.my_pg.login }}:{{ conn.my_pg.password }}@{{ conn.my_pg.host }}/{{ conn.my_pg.schema }}"
COMMUNITY_CRAWLER_NLP_IMAGE = "usa6463/community-crawler-nlp:v0.2.0"


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


@dag(default_args=default_args, schedule_interval="@daily", start_date=datetime(2023, 7, 21), max_active_runs=1)
def dc_scrapping():
    man_fashion_gall = KubernetesPodOperator(
        name="scrap-dc-man-fashion",  # pod name
        namespace="airflow",
        env_vars=[
            V1EnvVar(name="ES_HOST", value=ES_URL),
            V1EnvVar(name="ES_PORT", value="9200"),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="LOGGING_LEVEL", value="debug"),
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

    tag_morpheme = KubernetesPodOperator(
        name="tag_morpheme",  # pod name
        namespace="airflow",
        env_vars=[
            V1EnvVar(name="ES_URL", value=ES_URL),
            V1EnvVar(name="PGSQL_URL",
                     value=PGSQL_URL),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
            V1EnvVar(name="TARGET_INDEX", value="dc-content-mgallery-man-fashion"),
        ],
        image=("%s" % COMMUNITY_CRAWLER_NLP_IMAGE),
        task_id="tag_morpheme",
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "2G", "cpu": "2000m"},
        ),
        retries=1
    )

    monthly_statistics = KubernetesPodOperator(
        name="monthly_statistics",  # pod name
        namespace="airflow",
        env_vars=[
            V1EnvVar(name="PGSQL_URL",
                     value=PGSQL_URL),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
        ],
        image=COMMUNITY_CRAWLER_NLP_IMAGE,
        task_id="monthly_statistics",
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "2G", "cpu": "2000m"},
        ),
        cmds=["pipenv", "run", "run_monthly_statistics"],
        retries=1
    )

    extract_brand_from_static_name_pool = KubernetesPodOperator(
        name="extract_brand_from_static_name_pool",  # pod name
        namespace="airflow",
        env_vars=[
            V1EnvVar(name="ES_URL", value=ES_URL),
            V1EnvVar(name="PGSQL_URL",
                     value=PGSQL_URL),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
            V1EnvVar(name="TARGET_INDEX", value="dc-content-mgallery-man-fashion"),
        ],
        image=("%s" % COMMUNITY_CRAWLER_NLP_IMAGE),
        task_id="extract_brand_from_static_name_pool",
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "2G", "cpu": "2000m"},
        ),
        cmds=["pipenv", "run", "run_extract_brand_from_static_name_pool"],
        retries=1
    )

    monthly_brand_from_static_name_pool = KubernetesPodOperator(
        name="monthly_brand_from_static_name_pool",  # pod name
        namespace="airflow",
        env_vars=[
            V1EnvVar(name="PGSQL_URL",
                     value=PGSQL_URL),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
        ],
        image=("%s" % COMMUNITY_CRAWLER_NLP_IMAGE),
        task_id="monthly_brand_from_static_name_pool",
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "2G", "cpu": "2000m"},
        ),
        cmds=["pipenv", "run", "run_extract_brand_from_static_name_pool"],
        retries=1
    )

    man_fashion_gall >> tag_morpheme >> monthly_statistics
    man_fashion_gall >> extract_brand_from_static_name_pool >> monthly_brand_from_static_name_pool


dag = dc_scrapping()
