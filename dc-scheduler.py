from datetime import datetime

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import V1EnvVar

default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval="@daily", start_date=datetime(2022, 4, 20), max_active_runs=1)
def dc_scrapping():
    k = KubernetesPodOperator(
        name="scrap-dc-stock",  # pod name
        namespace="default",
        env_vars=[
            V1EnvVar(name="ES_HOST", value="elasticsearch-master.default.svc.cluster.local"),
            V1EnvVar(name="ES_PORT", value="9200"),
            V1EnvVar(name="TARGET_DATE", value="{{ prev_ds }}"),
            V1EnvVar(name="LOGGING_LEVEL", value="info"),
            V1EnvVar(name="BOARD_BASE_URL", value="https://gall.dcinside.com/board/lists/?id=rlike"),
            V1EnvVar(name="ES_INDEX_NAME", value="dc-content-loglike"),
            V1EnvVar(name="WEB_DRIVER_PATH", value="/chromedriver"),
        ],
        image="usa6463/community-crawler:2.0.0",
        task_id="scrap-dc-stock",
    )

    k.dry_run()


dag = dc_scrapping()
