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
            V1EnvVar(name="TARGET_DATE", value="{{ ds }}")
        ],
        image="usa6463/community-crawler:2.0.0",
        # arguments=["--target_date", "{{ next_ds }}",
        #            "--last_content_num", "2430001",
        #            "--elasticsearch_hostname", "elasticsearch-master.default.svc.cluster.local",
        #            "--elasticsearch_port", "9200",
        #            "--elasticsearch_index_name", "dc-content-test"],
        task_id="scrap-dc-stock",
    )

    k.dry_run()


dag = dc_scrapping()
