from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
import slack_alerter

default_args = {
    'owner': 'airflow',
    'on_success_callback': slack_alerter.success_msg,
    'on_failure_callback': slack_alerter.failure_msg,
}


@dag(default_args=default_args, schedule_interval=None, start_date=None, max_active_runs=1)
def test_dag():
    test_bash = BashOperator(
        name="test-bash",  # pod name
        bash_command="echo hello"
    )

    test_bash.dry_run()


dag = test_dag()
