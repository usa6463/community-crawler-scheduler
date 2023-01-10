from airflow.decorators import dag
from airflow.operators.bash import BashOperator
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


@dag(default_args=default_args, schedule_interval=None, start_date=None, max_active_runs=1)
def test_dag():
    test_bash = BashOperator(
        name="test-bash",  # pod name
        bash_command="echo hello"
    )

    test_bash.dry_run()


dag = test_dag()
