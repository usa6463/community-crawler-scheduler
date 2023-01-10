from slack_sdk import WebClient
from airflow.models import Variable


class slack_alerter:

    def success_msg(self, context):
        channel, client = self._get_slack_info()

        text = f'''
        {context.get('task_instance').dag_id} {context.get('task_instance').task_id} success
        date: {context.get('execution_time')}
        '''
        client.chat_postMessage(channel=channel, text=text)

    def failure_msg(self, context):
        channel, client = self._get_slack_info()

        text = f'''
        {context.get('task_instance').dag_id} {context.get('task_instance').task_id} fail
        date: {context.get('execution_time')}
        exception: {context.get('exception')} 
        '''
        client.chat_postMessage(channel=channel, text=text)

    def _get_slack_info(self):
        channel = Variable.get("channel")
        client = WebClient(token=Variable.get("slack_app_token"))
        return channel, client
