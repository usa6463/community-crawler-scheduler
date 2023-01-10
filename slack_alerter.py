from slack_sdk import WebClient
from datetime import datetime

class slack_alerter:

    def __init__(self, channel, token):
        self.client = WebClient(token=token)
        self.channel = channel

    def success_msg(self, context):
        text = f'''
        {context.get('task_instance').dag_id} {context.get('task_instance').task_id} success
        date: {context.get('execution_time')}
        '''
        self.client.chat_postMessage(channel=self.channel, text=text)

    def failure_msg(self, context):
        text = f'''
        {context.get('task_instance').dag_id} {context.get('task_instance').task_id} fail
        date: {context.get('execution_time')}
        exception: {context.get('exception')} 
        '''
        self.client.chat_postMessage(channel=self.channel, text=text)

