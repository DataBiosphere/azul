import boto3

from botocore.exceptions import ClientError

from azul import config
from azul.es import es_client


def get_elasticsearch_health():
    is_es_reachable = es_client().ping()

    result = {
        'status': 'UP' if is_es_reachable else 'DOWN',
        'domain': config.es_domain
    }
    if not is_es_reachable:
        result['message'] = 'Unable to reach the host'
    return result


def get_queue_health():
    sqs = boto3.resource('sqs')

    def queue_health(queue_name):
        try:
            queue = sqs.get_queue_by_name(QueueName=f'azul-{queue_name}-{config.deployment_stage}')
            return {
                'status': 'UP',
                'messages': {
                    'queued': queue.attributes['ApproximateNumberOfMessages'],
                    'delayed': queue.attributes['ApproximateNumberOfMessagesDelayed'],
                    'invisible': queue.attributes['ApproximateNumberOfMessagesNotVisible']
                }
            }
        except ClientError as ex:
            return {
                'status': 'DOWN',
                'message': ex.response['Error']['Message']
            }

    return {queue_name: queue_health(queue_name) for queue_name in ['notify', 'fail']}
