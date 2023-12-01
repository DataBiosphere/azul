"""
This script prompts the user for an AWS SNS subscription confirmation link and
confirms the subscription with AuthenticateOnUnsubscribe enabled. Without
AuthenticateOnUnsubscribe, anyone with an unsubscribe link would be able to
delete the subscription, regardless if the subscription was for a group email
address.
"""
import logging
from unittest import (
    mock,
)

from furl import (
    furl,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main() -> None:
    if config.disable_monitoring:
        print('SNS subscription confirmation skipped, monitoring is disabled.')
    elif subscription_pending_confirmation():
        url = prompt_for_subscription_url()
        confirm_subscription(url)
    else:
        print('SNS subscription confirmation skipped, no pending subscriptions found.')


def sns_topic_arn() -> str:
    """
    Return the ARN of the SNS topic.
    """
    return ':'.join([
        'arn',
        'aws',
        'sns',
        config.region,
        config.aws_account_id,
        aws.monitoring_topic_name
    ])


def subscription_pending_confirmation() -> bool:
    """
    Return True if there is a pending subscription to the deployment's SNS topic.
    """
    paginator = aws.sns.get_paginator('list_subscriptions_by_topic')
    pending_subs = []
    for page in paginator.paginate(TopicArn=sns_topic_arn()):
        for subscription in page['Subscriptions']:
            if subscription['SubscriptionArn'] == 'PendingConfirmation':
                assert config.monitoring_email == subscription['Endpoint'], subscription
                pending_subs.append(subscription)
    assert len(pending_subs) in (0, 1), pending_subs
    return bool(pending_subs)


def prompt_for_subscription_url() -> furl:
    """
    Prompt user for a confirmation URL.
    """
    print('\n'.join([
        '',
        f'To confirm the pending subscription of {config.monitoring_email}',
        'to the SNS topic that receives monitoring and security notifications, please',
        'open the "Subscription Confirmation" email, copy the "Confirm subscription"',
        'link address, and paste it here.'
    ]))
    while True:
        url = input_tty('URL: ').strip()
        if url:
            return furl(url)


def confirm_subscription(url: furl) -> None:
    """
    Confirm an SNS subscription and enable AuthenticateOnUnsubscribe.
    """
    topic_arn = url.args['TopicArn']
    token = url.args['Token']
    endpoint = url.args['Endpoint']

    aws.sns.confirm_subscription(TopicArn=topic_arn,
                                 Token=token,
                                 AuthenticateOnUnsubscribe='true')
    print('Subscription confirmed for:', endpoint)


def input_tty(prompt: str = '') -> str:
    try:
        return input(prompt)
    except EOFError:
        # Handles input when the script is invoked via Terraform with a
        # local-exec provisioner
        with open('/dev/tty', 'r') as f:
            with mock.patch('sys.stdin', new=f):
                return input(prompt)


if __name__ == '__main__':
    configure_script_logging(log)
    main()
