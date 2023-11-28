import json
import logging

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.strings import (
    trunc_ellipses,
)

log = logging.getLogger(__name__)


class EmailService:

    @property
    def to_email(self):
        return config.monitoring_email

    @property
    def from_email(self):
        return ' '.join([
            'Azul',
            config.deployment_stage,
            'Monitoring',
            '<monitoring@' + config.api_lambda_domain('indexer') + '>'
        ])

    def send_message(self, subject: str, body: str) -> None:
        log.info('Sending message %r with body %r',
                 subject, trunc_ellipses(body, 256))
        try:
            body = json.loads(body)
        except json.decoder.JSONDecodeError:
            log.warning('Not a JSON serializable event, sending as is')
        else:
            body = json.dumps(body, indent=4)
        content = {
            'Simple': {
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body
                    }
                }
            }
        }
        response = aws.ses.send_email(FromEmailAddress=self.from_email,
                                      Destination=dict(ToAddresses=[self.to_email]),
                                      Content=content)
        log.info('Successfully sent message %r, message ID is %r',
                 subject, response['MessageId'])
