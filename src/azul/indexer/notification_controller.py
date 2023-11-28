import chalice.app

from azul import (
    cached_property,
)
from azul.chalice import (
    AppController,
)
from azul.indexer.notify_service import (
    EmailService,
)


class MonitoringController(AppController):

    @cached_property
    def email_service(self):
        return EmailService()

    def notify_group(self, event: chalice.app.SNSEvent) -> None:
        self.email_service.send_message(event.subject, event.message)
