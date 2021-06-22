import json
import logging
import subprocess

import requests
from requests.auth import (
    HTTPBasicAuth,
)

from azul import (
    config,
)
from azul.logging import (
    configure_script_logging,
)

logger = logging.getLogger(__name__)


def main():
    if config.enable_monitoring:
        base_url = f'{config.grafana_endpoint}/api'
        for dashboard in ['azul', 'data_portal']:
            update_dashboard(base_url, get_dashboard_json_from_terraform(dashboard))
    else:
        logger.info('Skipping publishing of Grafana dashboard')


def get_dashboard_json_from_terraform(dashboard):
    logger.info('Extracting dashboard definition for %s', dashboard)
    cmd = f'terraform output grafana_dashboard_{dashboard}'
    completed_process = subprocess.run(cmd,
                                       stdout=subprocess.PIPE,
                                       shell=True,
                                       cwd=f'{config.project_root}/terraform')
    return json.loads(completed_process.stdout)


def update_dashboard(base_url, dashboard):
    url = base_url + '/dashboards/db'
    logger.info('Updating Grafana dashboard definition at %s', url)
    body = {
        "dashboard": dashboard,
        "overwrite": True
    }
    response = requests.post(url,
                             json=body,
                             auth=HTTPBasicAuth(username=config.grafana_user,
                                                password=config.grafana_password),
                             headers={
                                 'Content-Type': 'application/json',
                                 'Accept': 'application/json'
                             })
    response.raise_for_status()
    logger.debug('Grafana response %s', response.json())


if __name__ == '__main__':
    configure_script_logging(logger)
    main()
