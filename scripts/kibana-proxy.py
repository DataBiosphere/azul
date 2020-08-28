#!/usr/bin/env python
"""
Runs Kibana and aws-signing-proxy locally. The latter is used to sign requests by the former and forward them to an
Amazon Elasticsearch instance. The default instance is the main ES instance for the current DSS stage.

Requires docker to be installed.

Before using this script, make sure that

 * the Azul virtualenv is active,

 * the desired deployment selected

 * and `source environment` has been run.

Then run

   kibana-proxy.py

and open `http://localhost:5601` in your browser while leaving the script running. Hitting Ctrl-C terminates it and
its child processes.
"""
import logging
import os
import sys
import time

import boto3
import docker

from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


class KibanaProxy:

    def __init__(self, options) -> None:
        self.options = options
        self.containers = []

    def run(self):
        # aws-signing-proxy doesn't support credentials
        creds = boto3.Session().get_credentials().get_frozen_credentials()
        kibana_port = self.options.kibana_port
        cerebro_port = self.options.cerebro_port or kibana_port + 1
        proxy_port = self.options.proxy_port or kibana_port + 2
        client = docker.from_env()
        try:
            proxy = client.containers.run('cllunsford/aws-signing-proxy',
                                          auto_remove=True,
                                          command=['-target', self.dss_end_point, '-port', str(proxy_port)],
                                          detach=True,
                                          environment={
                                              'AWS_ACCESS_KEY_ID': creds.access_key,
                                              'AWS_SECRET_ACCESS_KEY': creds.secret_key,
                                              'AWS_SESSION_TOKEN': creds.token,
                                              'AWS_REGION': os.environ['AWS_DEFAULT_REGION']
                                          },
                                          ports={port: port for port in (kibana_port, cerebro_port, proxy_port)},
                                          tty=True)
            self.containers.append(proxy)
            time.sleep(3)
            kibana = client.containers.run('docker.elastic.co/kibana/kibana-oss:6.8.0',
                                           auto_remove=True,
                                           detach=True,
                                           environment={
                                               'ELASTICSEARCH_HOSTS': f'http://localhost:{proxy_port}',
                                               'SERVER_PORT': kibana_port
                                           },
                                           network_mode=f'container:{proxy.name}',
                                           tty=True)
            self.containers.append(kibana)
            # 0.9.1 does not work against ES 6.8.0
            cerebro = client.containers.run('lmenezes/cerebro:0.8.5',
                                            auto_remove=True,
                                            command=[f'-Dhttp.port={cerebro_port}'],
                                            detach=True,
                                            network_mode=f'container:{proxy.name}',
                                            tty=True)
            self.containers.append(cerebro)
            time.sleep(10)
            print(f'Now open Kibana at http://127.0.0.1:{kibana_port}/ and open Cerebro '
                  f'at http://127.0.0.1:{cerebro_port}/#/overview?host=http://localhost:'
                  f'{proxy_port} (or paste in http://localhost:{proxy_port})')
            for container in self.containers:
                container.wait()
        except KeyboardInterrupt:
            pass
        finally:
            for container in self.containers:
                container.kill()

    @property
    def dss_end_point(self):
        log.info('Getting domain endpoint')
        es = boto3.client('es')
        domain = es.describe_elasticsearch_domain(DomainName=self.options.domain)
        return 'https://' + domain['DomainStatus']['Endpoint']


def main(argv):
    import argparse
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--kibana-port', '-k', metavar='PORT', default=5601, type=int,
                     help="The port Kibana should be listening on.")
    cli.add_argument('--cerebro-port', '-c', metavar='PORT', type=int,
                     help="The port Cerebro should be listening on. "
                          "The default is the Kibana port plus 1.")
    cli.add_argument('--proxy-port', '-p', metavar='PORT', type=int,
                     help="The port the AWS signing proxy should be listening on. "
                          "The default is the Kibana port plus 2.")
    cli.add_argument('--domain', '-d', metavar='DOMAIN', default=os.environ.get('AZUL_ES_DOMAIN'),
                     help="The AWS Elasticsearch domain to use.")
    options = cli.parse_args(argv)
    if not options.domain:
        raise RuntimeError('Please pass --domain or set AZUL_ES_DOMAIN')
    KibanaProxy(options).run()


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
