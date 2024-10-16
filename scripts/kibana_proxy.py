"""
Runs Kibana, Cerebro and aws-signing-proxy locally. The latter is used to sign
requests by the former and forward them to an Amazon Elasticsearch domain. The
default domain is the one configured for current Azul deployment.

Requires docker to be installed.

Before using this script, make sure that

 * the Azul virtualenv is active,

 * the desired deployment selected

 * and `source environment` has been run.

Then run

   kibana_proxy.py

and look for a log message starting in 'Now open'. Open the URL referred to by
that log message  in your browser while leaving this script running. Hit Ctrl-C
twice to terminate this script and the containers it launches.
"""
from concurrent.futures import (
    as_completed,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
from functools import (
    partial,
)
import logging
import os
import sys
import time

import docker
import docker.errors
from docker.models.containers import (
    Container,
)

from azul import (
    cached_property,
)
from azul.deployment import (
    aws,
)
from azul.docker import (
    resolve_docker_image_for_launch,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


class KibanaProxy:

    def __init__(self, options) -> None:
        self.options = options
        self.docker = docker.from_env()

    def create_container(self, image: str, *args, **kwargs) -> Container:
        try:
            container = self.docker.containers.create(image, *args, **kwargs)
        except docker.errors.ImageNotFound:
            log.info('Pulling image %r', image)
            self.docker.images.pull(image)
            container = self.docker.containers.create(image, *args, **kwargs)
        return container

    def run(self):
        # aws-signing-proxy doesn't support credentials
        creds = aws.boto3_session.get_credentials().get_frozen_credentials()
        kibana_port = self.options.kibana_port
        cerebro_port = self.options.cerebro_port or kibana_port + 1
        proxy_port = self.options.proxy_port or kibana_port + 2
        containers = []
        try:
            if self.options.local_port is None:
                image = resolve_docker_image_for_launch('_signing_proxy')
                proxy = self.create_container(image=image,
                                              name='proxy',
                                              auto_remove=True,
                                              command=['-target', self.es_endpoint, '-port', str(proxy_port)],
                                              detach=True,
                                              environment={
                                                  'AWS_ACCESS_KEY_ID': creds.access_key,
                                                  'AWS_SECRET_ACCESS_KEY': creds.secret_key,
                                                  'AWS_SESSION_TOKEN': creds.token,
                                                  'AWS_REGION': os.environ['AWS_DEFAULT_REGION']
                                              },
                                              ports={port: port for port in (kibana_port, cerebro_port, proxy_port)})
                containers.append(proxy)
                es_port = proxy_port
                network_mode = f'container:{proxy.name}'
            else:
                proxy = None
                es_port = self.options.local_port
                network_mode = 'host'
            image = resolve_docker_image_for_launch('_kibana')
            kibana = self.create_container(image=image,
                                           name='kibana',
                                           auto_remove=True,
                                           detach=True,
                                           environment={
                                               'KIBANA_ELASTICSEARCH_URL': f'http://localhost:{es_port}',
                                               'KIBANA_PORT_NUMBER': kibana_port
                                           },
                                           network_mode=network_mode)
            containers.append(kibana)
            image = resolve_docker_image_for_launch('_cerebro')
            cerebro = self.create_container(image=image,
                                            name='cerebro',
                                            auto_remove=True,
                                            command=[f'-Dhttp.port={cerebro_port}'],
                                            detach=True,
                                            network_mode=network_mode)
            containers.append(cerebro)

            def start_containers():
                if proxy is not None:
                    proxy.start()
                    time.sleep(1)
                kibana.start()
                cerebro.start()

            def handle_container(container):
                while container.status not in ('running', 'exited'):
                    container.reload()
                    time.sleep(0.1)
                for buf in container.logs(stream=True):
                    for line in buf.decode().splitlines():
                        log.info('%s: %s', container.name, line)

            def print_instructions():
                time.sleep(10)
                log.info('Now open Kibana at http://127.0.0.1:%i/ or Cerebro at '
                         'http://127.0.0.1:%i/#!/overview?host=http://localhost:%i/',
                         kibana_port, cerebro_port, es_port)

            tasks = [
                start_containers,
                print_instructions,
                *((partial(handle_container, c)) for c in containers)
            ]
            with ThreadPoolExecutor(max_workers=len(tasks)) as tpe:
                futures = list(map(tpe.submit, tasks))
                for f in as_completed(futures):
                    assert f.result() is None
        finally:
            for container in containers:
                container.kill()

    @cached_property
    def es_endpoint(self):
        log.info('Getting domain endpoint')
        es = aws.es
        domain = es.describe_elasticsearch_domain(DomainName=self.options.domain)
        return 'https://' + domain['DomainStatus']['Endpoints']['vpc']


def main(argv):
    import argparse
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--kibana-port', '-k', metavar='PORT', default=5601, type=int,
                     help='The port Kibana should be listening on.')
    cli.add_argument('--cerebro-port', '-c', metavar='PORT', type=int,
                     help='The port Cerebro should be listening on. '
                          'The default is the Kibana port plus 1.')
    cli.add_argument('--proxy-port', '-p', metavar='PORT', type=int,
                     help='The port the AWS signing proxy should be listening on. '
                          'The default is the Kibana port plus 2.')
    cli.add_argument('--domain', '-d', metavar='DOMAIN', default=os.environ.get('AZUL_ES_DOMAIN'),
                     help='The AWS Elasticsearch domain to use.')
    cli.add_argument('--local-port', '-l', metavar='PORT', type=int,
                     help='Configure Kibana to connect to an ES container running on the local'
                          'machine at the specified port. This disables the signing proxy.')
    options = cli.parse_args(argv)
    if not options.domain:
        raise RuntimeError('Please pass --domain or set AZUL_ES_DOMAIN')
    KibanaProxy(options).run()


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
