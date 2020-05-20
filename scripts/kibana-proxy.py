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

from itertools import chain
import logging
import os
import shlex
import signal
import sys
import time

import boto3

from azul.logging import configure_script_logging

log = logging.getLogger(__name__)


class KibanaProxy:

    def __init__(self, options) -> None:
        self.options = options
        self.pids = {}

    def run(self):
        # aws-signing-proxy doesn't support credentials
        creds = boto3.Session().get_credentials().get_frozen_credentials()
        kibana_port = self.options.kibana_port
        proxy_port = self.options.proxy_port or kibana_port + 10
        try:
            self.spawn('docker', 'run', '--rm', '-t',
                       '--name', f'aws-signing-proxy-{kibana_port}',
                       '-p', f'127.0.0.1:{proxy_port}:{proxy_port}',
                       '-p', f'127.0.0.1:{kibana_port}:{kibana_port}',
                       '-e', f'AWS_ACCESS_KEY_ID={creds.access_key}',
                       '-e', f'AWS_SECRET_ACCESS_KEY={creds.secret_key}',
                       '-e', f'AWS_SESSION_TOKEN={creds.token}',
                       '-e', 'AWS_REGION',
                       'cllunsford/aws-signing-proxy',
                       '-target', self.dss_end_point,
                       '-port', str(proxy_port),
                       AWS_REGION=os.environ['AWS_DEFAULT_REGION'])
            time.sleep(3)
            self.spawn('docker', 'run', '--rm', '-t',
                       '--network', f'container:aws-signing-proxy-{kibana_port}',
                       'kibana:5.5.2',
                       '--port', str(kibana_port),
                       '--elasticsearch', f'http://localhost:{proxy_port}')
            time.sleep(3)
            print(f'Now open https://localhost:{kibana_port}/')
            self.wait()
        finally:
            self.kill()

    @property
    def dss_end_point(self):
        log.info('Getting domain endpoint')
        es = boto3.client('es')
        domain = es.describe_elasticsearch_domain(DomainName=self.options.domain)
        return 'https://' + domain['DomainStatus']['Endpoint']

    def spawn(self, *args: str, **env: str):
        logged_command = ' '.join(chain(
            (k + '=' + shlex.quote(v) for k, v in env.items()),
            map(shlex.quote, args)))
        log.info('Running %s', logged_command)
        pid = os.spawnvpe(os.P_NOWAIT, args[0], list(args), env={**os.environ, **env})
        self.pids[pid] = logged_command

    def wait(self):
        while self.pids:
            pid, status = os.waitpid(-1, 0)
            args = self.pids.pop(pid)
            raise Exception(f'Exited: {args}')

    def kill(self):
        for pid, args in self.pids.items():
            log.info('Terminating: %s', args)
            os.kill(pid, signal.SIGINT)
            os.waitpid(pid, 0)


def main(argv):
    import argparse
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--kibana-port', '-p', metavar='PORT', default=5601, type=int,
                     help="The port Kibana should be listening on.")
    cli.add_argument('--proxy-port', '-P', metavar='PORT', type=int,
                     help="The port the proxy should be listening on. The default is the Kibana port plus 10.")
    cli.add_argument('--domain', '-d', metavar='DOMAIN', default=os.environ.get('AZUL_ES_DOMAIN'),
                     help="The AWS Elasticsearch domain to use.")
    options = cli.parse_args(argv)
    if not options.domain:
        raise RuntimeError('Please pass --domain or set AZUL_ES_DOMAIN')
    KibanaProxy(options).run()


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
