import logging

import time

import boto3
import botocore
import docker

logger = logging.getLogger('S3TestCaseMixin')


class S3TestCaseMixin:
    _s3_container = None
    _s3_container_fake_access_key = 'happyWhale'
    _s3_container_fake_access_secret = 'happyMoose'
    _s3_client = None

    @classmethod
    def start_s3_server(cls):
        api_container_port = '9000/tcp'
        container_options = {
            'command': 'server /tmp',
            'detach': True,
            'auto_remove': True,
            'ports': {api_container_port: ('127.0.0.1', None)},
            'environment': (f'MINIO_ACCESS_KEY={cls._s3_container_fake_access_key}',
                            f'MINIO_SECRET_KEY={cls._s3_container_fake_access_secret}')
        }

        docker_client = docker.from_env()

        pretest_number_of_running_containers = len(docker_client.containers.list())

        cls._s3_container = docker_client.containers.run('minio/minio', **container_options)

        started_waiting_at = time.time()

        while pretest_number_of_running_containers >= len(docker_client.containers.list()):
            if time.time() - started_waiting_at >= 60:
                raise RuntimeError('Minio container takes too long to start up.')

            logger.info('Waiting for Minio...')
            time.sleep(1)

        logger.info(f'Minio started! ({time.time() - started_waiting_at:.3f}s)')

        container_info = docker_client.api.inspect_container(cls._s3_container.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]

        endpoint_url = f'http://{container_port["HostIp"]}:{container_port["HostPort"]}'

        cls._s3_client = boto3.client('s3', endpoint_url=endpoint_url,
                                      aws_access_key_id=cls._s3_container_fake_access_key,
                                      aws_secret_access_key=cls._s3_container_fake_access_secret,
                                      region_name='us-east-1',
                                      config=botocore.client.Config(signature_version='s3v4'))
        cls._s3_client.create_bucket(Bucket='samples')

    @classmethod
    def stop_s3_server(cls):
        cls._s3_container.kill()

        cls._s3_container = None

    @classmethod
    def s3_client(cls):
        return cls._s3_client