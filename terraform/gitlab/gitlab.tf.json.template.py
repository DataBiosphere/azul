from collections.abc import (
    Iterable,
)
import gzip
from itertools import (
    chain,
)
import json
import os
from typing import (
    Union,
)

import yaml

from azul import (
    config,
    lru_cache,
)
from azul.aws_service_model import (
    ServiceActionType,
)
from azul.chalice import (
    private_api_policy,
)
from azul.collections import (
    dict_merge,
    explode_dict,
)
from azul.deployment import (
    aws,
)
from azul.strings import (
    departition,
)
from azul.terraform import (
    emit_tf,
    vpc,
)
from azul.types import (
    JSON,
)

# This Terraform config creates a single EC2 instance with a bunch of Docker
# containers running on it:
#
#                  ╔═══════════════════════════════════════════════════════════════════════════════════════╗
#                  ║                                        gitlab                                         ║
#                  ║ ┏━━━━━━━━━━━━━━━━━━━┓ ┏━━━━━━━━━━━━━━━━━━━┓ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ║
#         ┌─────┐  ║ ┃      gitlab       ┃ ┃   gitlab-runner   ┃ ┃              gitlab-dind              ┃ ║
#         │ ALB │  ║ ┃       ┌─────────┐ ┃ ┃ ┌───────────────┐ ┃ ┃        ┌────────────┐                 ┃ ║
# ┌──▶ 443├ ─ ─ ┼──╬─╋───▶ 80│  nginx  │ ┃ ┃ │ gitlab-runner ├─╋─╋──▶ 2375│  dockerd   │docker.sock ◀──┐ ┃ ║
# │       └─────┘  ║ ┃       └─────────┘ ┃ ┃ └───────┬───┬───┘ ┃ ┃        └────────────┘               │ ┃ ║
# │       ┌─────┐  ║ ┃       ┌─────────┐ ┃ ┗━━━━━━━━━╋━━━━━━━━━┛ ┃        ┌────────────┐               │ ┃ ║
# │     22├ ─ ─ ┼──╬─╋─▶ 2222│  sshd   │ ┃       ▲   │   │       ┃        │ containerd │               │ ┃ ║
# │       │     │  ║ ┃       └─────────┘ ┃           │           ┃        └────────────┘               │ ┃ ║
# │       │     │  ║ ┃       ┌─────────┐ ┃       │   │   │       ┃      ┏━━━━━━━━━━━━━━━━┓             │ ┃ ║
# │       │     │  ║ ┃       │  rails  │─┃─ ─ ─ ─    │           ┃      ┃    'build'     ┃             │ ┃ ║
# │       │     │  ║ ┃       └─────────┘ ┃           │   │       ┃      ┃ ┌────────────┐ ┃             │ ┃ ║
# │       │     │  ║ ┃       ┌─────────┐ ┃           │           ┃      ┃ │    make    │ ┃             │ ┃ ║
# │       │ NLB │  ║ ┃       │postgres │ ┃           │   └ ─ ─ ─ ╋ ─ ─ ▶┃ └────────────┘ ┃             │ ┃ ║
# │       │     │  ║ ┃       └─────────┘ ┃           │           ┃      ┃ ┌────────────┐ ┃             │ ┃ ║
# │       │     │  ║ ┗━━━━━━━━━━━━━━━━━━━┛           │           ┃ ┌────╋─┤   python   ├─╋─────────────┘ ┃ ║
# │       │     │  ║ ┏━━━━━━━━━━━━━━━━━━━┓           │           ┃ │    ┃ └──────┬─────┘ ┃               ┃ ║
# │       │     │  ║ ┃      console      ┃           │           ┃ │    ┗━━━━━━━━━━━━━━━━┛               ┃ ║
# │       │     │  ║ ┃       ┌─────────┐ ┃           │           ┃ │             └ ─ ─ ─ ─ ─ ─ ─ ┐       ┃ ║
# │   2222├ ─ ─ ┼──╬─╋───▶ 22│  sshd   │ ┃           │           ┃ │ ┏━━━━━━━━━━━━━━━━━━━━━━┓            ┃ ║
# │       └─────┘  ║ ┃       └─────────┘ ┃           │           ┃ │ ┃    elasticsearch     ┃    │       ┃ ║
# │                ║ ┗━━━━━━━━━━━━━━━━━━━┛           │           ┃ │ ┃       ┌────────────┐ ┃            ┃ ║
# └────────────────╬─────────────────────────────────┘           ┃ ├─╋─▶ 9200│    java    │ ┃◀ ─ ┤       ┃ ║
#                  ║                                             ┃ │ ┃       └────────────┘ ┃            ┃ ║
#                  ║                                             ┃ │ ┗━━━━━━━━━━━━━━━━━━━━━━┛    │       ┃ ║
#                  ║                                             ┃ │ ┏━━━━━━━━━━━━━━━━━━━━━━┓            ┃ ║
#                  ║                                             ┃ │ ┃    dynamodb-local    ┃    │       ┃ ║
#                  ║                                             ┃ │ ┃       ┌────────────┐ ┃            ┃ ║
#                  ║                                             ┃ └─╋─▶ 8000│    java    │ ┃◀ ─ ┘       ┃ ║
#                  ║                                             ┃   ┃       └────────────┘ ┃            ┃ ║
#                  ║                                             ┃   ┗━━━━━━━━━━━━━━━━━━━━━━┛            ┃ ║
#                  ║                                             ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
#                  ╚═══════════════════════════════════════════════════════════════════════════════════════╝
#
#                  ╔══════════╗ ┏━━━━━━━━━━━┓ ┌─────────┐
#                  ║ instance ║ ┃ container ┃ │ process │    ──────interact──────▶    ─ ─ ─ ─invoke ─ ─ ─ ▶
#                  ╚══════════╝ ┗━━━━━━━━━━━┛ └─────────┘
#
# The instance is fronted by two AWS load balancers:
#
# 1) an application load balancer (ALB) that terminates SSL and forwards to the
#    Gitlab web UI
#
# 2) an network load balancer that forwards port 22 to an SSH daemon in the
#    Gitlab container (for git+ssh://) and port 2222 to an SSH daemon for shell
#    access in RancherOS' `console` container.
#
# The instance itself does not have a public IP and is only reachable from the
# internet through the load balancers.
#
# The NLB's public IP is bound to ssh.gitlab.
# {dev,prod}.singlecell.gi.ucsc.edu The ALB's public IP is bound to gitlab.
# {dev,prod}.singlecell.gi.ucsc.edu To log into the instance run `ssh
# rancher@ssh.gitlab.dev.singlecell.gi.ucsc.edu -p 2222`. Your SSH key must be
# mentioned in public_key or other_public_keys below.
#
# The Gitlab web UI is at https://gitlab.{dev,prod}.singlecell.gi.ucsc.edu/. It
# is safe to destroy all resources in this TF config. You can always build them
# up again. The only golden egg is the EBS volume that's attached to the
# instance. See below under ebs_volume_name. RancherOS was chosen for the AMI
# because it has Docker pre installed and supports cloud-init user data.
#
# The container wiring is fairly complicated as it involves docker-in-docker. It
# is inspired by
#
# https://medium.com/@tonywooster/docker-in-docker-in-gitlab-runners-220caeb708ca
#
# In this setup the build container is not privileged while allowing for image
# layer caching between builds. The `elasticsearch` and `dynamodb-local`
# containers are included as examples of test fixtures launched during test
# setup. This aspect may evolve over time. It's worth noting that these fixture
# containers are siblings of the build container. When the tests are run
# locally or on Travis, the tests run on the host. The above diagram also
# glosses over the fact that there are multiple separate bridge networks
# involved. The `gitlab-dind` and `gitlab-runner` containers are attached to a
# separate bridge network. The `gitlab` container is on the default bridge
# network. IMPORTANT: There is a bug in the Terraform AWS provider (I think
# it's conflating the listeners) which causes one of the NLB listeners to be
# missing after `terraform apply`.

# The name of an EBS volume to attach to the instance. This EBS volume must
# exist and be formatted with ext4. We don't manage the volume in Terraform
# because that would require formatting it once after creation. That can only
# be one after attaching it to an EC2 instance but before mounting it. This
# turns out to be difficult and risks overwriting existing data on the volume.
# We'd also have to prevent the volume from being deleted during `terraform
# destroy`.
#
# If this EBS volume does not exist you must create it with the desired size
# before running Terraform. For example:
#
# aws ec2 create-volume \
# --size 100 \
# --availability-zone "${AWS_DEFAULT_REGION}a" \
# --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=azul-gitlab},{Key=owner,Value=hannes@ucsc.edu}]'
#
# To then format the volume, you can then either attach it to some other Linux
# instance and format it there or use `make terraform` to create the actual
# Gitlab instance and attach the volume. For the latter you would need to ssh
# into the Gitlab instance, format `/dev/xvdf` (`/dev/nvme1n1` on newer
# instance types) and reboot the instance. For example:
#
# docker stop gitlab-runner
# docker stop gitlab
# docker stop gitlab-dind
# sudo mv /mnt/gitlab /mnt/gitlab.deleteme
# sudo mkdir /mnt/gitlab
# sudo mkfs.ext4 /dev/nvme1n1
# sudo reboot
# sudo rm -rf /mnt/gitlab.deleteme
#
# The EBS volume should be backed up (EBS snapshot) periodically. Not only does
# it contain Gitlab's data but also its config.
#
ebs_volume_name = 'azul-gitlab'

num_zones = 2  # An ALB needs at least two availability zones

# List of port forwardings by the network load balancer (NLB). The first element
# in the tuple is the port on the external interface of the NLB, the second
# element is the port on the instance the NLB forwards to.
#
nlb_ports = [(22, 2222, 'git'), (2222, 22, 'ssh')]

# The Azul Gitlab instance uses one VPC. This variable specifies the IPv4
# address block to be used by that VPC.
#
# Be sure to avoid the default Docker address pool:
#
# https://github.com/docker/libnetwork/blob/a79d3687931697244b8e03485bf7b2042f8ec6b6/ipamutils/utils.go#L10
#

cidr_offset = ['dev', 'prod', 'anvildev', 'anvilprod'].index(config.deployment_stage)

vpc_cidr = f'172.{71 + cidr_offset}.0.0/16'

vpn_subnet = f'10.{42 + cidr_offset}.0.0/16'  # can't overlap VPC CIDR and subnet mask must be <= 22 bits

# The name of the SSH keypair whose public key is to be deposited on the
# instance by AWS
#
key_name = 'hannes@ucsc.edu'

# The public key of that keypair
#
public_key = (
    'ssh-rsa'
    ' '
    'AAAAB3NzaC1yc2EAAAADAQABAAABAQDhRBbejN2qT5+6nfpzxPTfTFuSDSiPrAyDKH+V/A9+Xw4ZT8Z3K4d0w0KlwjtRZ'
    '7shmIxkN44DY8R8LGCiybYHHVHqRNoQYqY1BkfSSP8h+eTylo4kRE4hKzs97dsBKYN1iXYXxd9yJGf6u3iR51LFijNLNN'
    '6QEsxC6PhBReye21X8KdrlOO1owG3D+BVF6Q8PxpBFTjwMLiJUe3hm/vNTrCJErtHAr6ok28BY7rj3UVbGscrnsMIpdsX'
    'OFDl5NU7tB6H9HlQ46l/W70ZSpzx8FQel9kbxcjZLinmsujuILC2bI1ev4EcdTRXo9SHo5VLPnE9J2f6StlqbBYJpbdOl'
    ' '
    'hannes@ucsc.edu'
)

other_public_keys = {
    'dev': [
        (
            'ssh-rsa'
            ' '
            'AAAAB3NzaC1yc2EAAAADAQABAAABgQCrIU25zlzHBxIdEATJZsGXvatdWuen5zlOw1uE25spQ8eNnOUfbz5fR'
            'yiQqyMNxE/dX2hCCDT1mr5Flke4uJ0FayC/l5ZC3bKYE2gnILbZBNsFuueZuDy9pRmZ+eTYs3vKXN361+loRi'
            '6ag8h/pOQCvx6oO5NrVSBse0NcEn1tk1h7C1hOf8sblW17+OO9aDQJAA7G4PJw2kBRCYYEwDNLBRy3k1wBdcK'
            'G2t2SuVh+PCpmMPA5/i/raDUqATO1H3bcRubtyGHNbAtihL5HLZK83O9fHVf/MD7il4N/9OwBNpOwvc2gi9zp'
            'ChGpbl5jA2ZfoEDEOhX4ffOD1UwmkmkoUC82BvHyAwdnqgh3Nk4qCum53TsMhXVWMW/8tr/t+AxjE3/Acwj6H'
            'VMz2j+67A0p1oaTbxBXdf00BmAYV2xPZNg8Fa2/AkQWPt4c4JJnktVjWM8/PU1h6FamyHfQ6pNmi+j6rHz9UZ'
            'e1Zt6WybGr+Tt+KifhbCnZQkg74I1uT6M='
            ' '
            'achave11@ucsc.edu'
        ),
    ],
    'prod': []
}

# AWS accounts we trust enough to assume roles in
#
friend_accounts = {
    861229788715: 'hca-dev',
    109067257620: 'hca-prod',
    122796619775: 'platform-hca-dev',
    542754589326: 'platform-hca-prod'
}

logs_path_prefix = 'logs/alb'
gitlab_logs_path = f'{logs_path_prefix}/AWSLogs/{aws.account}/*'

# An ELB account ID, which varies depending on region, is needed to specify a
# principal in log bucket policy.
#
# https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-logging-bucket-permissions
#
elb_region_account_id = {
    'us-east-1': '127311923021',
    'us-east-2': '033677994240',
    'us-west-1': '027434742980',
    'us-west-2': '797873946194',
    'af-south-1': '098369216593',
    'ca-central-1': '985666609251',
    'eu-central-1': '054676820928',
    'eu-west-1': '156460612806',
    'eu-west-2': '652711504416',
    'eu-south-1': '635631232127',
    'eu-west-3': '009996457667',
    'eu-north-1': '897822967062',
    'ap-east-1': '754344448648',
    'ap-northeast-1': '582318560864',
    'ap-northeast-2': '600734575887',
    'ap-northeast-3': '383597477331',
    'ap-southeast-1': '114774131450',
    'ap-southeast-2': '783225319266',
    'ap-south-1': '718504428378',
    'me-south-1': '076674570225',
    'sa-east-1': '507241528517'
}


@lru_cache(maxsize=1)
def iam() -> JSON:
    with gzip.open(os.path.join(os.path.dirname(__file__), 'aws_service_model.json.gz'), 'rt') as f:
        return json.load(f)


def aws_service_actions(service: str, types: set[ServiceActionType] = None, is_global: bool = None) -> list[str]:
    if types is None and is_global is None:
        return [iam()['services'][service]['serviceName'] + ':*']
    else:
        actions = iam()['actions'][service]
        return [
            name
            for name, action in actions.items()
            if (
                (types is None or ServiceActionType[action['type']] in types)
                and (is_global is None or bool(action['resources']) == (not is_global))
            )
        ]


def aws_service_arns(service: str, *resource_names: str, **arn_fields: str) -> list[str]:
    resources = iam()['resources'].get(service, {})
    resource_names = set(resource_names)
    all_names = resources.keys()
    invalid_names = resource_names.difference(all_names)
    assert not invalid_names, f'No such resource in {service}: {invalid_names}'
    arn_fields = {
        'Account': aws.account,
        'Region': aws.region_name,
        **arn_fields
    }
    arns = []
    for arn_fields in explode_dict(arn_fields):
        for name, arn in resources.items():
            if not resource_names or name in resource_names:
                arn = arn.replace('${', '{')
                arn = arn.format_map(arn_fields)
                arns.append(arn)
    return arns


def merge(sets: Iterable[Iterable[str]]) -> Iterable[str]:
    return sorted(set(chain(*sets)))


def allow_global_actions(service, types: set[ServiceActionType] = None) -> JSON:
    return {
        'actions': aws_service_actions(service, types=types, is_global=True),
        'resources': ['*']
    }


def allow_service(service: str,
                  *resource_names: str,
                  action_types: set[ServiceActionType] = None,
                  global_action_types: set[ServiceActionType] = None,
                  **arn_fields: Union[str, list[str], set[str], tuple[str, ...]]) -> list[JSON]:
    if global_action_types is None:
        global_action_types = action_types
    return remove_inconsequential_statements([
        allow_global_actions(service, types=global_action_types),
        {
            'actions': aws_service_actions(service, types=action_types),
            'resources': aws_service_arns(service, *resource_names, **arn_fields)
        }
    ])


def remove_inconsequential_statements(statements: list[JSON]) -> list[JSON]:
    return [s for s in statements if s['actions'] and s['resources']]


dss_direct_access_policy_statement = {
    'actions': [
        'sts:AssumeRole',
    ],
    'resources': [
        f'arn:aws:iam::{account}:role/azul-*'
        for account in friend_accounts.keys()
    ]
}

clamav_image = 'clamav/clamav:0.104'
dind_image = 'docker:20.10.18-dind'
gitlab_image = 'gitlab/gitlab-ce:15.3.3-ce.0'
runner_image = 'gitlab/gitlab-runner:v15.3.0'

# There are ways to dynamically determine the latest Amazon Linux AMI but in the
# spirit of reproducable builds we would rather pin the AMI and adopt updates at
# our own discretion so as to avoid unexpected failures due to AMI changes. To
# determine the latest AMI for Amazon Linux 2, I used the following commands:
#
# _select dev.gitlab
# aws ssm get-parameters \
#   --names \
#       $(aws ssm get-parameters-by-path \
#           --path /aws/service/ami-amazon-linux-latest \
#           --query "Parameters[].Name" \
#       | jq -r .[] \
#       | grep -F amzn2 \
#       | grep -Fv minimal \
#       | grep -Fv kernel-5.10 \
#       | grep -F x86_64 \
#       | grep -F ebs) \
# | jq -r .Parameters[].Value
#
# The AMI ID is specific to a region. If there are ….gitlab components in more
# than one AWS region, you need to select at least one ….gitlab component in
# each of these regions, rerun the command for each such component and add or
# update the entry for the respective region in the dictionary literal below.
#
ami_id = {
    'us-east-1': 'ami-0bb85ecb87fe01c2f'
}


def jw(*words):
    return ' '.join(words)


def jl(*lines):
    return '\n'.join(lines)


def qq(*words):
    return '"' + jw(*words) + '"'


emit_tf({} if config.terraform_component != 'gitlab' else {
    'data': {
        'aws_availability_zones': {
            'available': {}
        },
        'aws_acm_certificate': {
            'gitlab_vpn': {
                'domain': 'azul-gitlab-vpn-server-' + config.deployment_stage
            }
        },
        'aws_ebs_volume': {
            'gitlab': {
                'filter': [
                    {
                        'name': 'volume-type',
                        'values': ['gp2']
                    },
                    {
                        'name': 'tag:Name',
                        'values': [ebs_volume_name]
                    }
                ],
                'most_recent': True
            }
        },
        # This Route53 zone also has to exist.
        'aws_route53_zone': {
            'gitlab': {
                'name': config.domain_name + '.',
                'private_zone': False
            }
        },
        'aws_iam_policy_document': {
            # This policy is really close to the policy size limit, if you get
            # LimitExceeded: Cannot exceed quota for PolicySize: 6144, you need
            # to strip the existing policy down by essentially replacing the
            # calls to the helper functions like allow_service() with a
            # hand-curated list of actions, potentially by starting from a copy
            # of the template output.
            'gitlab_boundary': {
                'statement': [
                    allow_global_actions('S3', types={ServiceActionType.read, ServiceActionType.list}),
                    {
                        'actions': aws_service_actions('S3'),
                        'resources': merge(
                            aws_service_arns('S3', BucketName=bucket_name, ObjectName='*')
                            for bucket_name in (
                                [
                                    'edu-ucsc-gi-singlecell-azul-*',
                                    '*.url.singlecell.gi.ucsc.edu',
                                    'url.singlecell.gi.ucsc.edu',
                                ] if 'singlecell' in config.domain_name else [
                                    'edu-ucsc-gi-platform-anvil-dev.*',
                                    'edu-ucsc-gi-platform-anvil-dev-*',
                                    'url.*.anvil.gi.ucsc.edu',
                                    'url.anvil.gi.ucsc.edu',
                                    'edu-ucsc-gi-platform-anvil-anvilbox',
                                ] if 'anvil' in config.domain_name else [
                                    'edu-ucsc-gi-azul-*',
                                    '*.azul.data.humancellatlas.org',
                                ]
                            )
                        )
                    },

                    *allow_service('KMS',
                                   action_types={ServiceActionType.read, ServiceActionType.list},
                                   KeyId='*',
                                   Alias='*'),

                    *allow_service('SQS',
                                   QueueName='azul-*'),

                    # API Gateway ARNs refer to APIs by ID so we cannot restrict
                    # to name or prefix. Even though all API Gateway ARNs start
                    # with `arn:aws:apigateway:${Region}::`, using the pattern
                    # `arn:aws:apigateway:${Region}::*` in the policy causes the
                    # CreateDomainName action to return AccessDenied for unknown
                    # reasons. Other API Gateway actions succeed with that ARN
                    # pattern in the policy.
                    {
                        'actions': ['apigateway:*'],
                        'resources': ['*']
                    },

                    *allow_service('Elasticsearch Service',
                                   global_action_types={ServiceActionType.read, ServiceActionType.list},
                                   DomainName='azul-*'),
                    {
                        'actions': ['es:ListTags'],
                        'resources': aws_service_arns('Elasticsearch Service', DomainName='*')
                    },

                    *allow_service('STS',
                                   action_types={ServiceActionType.read, ServiceActionType.list},
                                   RelativeId='*',
                                   RoleNameWithPath='*',
                                   UserNameWithPath='*'),

                    dss_direct_access_policy_statement,

                    *allow_service('Certificate Manager',
                                   # ACM ARNs refer to certificates by ID so we
                                   # cannot restrict to name or prefix
                                   CertificateId='*',
                                   # API Gateway certs must reside in us-east-1,
                                   # so we'll always add that region
                                   Region={aws.region_name, 'us-east-1'}),

                    *allow_service('DynamoDB',
                                   'table',
                                   'index',
                                   global_action_types={ServiceActionType.list, ServiceActionType.read},
                                   TableName='azul-*',
                                   IndexName='*'),

                    # Lambda ARNs refer to event source mappings by UUID so we
                    # cannot restrict to name or prefix
                    *allow_service('Lambda',
                                   LayerName='azul-*',
                                   FunctionName='azul-*',
                                   UUID='*',
                                   LayerVersion='*'),

                    *private_api_policy(for_tf=True),

                    # CloudWatch does not describe any resource-level
                    # permissions
                    {
                        'actions': ['cloudwatch:*'],
                        'resources': ['*']
                    },

                    *allow_service('CloudWatch Events',
                                   global_action_types={ServiceActionType.list, ServiceActionType.read},
                                   RuleName='azul-*'),

                    # Route 53 ARNs refer to resources by ID so we cannot
                    # restrict to name or prefix
                    #
                    # FIXME: this is obviously problematic
                    {
                        'actions': ['route53:*'],
                        'resources': ['*']
                    },

                    # Secret Manager ARNs refer to secrets by UUID so we cannot
                    # restrict to name or prefix
                    #
                    # FIXME: this is obviously problematic
                    #
                    *allow_service('Secrets Manager', SecretId='*'),

                    {
                        'actions': ['ssm:GetParameter'],
                        'resources': aws_service_arns('Systems Manager',
                                                      'parameter',
                                                      FullyQualifiedParameterName='dcp/dss/*')
                    },

                    {
                        'actions': [
                            'states:*'
                        ],
                        'resources': aws_service_arns('Step Functions',
                                                      'execution',
                                                      'statemachine',
                                                      StateMachineName='azul-*',
                                                      ExecutionId='*')
                    },
                    {
                        'actions': [
                            'states:ListStateMachines',
                            'states:CreateStateMachine'
                        ],
                        'resources': [
                            '*'
                        ]
                    },

                    # CloudFront does not define any ARNs. We need it for
                    # friendly domain names for API Gateways
                    {
                        'actions': ['cloudfront:*'],
                        'resources': ['*']
                    },

                    # CloudWatch Logs
                    #
                    # FIXME: Tighten GitLab security boundary
                    #        https://github.com/DataBiosphere/azul/issues/4207
                    {
                        'actions': ['logs:*'],
                        'resources': ['*']
                    },

                    # WAFv2
                    {
                        'actions': ['wafv2:*'],
                        'resources': ['*']
                    }
                ]
            },
            'gitlab_iam': {
                'statement': [
                    # Let Gitlab manage roles as long as they specify the
                    # permissions boundary This prevent privilege escalation.
                    {
                        'actions': [
                            'iam:CreateRole',
                            'iam:TagRole',
                            'iam:UntagRole',
                            'iam:PutRolePolicy',
                            'iam:DeleteRolePolicy',
                            'iam:AttachRolePolicy',
                            'iam:DetachRolePolicy',
                            'iam:PutRolePermissionsBoundary'
                        ],
                        'resources': aws_service_arns('IAM', 'role', RoleNameWithPath='azul-*'),
                        'condition': {
                            'test': 'StringEquals',
                            'variable': 'iam:PermissionsBoundary',
                            'values': [aws.permissions_boundary_arn]
                        }
                    },

                    dss_direct_access_policy_statement,

                    {
                        'actions': [
                            'iam:UpdateAssumeRolePolicy',
                            'iam:TagRole',
                            'iam:DeleteRole',
                            'iam:PassRole'  # FIXME: consider iam:PassedToService condition
                        ],
                        'resources': aws_service_arns('IAM', 'role', RoleNameWithPath='azul-*')
                    },
                    {
                        'actions': aws_service_actions('IAM', types={ServiceActionType.read, ServiceActionType.list}),
                        'resources': ['*']
                    },
                    *(
                        # Permissions required to deploy Data Browser and
                        # Portal
                        [
                            {
                                'actions': [
                                    's3:PutObject',
                                    's3:GetObject',
                                    's3:ListBucket',
                                    's3:DeleteObject',
                                    's3:PutObjectAcl'
                                ],
                                'resources': [
                                    # Data Portal Dev
                                    'arn:aws:s3:::dev.singlecell.gi.ucsc.edu/*',
                                    'arn:aws:s3:::dev.singlecell.gi.ucsc.edu',
                                    # Data Browser Dev
                                    'arn:aws:s3:::dev.explore.singlecell.gi.ucsc.edu/*',
                                    'arn:aws:s3:::dev.explore.singlecell.gi.ucsc.edu',
                                    # Data Portal UX-Dev
                                    'arn:aws:s3:::ux-dev.singlecell.gi.ucsc.edu/*',
                                    'arn:aws:s3:::ux-dev.singlecell.gi.ucsc.edu',
                                    # Data Browser UX-Dev
                                    'arn:aws:s3:::ux-dev.explore.singlecell.gi.ucsc.edu/*',
                                    'arn:aws:s3:::ux-dev.explore.singlecell.gi.ucsc.edu',
                                    # Lungmap Data Portal Dev
                                    'arn:aws:s3:::data-browser.dev.lungmap.net/*',
                                    'arn:aws:s3:::data-browser.dev.lungmap.net',
                                    # Lungmap Data Browser Dev
                                    'arn:aws:s3:::dev.explore.lungmap.net/*',
                                    'arn:aws:s3:::dev.explore.lungmap.net'
                                ]
                            },
                            {
                                'actions': [
                                    'cloudfront:CreateInvalidation'
                                ],
                                'resources': [
                                    # dev.singlecell.gi.ucsc.edu
                                    'arn:aws:cloudfront::122796619775:distribution/E3562WJBOLN8W8',
                                    # ux-dev.singlecell.gi.ucsc.edu
                                    'arn:aws:cloudfront::122796619775:distribution/E3FFK49Z7TQ60R',
                                    # data-browser.dev.lungmap.net
                                    'arn:aws:cloudfront::122796619775:distribution/E21CJFOUWO9Q7X'
                                ]
                            }
                        ] if config.domain_name == 'dev.singlecell.gi.ucsc.edu' else [
                            {
                                'actions': [
                                    's3:PutObject',
                                    's3:GetObject',
                                    's3:ListBucket',
                                    's3:DeleteObject',
                                    's3:PutObjectAcl'
                                ],
                                'resources': [
                                    # HCA Data Portal Prod
                                    'arn:aws:s3:::org-humancellatlas-data-portal-dcp2-prod/*',
                                    'arn:aws:s3:::org-humancellatlas-data-portal-dcp2-prod',
                                    # HCA Data Browser Prod
                                    'arn:aws:s3:::org-humancellatlas-data-browser-dcp2-prod/*',
                                    'arn:aws:s3:::org-humancellatlas-data-browser-dcp2-prod',
                                    # Lungmap Data Browser Prod
                                    'arn:aws:s3:::data-browser.lungmap.net/*',
                                    'arn:aws:s3:::data-browser.lungmap.net',
                                    # Lungmap Data Portal Prod
                                    'arn:aws:s3:::data-browser.explore.lungmap.net/*',
                                    'arn:aws:s3:::data-browser.explore.lungmap.net'
                                ]
                            },
                            {
                                'actions': [
                                    'cloudfront:CreateInvalidation'
                                ],
                                'resources': [
                                    # data.humancellatlas.org
                                    'arn:aws:cloudfront::542754589326:distribution/E1LYQC3LZXO7M3',
                                    # data-browser.lungmap.net/
                                    'arn:aws:cloudfront::542754589326:distribution/E22L661MUAMMTD'
                                ]
                            }
                        ] if config.domain_name == 'azul.data.humancellatlas.org' else [
                        ]
                    ),
                    # Manage VPN infrastructure for private API
                    # FIXME: Tighten GitLab security boundary
                    #        https://github.com/DataBiosphere/azul/issues/4207
                    {
                        'actions': ['ec2:*'],
                        'resources': ['*']
                    },
                    {
                        'actions': ['elasticloadbalancing:*'],
                        'resources': ['*']
                    }
                ]
            }
        },
    },
    'resource': {
        'aws_vpc': {
            'gitlab': {
                'cidr_block': vpc_cidr
            }
        },
        'aws_subnet': {
            # A public and a private subnet per availability zone
            f'gitlab_{vpc.subnet_name(public)}_{zone}': {
                'availability_zone': f'${{data.aws_availability_zones.available.names[{zone}]}}',
                'cidr_block': f'${{cidrsubnet(aws_vpc.gitlab.cidr_block, 8, {vpc.subnet_number(zone, public)})}}',
                'map_public_ip_on_launch': public,
                'vpc_id': '${aws_vpc.gitlab.id}'
            }
            for public in (False, True)
            for zone in range(num_zones)
        },
        'aws_internet_gateway': {
            'gitlab': {
                'vpc_id': '${aws_vpc.gitlab.id}'
            }
        },
        'aws_route': {
            'gitlab': {
                'destination_cidr_block': '0.0.0.0/0',
                'gateway_id': '${aws_internet_gateway.gitlab.id}',
                'route_table_id': '${aws_vpc.gitlab.main_route_table_id}'
            }
        },
        'aws_eip': {
            f'gitlab_{zone}': {
                'depends_on': [
                    'aws_internet_gateway.gitlab'
                ],
                'vpc': True
            }
            for zone in range(num_zones)
        },
        'aws_nat_gateway': {
            f'gitlab_{zone}': {
                'allocation_id': f'${{aws_eip.gitlab_{zone}.id}}',
                'subnet_id': f'${{aws_subnet.gitlab_public_{zone}.id}}'
            }
            for zone in range(num_zones)
        },
        'aws_route_table': {
            f'gitlab_{zone}': {
                'route': [
                    {
                        'cidr_block': '0.0.0.0/0',
                        'nat_gateway_id': f'${{aws_nat_gateway.gitlab_{zone}.id}}',
                        'egress_only_gateway_id': None,
                        'gateway_id': None,
                        'instance_id': None,
                        'ipv6_cidr_block': None,
                        'network_interface_id': None,
                        'transit_gateway_id': None,
                        'vpc_peering_connection_id': None,
                        'carrier_gateway_id': None,
                        'destination_prefix_list_id': None,
                        'local_gateway_id': None,
                        'vpc_endpoint_id': None,
                        'core_network_arn': None,
                    }
                ],
                'vpc_id': '${aws_vpc.gitlab.id}'
            }
            for zone in range(num_zones)
        },
        'aws_route_table_association': {
            f'gitlab_{zone}': {
                'route_table_id': f'${{aws_route_table.gitlab_{zone}.id}}',
                'subnet_id': f'${{aws_subnet.gitlab_private_{zone}.id}}'
            }
            for zone in range(num_zones)
        },
        'aws_security_group': {
            'gitlab_vpn': {
                'name': 'azul-gitlab-vpn',
                'vpc_id': '${aws_vpc.gitlab.id}',
                'egress': [
                    vpc.security_rule(description='Any traffic to the VPC',
                                      cidr_blocks=['${aws_vpc.gitlab.cidr_block}'],
                                      protocol=-1,
                                      from_port=0,
                                      to_port=0),
                    vpc.security_rule(description='ICMP for PMTUD',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol='icmp',
                                      from_port=3,  # Destination Unreachable
                                      to_port=4)  # Fragmentation required DF-flag set
                ],
                'ingress': [
                    vpc.security_rule(description='Any traffic from the VPC',
                                      cidr_blocks=['${aws_vpc.gitlab.cidr_block}'],
                                      protocol=-1,
                                      from_port=0,
                                      to_port=0),
                    vpc.security_rule(description='ICMP for PMTUD',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol='icmp',
                                      from_port=3,  # Destination Unreachable
                                      to_port=4)  # Fragmentation required DF-flag set
                ]
            },
            'gitlab_alb': {
                'name': 'azul-gitlab-alb',
                'vpc_id': '${aws_vpc.gitlab.id}',
                'egress': [
                    vpc.security_rule(description='Any traffic to the VPC',
                                      cidr_blocks=['${aws_vpc.gitlab.cidr_block}'],
                                      protocol=-1,
                                      from_port=0,
                                      to_port=0),
                    vpc.security_rule(description='ICMP for PMTUD',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol='icmp',
                                      from_port=3,  # Destination Unreachable
                                      to_port=4)  # Fragmentation required DF-flag set
                ],
                'ingress': [
                    vpc.security_rule(description='HTTPS from the VPC',
                                      cidr_blocks=['${aws_vpc.gitlab.cidr_block}'],
                                      protocol='tcp',
                                      from_port=443,
                                      to_port=443),
                    vpc.security_rule(description='ICMP for PMTUD',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol='icmp',
                                      from_port=3,  # Destination Unreachable
                                      to_port=4)  # Fragmentation required DF-flag set

                ]
            },
            'gitlab': {
                'name': 'azul-gitlab',
                'vpc_id': '${aws_vpc.gitlab.id}',
                'egress': [
                    vpc.security_rule(description='Any traffic to anywhere (to be routed by NAT Gateway)',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol=-1,
                                      from_port=0,
                                      to_port=0),
                    # VXLAN for AWS Traffic Capture to a target in the same SG
                    # In a nutshell, start target instance in this SG, set up
                    # mirroring target for instance, set up mirroring session
                    # for source. Then on target instance:
                    #
                    # sudo ip link add vxlan0 type vxlan id <VNI from session> dev eth0 local 10.0.0.207 dstport 4789
                    # sudo sysctl net.ipv6.conf.vxlan0.disable_ipv6=1
                    # sudo ip link set vxlan0 up
                    # sudo tcpdump -i vxlan0 -w /tmp/gitlab.pcap
                    # OR
                    # sudo tcpdump -i vxlan0 -w - | tee /tmp/gitlab.pcap | tcpdump -r -
                    vpc.security_rule(description='VXLAN for AWS Traffic Capture to a target in the same SG',
                                      self=True,
                                      protocol='udp',
                                      from_port=4789,
                                      to_port=4789),
                    vpc.security_rule(description='ICMP for PMTUD',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol='icmp',
                                      from_port=3,  # Destination Unreachable
                                      to_port=4)  # Fragmentation required DF-flag set
                ],
                'ingress': [
                    vpc.security_rule(description='HTTP from VPC',
                                      cidr_blocks=['${aws_vpc.gitlab.cidr_block}'],
                                      protocol='tcp',
                                      from_port=80,
                                      to_port=80),
                    *(
                        vpc.security_rule(description=f'SSH for {name} from VPC',
                                          cidr_blocks=['${aws_vpc.gitlab.cidr_block}'],
                                          protocol='tcp',
                                          from_port=int_port,
                                          to_port=int_port)
                        for ext_port, int_port, name in nlb_ports
                    ),
                    vpc.security_rule(description='VXLAN for AWS Traffic Capture to a target in the same SG',
                                      self=True,
                                      protocol='udp',
                                      from_port=4789,
                                      to_port=4789),
                    vpc.security_rule(description='ICMP for PMTUD',
                                      cidr_blocks=['0.0.0.0/0'],
                                      protocol='icmp',
                                      from_port=3,  # Destination Unreachable
                                      to_port=4)  # Fragmentation required DF-flag set
                ]
            }
        },
        'aws_s3_bucket': {
            'gitlab': {
                'bucket':
                    f'edu-ucsc-gi-{aws.account_name}-gitlab.{aws.region_name}'
                    if 'anvil' in config.domain_name else
                    f'edu-ucsc-gi-singlecell-azul-gitlab-{config.deployment_stage}-{aws.region_name}'
            }
        },
        'aws_s3_bucket_policy': {
            'gitlab': {
                'bucket': '${aws_s3_bucket.gitlab.id}',
                # FIXME:  Expire old logs using lifecycle policy
                #         https://github.com/DataBiosphere/azul/issues/3620
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'AWS': f'arn:aws:iam::{elb_region_account_id[aws.region_name]}:root'
                            },
                            'Action': 's3:PutObject',
                            'Resource': f'${{aws_s3_bucket.gitlab.arn}}/{gitlab_logs_path}'
                        },
                        *[
                            {
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'delivery.logs.amazonaws.com'
                                },
                                'Action': f's3:{action}',
                                'Resource': resource,
                                **(
                                    {
                                        'Condition': {
                                            'StringEquals': {
                                                's3:x-amz-acl': 'bucket-owner-full-control'
                                            }
                                        }
                                    }
                                    if action == 'PutObject' else
                                    {}
                                )
                            }
                            for action, resource in [
                                ('PutObject', f'${{aws_s3_bucket.gitlab.arn}}/{gitlab_logs_path}'),
                                ('GetBucketAcl', '${aws_s3_bucket.gitlab.arn}')
                            ]
                        ]
                    ]
                })
            }
        },
        'aws_cloudwatch_log_group': {
            'gitlab_vpn': {
                'name': '/aws/vpn/azul-gitlab',
                'retention_in_days': 1827,
            }
        },
        'aws_ec2_client_vpn_endpoint': {
            'gitlab': {
                'client_cidr_block': vpn_subnet,
                'security_group_ids': ['${aws_security_group.gitlab_vpn.id}'],
                'server_certificate_arn': '${data.aws_acm_certificate.gitlab_vpn.arn}',
                'transport_protocol': 'udp',
                'split_tunnel': True,
                'authentication_options': {
                    'type': 'certificate-authentication',
                    'root_certificate_chain_arn': '${data.aws_acm_certificate.gitlab_vpn.arn}'
                },
                'session_timeout_hours': 8,
                'vpc_id': '${aws_vpc.gitlab.id}',
                'connection_log_options': {
                    'enabled': True,
                    'cloudwatch_log_group': '${aws_cloudwatch_log_group.gitlab_vpn.name}'
                }
            }
        },
        'aws_ec2_client_vpn_network_association': {
            f'gitlab_{zone}': {
                'client_vpn_endpoint_id': '${aws_ec2_client_vpn_endpoint.gitlab.id}',
                'subnet_id': f'${{aws_subnet.gitlab_public_{zone}.id}}'
            }
            for zone in range(num_zones)
        },
        'aws_ec2_client_vpn_authorization_rule': {
            'gitlab': {
                'client_vpn_endpoint_id': '${aws_ec2_client_vpn_endpoint.gitlab.id}',
                'target_network_cidr': '${aws_vpc.gitlab.cidr_block}',
                'authorize_all_groups': True
            }
        },
        'aws_lb': {
            # Add an NLB so we can have a Route 53 alias record pointing at it
            'gitlab_nlb': {
                'name': 'azul-gitlab-nlb',
                'load_balancer_type': 'network',
                'internal': 'true',
                'subnets': [
                    f'${{aws_subnet.gitlab_public_{zone}.id}}' for zone in range(num_zones)
                ]
            },
            # Add an ALB for the same reason and for terminating TLS
            'gitlab_alb': {
                'name': 'azul-gitlab-alb',
                'load_balancer_type': 'application',
                'internal': 'true',
                'subnets': [
                    f'${{aws_subnet.gitlab_public_{zone}.id}}' for zone in range(num_zones)
                ],
                'security_groups': [
                    '${aws_security_group.gitlab_alb.id}'
                ],
                'access_logs': {
                    'bucket': '${aws_s3_bucket.gitlab.id}',
                    'prefix': logs_path_prefix,
                    'enabled': True
                }
            }
        },
        'aws_lb_listener': {
            **(
                {
                    'gitlab_' + name: {
                        'port': ext_port,
                        'protocol': 'TCP',
                        'default_action': [
                            {
                                'target_group_arn': '${aws_lb_target_group.gitlab_' + name + '.id}',
                                'type': 'forward'
                            }
                        ],
                        'load_balancer_arn': '${aws_lb.gitlab_nlb.id}'
                    }
                    for ext_port, int_port, name in nlb_ports
                }
            ),
            'gitlab_http': {
                'port': 443,
                'protocol': 'HTTPS',
                'ssl_policy': 'ELBSecurityPolicy-2016-08',
                'certificate_arn': '${aws_acm_certificate.gitlab.arn}',
                'default_action': [
                    {
                        'target_group_arn': '${aws_lb_target_group.gitlab_http.id}',
                        'type': 'forward'
                    }
                ],
                'load_balancer_arn': '${aws_lb.gitlab_alb.id}'
            }
        },
        'aws_lb_target_group': {
            **(
                {
                    'gitlab_' + name: {
                        'name': 'azul-gitlab-' + name,
                        'port': int_port,
                        'protocol': 'TCP',
                        # A target type of `instance` preserves the source IP in
                        # packets forwarded by the NLB. Any security group
                        # guarding this traffic must allow ingress not from the
                        # NLB's internal IP but from the original source IP.
                        'target_type': 'instance',
                        'vpc_id': '${aws_vpc.gitlab.id}'
                    }
                    for ext_port, int_port, name in nlb_ports
                }
            ),
            'gitlab_http': {
                'name': 'azul-gitlab-http',
                'port': 80,
                'protocol': 'HTTP',
                'target_type': 'instance',
                'vpc_id': '${aws_vpc.gitlab.id}',
                'health_check': {
                    'protocol': 'HTTP',
                    'path': '/',
                    'port': 'traffic-port',
                    'healthy_threshold': 5,
                    'unhealthy_threshold': 2,
                    'timeout': 5,
                    'interval': 30,
                    'matcher': '302'
                }
            }
        },
        'aws_lb_target_group_attachment': {
            **(
                {
                    'gitlab_' + name: {
                        'target_group_arn': '${aws_lb_target_group.gitlab_' + name + '.arn}',
                        'target_id': '${aws_instance.gitlab.id}'
                    }
                    for ext_port, int_port, name in nlb_ports
                }
            ),
            'gitlab_http': {
                'target_group_arn': '${aws_lb_target_group.gitlab_http.arn}',
                'target_id': '${aws_instance.gitlab.id}'
            }
        },
        'aws_acm_certificate': {
            'gitlab': {
                'domain_name': '${aws_route53_record.gitlab.name}',
                'subject_alternative_names': ['${aws_route53_record.gitlab_docker.name}'],
                'validation_method': 'DNS',
                'lifecycle': {
                    'create_before_destroy': True
                }
            }
        },
        'aws_acm_certificate_validation': {
            'gitlab': {
                'certificate_arn': '${aws_acm_certificate.gitlab.arn}',
                'validation_record_fqdns': '${[for r in aws_route53_record.gitlab_validation : r.fqdn]}',
            }
        },
        'aws_route53_record': {
            'gitlab_validation': {
                # The double curlies are not a mistake. This is not an f-string,
                # it's a TF expression containing a dictiona
                'for_each': '${{for o in aws_acm_certificate.gitlab.domain_validation_options : o.domain_name => o}}',
                'name': '${each.value.resource_record_name}',
                'type': '${each.value.resource_record_type}',
                'zone_id': '${data.aws_route53_zone.gitlab.id}',
                'records': [
                    '${each.value.resource_record_value}',
                ],
                'ttl': 60
            },
            **dict_merge(
                {
                    departition('gitlab', '_', subdomain): {
                        'zone_id': '${data.aws_route53_zone.gitlab.id}',
                        'name': departition(subdomain, '.', f'gitlab.{config.domain_name}'),
                        'type': 'A',
                        'alias': {
                            'name': '${aws_lb.gitlab_alb.dns_name}',
                            'zone_id': '${aws_lb.gitlab_alb.zone_id}',
                            'evaluate_target_health': False
                        }
                    }
                }
                for subdomain in [None, 'docker']
            ),
            'gitlab_ssh': {
                'zone_id': '${data.aws_route53_zone.gitlab.id}',
                'name': f'ssh.gitlab.{config.domain_name}',
                'type': 'A',
                'alias': {
                    'name': '${aws_lb.gitlab_nlb.dns_name}',
                    'zone_id': '${aws_lb.gitlab_nlb.zone_id}',
                    'evaluate_target_health': False
                }
            }
        },
        'aws_network_interface': {
            'gitlab': {
                'subnet_id': '${aws_subnet.gitlab_private_0.id}',
                'security_groups': [
                    '${aws_security_group.gitlab.id}'
                ]
            }
        },
        'aws_volume_attachment': {
            'gitlab': {
                'device_name': '/dev/sdf',
                'volume_id': '${data.aws_ebs_volume.gitlab.id}',
                'instance_id': '${aws_instance.gitlab.id}',
                'provisioner': {
                    'local-exec': {
                        'when': 'destroy',
                        'command': 'aws ec2 stop-instances --instance-ids ${self.instance_id}'
                                   ' && aws ec2 wait instance-stopped --instance-ids ${self.instance_id}'
                    }
                }
            }
        },
        'aws_key_pair': {
            'gitlab': {
                'key_name': 'azul-gitlab',
                'public_key': public_key
            }
        },
        'aws_iam_role': {
            'gitlab': {
                'name': 'azul-gitlab',
                'path': '/',
                'assume_role_policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Principal': {
                                'Service': 'ec2.amazonaws.com'
                            },
                            'Effect': 'Allow',
                            'Sid': ''
                        }
                    ]
                })
            }
        },
        'aws_iam_instance_profile': {
            'gitlab': {
                'name': 'azul-gitlab',
                'role': '${aws_iam_role.gitlab.name}',
            }
        },
        'aws_iam_policy': {
            'gitlab_iam': {
                'name': 'azul-gitlab-iam',
                'path': '/',
                'policy': '${data.aws_iam_policy_document.gitlab_iam.json}'
            },
            'gitlab_boundary': {
                'name': config.permissions_boundary_name,
                'path': '/',
                'policy': '${data.aws_iam_policy_document.gitlab_boundary.json}'
            }
        },
        'aws_iam_role_policy_attachment': {
            'gitlab_iam': {
                'role': '${aws_iam_role.gitlab.name}',
                'policy_arn': '${aws_iam_policy.gitlab_iam.arn}'
            },
            # Since we are using the boundary as a policy Gitlab can explicitly
            # do everything within the boundary
            'gitlab_boundary': {
                'role': '${aws_iam_role.gitlab.name}',
                'policy_arn': '${aws_iam_policy.gitlab_boundary.arn}'
            }
        },
        'google_service_account': {
            'gitlab': {
                'project': '${local.google_project}',
                'account_id': name,
                'display_name': name,
            }
            for name in ['azul-gitlab']
        },
        'google_project_iam_member': {
            'gitlab_' + name: {
                'project': '${local.google_project}',
                'role': role,
                'member': 'serviceAccount:${google_service_account.gitlab.email}'
            }
            for name, role in [
                ('write', '${google_project_iam_custom_role.gitlab.id}'),
                ('read', 'roles/viewer')
            ]
        },
        'google_project_iam_custom_role': {
            'gitlab': {
                'role_id': 'azul_gitlab',
                'title': 'azul_gitlab',
                'permissions': [
                    'resourcemanager.projects.setIamPolicy',
                    *(
                        f'iam.{resource}.{operation}'
                        for operation in ['create', 'delete', 'get', 'list', 'update', 'undelete']
                        for resource in ['roles', 'serviceAccountKeys', 'serviceAccounts']
                        if resource != 'serviceAccountKeys' or operation not in ['update', 'undelete']
                    )
                ]
            }
        },
        'aws_instance': {
            'gitlab': {
                'iam_instance_profile': '${aws_iam_instance_profile.gitlab.name}',
                'ami': ami_id[config.region],
                'instance_type': 't3a.xlarge',
                'root_block_device': {
                    'volume_size': 64
                },
                'key_name': '${aws_key_pair.gitlab.key_name}',
                'network_interface': {
                    'network_interface_id': '${aws_network_interface.gitlab.id}',
                    'device_index': 0
                },
                'user_data_replace_on_change': True,
                'user_data': '#cloud-config\n' + yaml.dump({
                    'mounts': [
                        ['/dev/nvme1n1', '/mnt/gitlab', 'ext4', '']
                    ],
                    'packages': ['docker'],
                    'ssh_authorized_keys': other_public_keys.get(config.deployment_stage, []),
                    'write_files': [
                        {
                            'path': '/etc/systemd/system/gitlab-dind.service',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=Docker-in-Docker service for GitLab',
                                'After=docker.service',
                                'Requires=docker.service',
                                '[Service]',
                                'TimeoutStartSec=5min',  # `docker pull` may take a long time
                                'Restart=always',
                                'ExecStartPre=-/usr/bin/docker stop gitlab-dind',
                                'ExecStartPre=-/usr/bin/docker rm gitlab-dind',
                                'ExecStartPre=-/usr/bin/docker network rm gitlab-runner-net',
                                'ExecStartPre=/usr/bin/docker network create gitlab-runner-net',
                                'ExecStartPre=/usr/bin/docker pull ' + dind_image,
                                jw(
                                    'ExecStart=/usr/bin/docker',
                                    'run',
                                    '--name gitlab-dind',
                                    '--privileged',
                                    '--rm',
                                    '--network gitlab-runner-net',
                                    # The following option makes dockerd
                                    # listen on port 2375 without TLS. By
                                    # default, dockerd only listens on 2376
                                    # with TLS. The port is not exposed and
                                    # can only be reached from other
                                    # containers on the dedicated
                                    # gitlab-runner-net network, so TLS is
                                    # unnecessary.
                                    '--env DOCKER_TLS_CERTDIR=',
                                    '--volume /mnt/gitlab/docker:/var/lib/docker',
                                    '--volume /mnt/gitlab/runner/config:/etc/gitlab-runner',
                                    dind_image
                                ),
                                '[Install]',
                                'WantedBy=multi-user.target',
                            )
                        },
                        {
                            'path': '/etc/systemd/system/gitlab.service',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=GitLab service',
                                'After=docker.service',
                                'Requires=docker.service',
                                '[Service]',
                                'TimeoutStartSec=5min',  # `docker pull` may take a long time
                                'Restart=always',
                                'ExecStartPre=-/usr/bin/docker stop gitlab',
                                'ExecStartPre=-/usr/bin/docker rm gitlab',
                                'ExecStartPre=/usr/bin/docker pull ' + gitlab_image,
                                jw(
                                    'ExecStart=/usr/bin/docker',
                                    'run',
                                    '--name gitlab',
                                    '--hostname ${aws_route53_record.gitlab.name}',
                                    '--publish 80:80',
                                    '--publish 2222:22',
                                    '--rm',
                                    '--volume /mnt/gitlab/config:/etc/gitlab',
                                    '--volume /mnt/gitlab/logs:/var/log/gitlab',
                                    '--volume /mnt/gitlab/data:/var/opt/gitlab',
                                    gitlab_image
                                ),
                                '[Install]',
                                'WantedBy=multi-user.target'
                            )
                        },
                        {
                            'path': '/etc/systemd/system/gitlab-runner.service',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=GitLab runner service',
                                'After=docker.service gitlab-dind.service gitlab.service',
                                'Requires=docker.service gitlab-dind.service gitlab.service',
                                '[Service]',
                                'TimeoutStartSec=5min',  # `docker pull` may take a long time
                                'Restart=always',
                                'ExecStartPre=-/usr/bin/docker stop gitlab-runner',
                                'ExecStartPre=-/usr/bin/docker rm gitlab-runner',
                                'ExecStartPre=/usr/bin/docker pull ' + runner_image,
                                jw(
                                    'ExecStart=/usr/bin/docker',
                                    'run',
                                    '--name gitlab-runner',
                                    '--rm',
                                    '--volume /mnt/gitlab/runner/config:/etc/gitlab-runner',
                                    '--network gitlab-runner-net',
                                    '--env DOCKER_HOST=tcp://gitlab-dind:2375',
                                    runner_image
                                ),
                                '[Install]',
                                'WantedBy=multi-user.target'
                            )
                        },
                        {
                            'path': '/etc/systemd/system/clamscan.service',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=ClamAV malware scan of entire file system',
                                'After=docker.service gitlab-dind.service',
                                'Requires=docker.service gitlab-dind.service',
                                '[Service]',
                                'Type=simple',
                                'TimeoutStartSec=5min',  # `docker pull` may take a long time
                                'ExecStartPre=-/usr/bin/docker stop clamscan',
                                'ExecStartPre=-/usr/bin/docker rm clamscan',
                                'ExecStartPre=/usr/bin/docker pull ' + clamav_image,
                                jw(
                                    'ExecStart=/usr/bin/docker',
                                    'run',
                                    '--name clamscan',
                                    '--rm',
                                    '--volume /var/run/docker.sock:/var/run/docker.sock',
                                    '--volume /:/scan:ro',
                                    '--volume /mnt/gitlab/clamav:/var/lib/clamav:rw',
                                    clamav_image,
                                    '/bin/sh',
                                    '-c',
                                    qq(
                                        'freshclam',
                                        '&& echo freshclam succeeded',
                                        '|| echo freshclam failed',
                                        '&& clamscan',
                                        '--recursive',
                                        '--infected',  # Only print infected files
                                        '--allmatch=yes',  # Continue scanning within file after a match
                                        '--exclude-dir=^/scan/var/lib/docker/overlay2/.*/merged/sys',
                                        '--exclude-dir=^/scan/var/lib/docker/overlay2/.*/merged/proc',
                                        '--exclude-dir=^/scan/var/lib/docker/overlay2/.*/merged/dev',
                                        '--exclude-dir=^/scan/sys',
                                        '--exclude-dir=^/scan/proc',
                                        '--exclude-dir=^/scan/dev',
                                        '/scan',
                                        '&& echo clamscan succeeded',
                                        '|| echo clamscan failed'
                                    )
                                ),
                                '[Install]',
                                'WantedBy='
                            )
                        },
                        {
                            'path': '/etc/systemd/system/clamscan.timer',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=Scheduled ClamAV malware scan of entire file system',
                                '[Timer]',
                                'OnCalendar=Sun *-*-* 8:0:0',
                                '[Install]',
                                'WantedBy=timers.target'
                            )
                        },
                        {
                            'path': '/etc/systemd/system/prune-images.service',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=Pruning of stale docker images',
                                'After=docker.service gitlab-dind.service',
                                'Requires=docker.service gitlab-dind.service',
                                '[Service]',
                                'Type=simple',
                                'TimeoutStartSec=5min',  # `docker pull` may take a long time
                                'ExecStartPre=-/usr/bin/docker stop prune-images',
                                'ExecStartPre=-/usr/bin/docker rm prune-images',
                                'ExecStartPre=/usr/bin/docker pull ' + dind_image,
                                jw(
                                    'ExecStart=/usr/bin/docker',
                                    'run',
                                    '--name prune-images',
                                    '--rm',
                                    '--volume /var/run/docker.sock:/var/run/docker.sock',
                                    dind_image,
                                    'exec',  # Execute (as in `docker exec`) …
                                    'gitlab-dind',  # … inside the gitlab-dind container …
                                    'docker',  # … the docker …
                                    'image',  # … image command …
                                    'prune',  # … to delete, …
                                    '--force',  # … without prompting for confirmation, …
                                    '--all',  # … all images …
                                    f'--filter "until={30 * 24}"',  # … except those from more recent builds.
                                    #
                                    # If we deleted more recent images, we
                                    # would risk failing the requirements
                                    # check on sandbox builds since that
                                    # check depends on image caching. The
                                    # dead line below assumes that the most
                                    # recent pipeline was run less than a
                                    # month ago.
                                ),
                                '[Install]',
                                'WantedBy='
                            )
                        },
                        {
                            'path': '/etc/systemd/system/prune-images.timer',
                            'permissions': '0644',
                            'owner': 'root',
                            'content': jl(
                                '[Unit]',
                                'Description=Scheduled pruning of stale docker images',
                                '[Timer]',
                                'OnCalendar=Sat *-*-* 12:0:0',
                                '[Install]',
                                'WantedBy=timers.target'
                            )
                        }
                    ],
                    'runcmd': [
                        ['systemctl', 'daemon-reload'],
                        [
                            'systemctl',
                            'enable',
                            '--now',  # also start the units
                            '--no-block',  # avoid dead-lock with cloud-init which is an active systemd unit, too
                            'docker',
                            'gitlab-dind',
                            'gitlab',
                            'gitlab-runner',
                            'clamscan.timer',
                            'prune-images.timer'
                        ]
                    ],
                }, indent=2),
                'tags': {
                    'Owner': config.owner
                }
            }
        },
        'aws_sns_topic': {
            'azul_monitoring': {
                'name': 'azul-monitoring'
            }
        },
        'aws_sns_topic_subscription': {
            'azul_monitoring_azul_group': {
                'topic_arn': '${aws_sns_topic.azul_monitoring.arn}',
                # The `email` protocol is only partially supported. Be careful
                # when changing or removing this.
                # https://registry.terraform.io/providers/hashicorp/aws/4.3.0/docs/resources/sns_topic_subscription#protocol-support
                'protocol': 'email',
                'endpoint': 'azul-group@ucsc.edu'
            }
        }
    }
})
