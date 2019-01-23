from textwrap import dedent

from azul import config
from azul.template import emit

# This Terraform config creates a single EC2 instance with a bunch of Docker containers running on it:
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
# │       │     │  ║ ┃       │  rails  │─┃─ ─ ─ ─    │           ┃      ┃    "build"     ┃             │ ┃ ║
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
# 1) an application load balancer (ALB) that terminates SSL and forwards to the Gitlab web UI
#
# 2) an network load balancer that forwards port 22 to an SSH daemon in the the Gitlab container (for git+ssh://) and
# port 2222 to an SSH daemon for shell access in RancherOS' `console` container.
#
# The instance itself does not have a public IP and is only reachable from the internet through the load balancers.
#
# The NLB's public IP is bound to ssh.gitlab.{dev,prod}.explore.data.humancellatlas.org
# The ALB's public IP is bound to gitlab.{dev,prod}.explore.data.humancellatlas.org


# To log into the instance run `ssh rancher@ssh.gitlab.dev.explore.data.humancellatlas.org -p 2222`. Your SSH key
# must be mentioned in public_key or other_public_keys below.
#
# The Gitlab web UI is at https://gitlab.{dev,prod}.explore.data.humancellatlas.org/.

# It's safe to destroy all resources in this TF config. You can always build them up again. The only golden egg is
# the EBS volume that's attached to the instance. See below under ebs_volume_name.


# RancherOS was chosen for the AMI because it has Docker pre installed and supports cloud-init user data.
#
# The container wiring is fairly complicated as it involves docker-in-docker. It is inspired by
#
# https://medium.com/@tonywooster/docker-in-docker-in-gitlab-runners-220caeb708ca
#
# In this setup the build container is not privileged while allowing for image layer caching between builds. The
# `elasticsearch` and `dynamodb-local` containers are included as examples of test fixtures launched during test
# setup. This aspect may evolve over time. It's worth noting that these fixture containers are siblings of the build
# container. When the tests are run locally or on Travis, the tests run on the host. The above diagram also glosses
# over the fact that there are multiple separate bridge networks involved. The `gitlab-dind` and `gitlab-runner`
# containers are attached to a separate bridge network. The `gitlab` container is on the default bridge network.


# IMPORTANT: Before running `make destroy`, the Gitlab instance needs to be stopped. Otherwise `make destroy` will
# timeout trying to detach the volume.


# IMPORTANT: There is a bug in the Terraform AWS provider (I think its conflating the listeners) which causes one of
# the NLB listeners to be missing after `terraform apply`.


# The name of an EBS volume to attach to the instance. This EBS volume must exist and be formatted with ext4. We
# don't manage the volume in Terraform because that would require formatting it once after creation. That can only be
# one after attaching it to an EC2 instance but before mounting it. This turns out to be difficult and risks
# overwriting existing data on the volume. We'd also have to prevent the volume from being deleted during `terraform
# destroy`.
#
# If this EBS volume does not exist you must create it with the desired size before running Terraform. To then format
# the volume, you can then either attach it to some other Linux instance and format it there or use `make terraform`
# to create the actual Gitlab instance and attach the volume. For the latter you would need to ssh into the Gitlab
# instance, format `/dev/xvdf` and reboot the instance.
#
# The EBS volume should be backed up (EBS snapshot) periodically. Not only does it contain Gitlabs data but also its
# config. FIXME: I wish I had tracked the changes to the config. Some of the config in the EBS volume duplicates or
# corresponds to settings in here.
#
ebs_volume_name = "azul-gitlab"

num_zones = 2  # An ALB needs at least two availability zones

# List of port forwardings by the network load balancer (NLB). The first element in the tuple is the port on the
# external interface of the NLB, the second element is the port on the instance the the NLB forwards to.
#
nlb_ports = [(22, 2222, 'git'), (2222, 22, 'ssh')]

# The Azul Gitlab instance uses one VPC. This variable specifies the IPv4 address block to be used by that VPC.
#
# Be sure to avoid the default Docker address pool:
#
# https://github.com/docker/libnetwork/blob/a79d3687931697244b8e03485bf7b2042f8ec6b6/ipamutils/utils.go#L10
#
vpc_cidr = "172.71.0.0/16"

# The name of the SSH keypair whose public key is to be deposited on the instance by AWS
#
key_name = "hannes@ucsc.edu"

# The public key of that keypair
#
public_key = (
    "ssh-rsa"
    " "
    "AAAAB3NzaC1yc2EAAAADAQABAAABAQDhRBbejN2qT5+6nfpzxPTfTFuSDSiPrAyDKH+V/A9+Xw4ZT8Z3K4d0w0KlwjtRZ"
    "7shmIxkN44DY8R8LGCiybYHHVHqRNoQYqY1BkfSSP8h+eTylo4kRE4hKzs97dsBKYN1iXYXxd9yJGf6u3iR51LFijNLNN"
    "6QEsxC6PhBReye21X8KdrlOO1owG3D+BVF6Q8PxpBFTjwMLiJUe3hm/vNTrCJErtHAr6ok28BY7rj3UVbGscrnsMIpdsX"
    "OFDl5NU7tB6H9HlQ46l/W70ZSpzx8FQel9kbxcjZLinmsujuILC2bI1ev4EcdTRXo9SHo5VLPnE9J2f6StlqbBYJpbdOl"
    " "
    "hannes@ucsc.edu"
)

other_public_keys = [
    (
        "ssh-rsa"
        " "
        "AAAAB3NzaC1yc2EAAAADAQABAAABAQCmNbmfZWBPg+jKhH20KjmpOOxo4I6HaL3qQg7ilxtDyvg+F4PG0vwBgAPiTd04o"
        "iaOQmXy/On/B2aZNd/GpZLpywTu1f+QhFl4CgDOd3uK9Dq88VzLFEHjrfrzv21pnuu2FIO+u+zVgPU3i4dNlYK10MYGW2"
        "tWXEIA0AV3lO6Erk8Xcoru72iYXsT9RP2Md0o8FsM/bytRPDDk4GRWcLR6oVLEzxhvnYJANIAaJvAjKdC0tqaSPmseAzI"
        "UjFeDpX/8tvXhhXao2lgzFkSITIvJkKiKiQL+bykLy63j6PDfKY1jLoqIVdfHjSvj3XpVTlSvy9pOJ4LsYGysSnWVeq4j"
        " "
        "dave@clevercanary.com"
    )
]


def subnet_name(public):
    return 'public' if public else 'private'


def subnet_number(zone, public):
    # Even numbers for private subnets, odd numbers for public subnets. The advantage of this numbering scheme is
    # that it won't be perturbed by adding zones.
    return 2 * zone + int(public)


# If the attachment of an instance to an NLB target group is by instance ID, the NLB preserves the source IP of
# ingress packets. For that to work, the security group protecting the instance must allow ingress from everywhere
# for the port being forwarded by the NLB. This should be ok because the instance in in a private subnet.
#
# If the attachement is by IP, the source IP is rewritten to be that of the load balancers internal interface. The
# security group can be restricted to the internal subnet but the original source IP is lost and can't be used for
# logging and the like.
#
nlb_preserve_source_ip = True

emit({} if config.deployment_stage not in ('dev', 'prod') else {
    "data": {
        "aws_availability_zones": {
            "available": {}
        },
        "aws_ebs_volume": {
            "gitlab": {
                "filter": [
                    {
                        "name": "volume-type",
                        "values": ["gp2"]
                    },
                    {
                        "name": "tag:Name",
                        "values": [ebs_volume_name]
                    }
                ],
                "most_recent": True
            }
        },
        # This Route53 zone also has to exist.
        "aws_route53_zone": {
            "gitlab": {
                "name": config.domain_name + ".",
                "private_zone": False
            }
        }
    },
    "resource": {
        "aws_vpc": {
            "gitlab": {
                "cidr_block": vpc_cidr,
                "tags": {
                    "Name": "azul-gitlab"
                }
            }
        },
        "aws_subnet": {  # a public and a private subnet per availability zone
            f"gitlab_{subnet_name(public)}_{zone}": {
                "availability_zone": f"${{data.aws_availability_zones.available.names[{zone}]}}",
                "cidr_block": f"${{cidrsubnet(aws_vpc.gitlab.cidr_block, 8, {subnet_number(zone, public)})}}",
                "map_public_ip_on_launch": public,
                "vpc_id": "${aws_vpc.gitlab.id}",
                "tags": {
                    "Name": f"azul-gitlab-{subnet_name(public)}-{subnet_number(zone, public)}"
                }
            } for public in (False, True) for zone in range(num_zones)
        },
        "aws_internet_gateway": {
            "gitlab": {
                "vpc_id": "${aws_vpc.gitlab.id}",
                "tags": {
                    "Name": "azul-gitlab"
                }
            }
        },
        "aws_route": {
            "gitlab": {
                "destination_cidr_block": "0.0.0.0/0",
                "gateway_id": "${aws_internet_gateway.gitlab.id}",
                "route_table_id": "${aws_vpc.gitlab.main_route_table_id}"
            }
        },
        "aws_eip": {
            f"gitlab_{zone}": {
                "depends_on": [
                    "aws_internet_gateway.gitlab"
                ],
                "vpc": True,
                "tags": {
                    "Name": f"azul-gitlab-{zone}"
                }
            } for zone in range(num_zones)
        },
        "aws_nat_gateway": {
            f"gitlab_{zone}": {
                "allocation_id": f"${{aws_eip.gitlab_{zone}.id}}",
                "subnet_id": f"${{aws_subnet.gitlab_public_{zone}.id}}",
                "tags": {
                    "Name": f"azul-gitlab-{zone}"
                }
            } for zone in range(num_zones)
        },
        "aws_route_table": {
            f"gitlab_{zone}": {
                "route": [
                    {
                        "cidr_block": "0.0.0.0/0",
                        "nat_gateway_id": f"${{aws_nat_gateway.gitlab_{zone}.id}}"
                    }
                ],
                "vpc_id": "${aws_vpc.gitlab.id}",
                "tags": {
                    "Name": f"azul-gitlab-{zone}"
                }
            } for zone in range(num_zones)
        },
        "aws_route_table_association": {
            f"gitlab_{zone}": {
                "route_table_id": f"${{aws_route_table.gitlab_{zone}.id}}",
                "subnet_id": f"${{aws_subnet.gitlab_private_{zone}.id}}"
            } for zone in range(num_zones)
        },
        "aws_security_group": {
            "gitlab_alb": {
                "name": "azul-gitlab-alb",
                "vpc_id": "${aws_vpc.gitlab.id}",
                "egress": [
                    {
                        "cidr_blocks": ["0.0.0.0/0"],
                        "protocol": -1,
                        "from_port": 0,
                        "to_port": 0
                    }
                ],
                "ingress": [
                    {
                        "cidr_blocks": ["0.0.0.0/0"],
                        "protocol": "tcp",
                        "from_port": 443,
                        "to_port": 443
                    },
                    *({
                        "cidr_blocks": ["0.0.0.0/0"],
                        "protocol": "tcp",
                        "from_port": ext_port,
                        "to_port": ext_port
                    } for ext_port, int_port, name in nlb_ports)
                ]
            },
            "gitlab": {
                "name": "azul-gitlab",
                "vpc_id": "${aws_vpc.gitlab.id}",
                "egress": [
                    {
                        "cidr_blocks": ["0.0.0.0/0"],
                        "protocol": -1,
                        "from_port": 0,
                        "to_port": 0
                    }
                ],
                "ingress": [
                    {
                        "from_port": 80,
                        "protocol": "tcp",
                        "security_groups": [
                            "${aws_security_group.gitlab_alb.id}"
                        ],
                        "to_port": 80
                    },
                    *({
                        "cidr_blocks": [
                            "0.0.0.0/0" if nlb_preserve_source_ip else "${aws_vpc.gitlab.cidr_block}"
                        ],
                        "protocol": "tcp",
                        "from_port": int_port,
                        "to_port": int_port
                    } for ext_port, int_port, name in nlb_ports)
                ]
            }
        },
        "aws_lb": {
            "gitlab_nlb": {
                "name": "azul-gitlab-nlb",
                "load_balancer_type": "network",
                "subnets": [
                    f"${{aws_subnet.gitlab_public_{zone}.id}}" for zone in range(num_zones)
                ],
                "tags": {
                    "Name": "azul-gitlab"
                }
            },
            "gitlab_alb": {
                "name": "azul-gitlab-alb",
                "load_balancer_type": "application",
                "subnets": [
                    f"${{aws_subnet.gitlab_public_{zone}.id}}" for zone in range(num_zones)
                ],
                "security_groups": [
                    "${aws_security_group.gitlab_alb.id}"
                ],
                "tags": {
                    "Name": "azul-gitlab"
                }
            }
        },
        "aws_lb_listener": {
            **({
                "gitlab_" + name: {
                    "port": ext_port,
                    "protocol": "TCP",
                    "default_action": [
                        {
                            "target_group_arn": "${aws_lb_target_group.gitlab_" + name + ".id}",
                            "type": "forward"
                        }
                    ],
                    "load_balancer_arn": "${aws_lb.gitlab_nlb.id}"
                } for ext_port, int_port, name in nlb_ports
            }),
            "gitlab_http": {
                "port": 443,
                "protocol": "HTTPS",
                "ssl_policy": "ELBSecurityPolicy-2016-08",
                "certificate_arn": "${aws_acm_certificate.gitlab.arn}",
                "default_action": [
                    {
                        "target_group_arn": "${aws_lb_target_group.gitlab_http.id}",
                        "type": "forward"
                    }
                ],
                "load_balancer_arn": "${aws_lb.gitlab_alb.id}"
            }
        },
        "aws_lb_target_group": {
            **({
                "gitlab_" + name: {
                    "name": "azul-gitlab-" + name,
                    "port": int_port,
                    "protocol": "TCP",
                    "target_type": "instance" if nlb_preserve_source_ip else "ip",
                    "stickiness": {
                        "type": "lb_cookie",
                        "enabled": False
                    },
                    "vpc_id": "${aws_vpc.gitlab.id}"
                } for ext_port, int_port, name in nlb_ports
            }),
            "gitlab_http": {
                "name": "azul-gitlab-http",
                "port": 80,
                "protocol": "HTTP",
                "target_type": "instance",
                "stickiness": {
                    "type": "lb_cookie",
                    "enabled": False
                },
                "vpc_id": "${aws_vpc.gitlab.id}",
                "health_check": {
                    "protocol": "HTTP",
                    "path": "/",
                    "port": "traffic-port",
                    "healthy_threshold": 5,
                    "unhealthy_threshold": 2,
                    "timeout": 5,
                    "interval": 30,
                    "matcher": "302"
                },
                "tags": {
                    "Name": "azul-gitlab-http"
                }
            }
        },
        "aws_lb_target_group_attachment": {
            **({
                "gitlab_" + name: {
                    "target_group_arn": "${aws_lb_target_group.gitlab_" + name + ".arn}",
                    "target_id": f"${{aws_instance.gitlab.{'id' if nlb_preserve_source_ip else 'private_ip'}}}"
                } for ext_port, int_port, name in nlb_ports
            }),
            "gitlab_http": {
                "target_group_arn": "${aws_lb_target_group.gitlab_http.arn}",
                "target_id": "${aws_instance.gitlab.id}"
            }
        },
        "aws_acm_certificate": {
            "gitlab": {
                "domain_name": "${aws_route53_record.gitlab.name}",
                "validation_method": "DNS",
                "provider": "aws.us-east-1",
                "tags": {
                    "Name": "azul-gitlab"
                }
            }
        },
        "aws_acm_certificate_validation": {
            "gitlab": {
                "certificate_arn": "${aws_acm_certificate.gitlab.arn}",
                "validation_record_fqdns": ["${aws_route53_record.gitlab_validation.fqdn}"],
                "provider": "aws.us-east-1"
            }
        },
        "aws_route53_record": {
            "gitlab_validation": {
                "name": "${aws_acm_certificate.gitlab.domain_validation_options.0.resource_record_name}",
                "type": "${aws_acm_certificate.gitlab.domain_validation_options.0.resource_record_type}",
                "zone_id": "${data.aws_route53_zone.gitlab.id}",
                "records": ["${aws_acm_certificate.gitlab.domain_validation_options.0.resource_record_value}"],
                "ttl": 60
            },
            "gitlab": {
                "zone_id": "${data.aws_route53_zone.gitlab.id}",
                "name": f"gitlab.{config.domain_name}",
                "type": "A",
                "alias": {
                    "name": "${aws_lb.gitlab_alb.dns_name}",
                    "zone_id": "${aws_lb.gitlab_alb.zone_id}",
                    "evaluate_target_health": False
                }
            },
            "gitlab_ssh": {
                "zone_id": "${data.aws_route53_zone.gitlab.id}",
                "name": f"ssh.gitlab.{config.domain_name}",
                "type": "A",
                "alias": {
                    "name": "${aws_lb.gitlab_nlb.dns_name}",
                    "zone_id": "${aws_lb.gitlab_nlb.zone_id}",
                    "evaluate_target_health": False
                }
            }
        },
        "aws_network_interface": {
            "gitlab": {
                "subnet_id": "${aws_subnet.gitlab_private_0.id}",
                "security_groups": [
                    "${aws_security_group.gitlab.id}"
                ],
                "tags": {
                    "Name": "azul-gitlab"
                }
            }
        },
        "aws_volume_attachment": {
            "gitlab": {
                "device_name": "/dev/sdf",
                "volume_id": "${data.aws_ebs_volume.gitlab.id}",
                "instance_id": "${aws_instance.gitlab.id}"
            }
        },
        "aws_key_pair": {
            "admin": {
                "key_name": key_name,
                "public_key": public_key
            }
        },
        "aws_instance": {
            "gitlab": {
                "ami": "ami-08bb050b78c315da3",
                "instance_type": "t2.large",
                "key_name": "${aws_key_pair.admin.key_name}",
                "network_interface": {
                    "network_interface_id": "${aws_network_interface.gitlab.id}",
                    "device_index": 0
                },
                "user_data": dedent(rf"""
                    #cloud-config
                    mounts:
                    - ["/dev/xvdf", "/mnt/gitlab", "ext4", ""]
                    rancher:
                    write_files:
                    - path: /etc/rc.local
                      permissions: "0755"
                      owner: root
                      content: |
                        #!/bin/bash
                        wait-for-docker
                        docker network \
                               create gitlab-runner-net
                        docker run \
                               --detach \
                               --name gitlab-dind \
                               --privileged \
                               --restart always \
                               --network gitlab-runner-net \
                               --volume /var/lib/docker \
                               docker:18.03.1-ce-dind
                        docker run \
                               --detach \
                               --name gitlab \
                               --hostname ${{aws_route53_record.gitlab.name}} \
                               --publish 80:80 \
                               --publish 2222:22 \
                               --restart always \
                               --volume /mnt/gitlab/config:/etc/gitlab \
                               --volume /mnt/gitlab/logs:/var/log/gitlab \
                               --volume /mnt/gitlab/data:/var/opt/gitlab \
                               gitlab/gitlab-ce:latest
                        docker run \
                               --detach \
                               --name gitlab-runner \
                               --restart always \
                               --volume /mnt/gitlab/runner/config:/etc/gitlab-runner \
                               --network gitlab-runner-net \
                               --env DOCKER_HOST=tcp://gitlab-dind:2375 \
                               gitlab/gitlab-runner:latest
                    ssh_authorized_keys: {other_public_keys}
                    """[1:]),  # trim newline char at the beginning as dedent() only removes indent common to all lines
                "tags": {
                    "Name": "azul-gitlab"
                }
            }
        }
    }
})
