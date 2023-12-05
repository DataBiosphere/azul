import json
from operator import (
    attrgetter,
)
import os
from pathlib import (
    Path,
)
import uuid

from furl import (
    furl,
)
import gitlab
from more_itertools import (
    before_and_after,
    flatten,
    one,
)

from azul import (
    JSON,
    cached_property,
    config,
)
from azul.collections import (
    adict,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    block_public_s3_bucket_access,
    emit_tf,
    enable_s3_bucket_inventory,
    set_empty_s3_bucket_lifecycle_config,
)

buckets = {
    site['bucket']: aws.qualified_bucket_name(site['bucket'])
    for project, branches in config.browser_sites.items()
    for branch, sites in branches.items()
    for site_name, site in sites.items()
}


def emit():
    bucket_id_key = '.bucket_id'
    tf_config = {
        'data': {
            'aws_s3_bucket': {
                'logs': {
                    'bucket': aws.logs_bucket,
                }
            },
            'aws_cloudfront_cache_policy': {
                'caching_optimized': {
                    'name': 'Managed-CachingOptimized'
                },
                'caching_disabled': {
                    'name': 'Managed-CachingDisabled'
                }
            },
            'aws_cloudfront_response_headers_policy': {
                'security_headers': {
                    'name': 'Managed-SecurityHeadersPolicy'
                }
            },
            'aws_route53_zone': {
                'portal': {
                    'name': config.domain_name + '.',
                    'private_zone': False
                }
            }
        },
        'resource': {
            'aws_s3_bucket': {
                bucket: {
                    'bucket': name,
                    'lifecycle': {
                        'prevent_destroy': True
                    }
                }
                for bucket, name in buckets.items()
            },
            'aws_s3_bucket_logging': {
                bucket: {
                    'bucket': '${aws_s3_bucket.%s.id}' % bucket,
                    'target_bucket': '${data.aws_s3_bucket.logs.id}',
                    # Other S3 log deliveries, like ELB, implicitly put a slash
                    # after the prefix. S3 doesn't, so we add one explicitly.
                    'target_prefix': config.s3_access_log_path_prefix(bucket) + '/'
                }
                for bucket in buckets
            },
            'aws_s3_bucket_policy': {
                bucket: {
                    'bucket': '${aws_s3_bucket.%s.id}' % bucket,
                    'policy': json.dumps({
                        'Version': '2008-10-17',
                        'Id': 'PolicyForCloudFrontPrivateContent',
                        'Statement': [
                            {
                                'Sid': 'AllowCloudFrontServicePrincipal',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'cloudfront.amazonaws.com'
                                },
                                'Action': 's3:GetObject',
                                'Resource': '${aws_s3_bucket.%s.arn}/*' % bucket,
                                'Condition': {
                                    'StringEquals': {
                                        'AWS:SourceArn': '${aws_cloudfront_distribution.portal.arn}'
                                    }
                                }
                            }
                        ]
                    })
                }
                for bucket in buckets
            },
            'aws_cloudfront_distribution': {
                'portal': {
                    'enabled': True,
                    'restrictions': {
                        'geo_restriction': {
                            'locations': [],
                            'restriction_type': 'none'
                        }
                    },
                    'price_class': 'PriceClass_100',
                    'aliases': [config.domain_name],
                    'default_root_object': 'index.html',
                    'is_ipv6_enabled': True,
                    'ordered_cache_behavior': [
                        bucket_behaviour('browser',
                                         path_pattern='/explore*',
                                         explorer_domain_router=True,
                                         add_response_security_headers=False),
                        google_search_behavior(),
                        *(
                            bucket_behaviour('consortia',
                                             path_pattern=path_pattern,
                                             ptm_next_path_mapper=True,
                                             ptm_add_response_headers=False)
                            for path_pattern in ['/consortia*', '_next/*']
                        ),
                    ],
                    'default_cache_behavior':
                        bucket_behaviour('portal',
                                         add_trailing_slash=True,
                                         add_response_security_headers=False),
                    'viewer_certificate': {
                        'acm_certificate_arn': '${aws_acm_certificate.portal.arn}',
                        'minimum_protocol_version': 'TLSv1.2_2021',
                        'ssl_support_method': 'sni-only'
                    },
                    'origin': [
                        *(
                            {
                                'origin_id': bucket_origin_id(bucket),
                                'domain_name': bucket_regional_domain_name(bucket),
                                'origin_access_control_id': '${aws_cloudfront_origin_access_control.%s.id}' % bucket
                            }
                            for bucket in buckets
                        ),
                        google_search_origin()
                    ],
                    'custom_error_response': [
                        {
                            'error_code': error_code,
                            'response_code': 404,
                            'response_page_path': '/404.html',
                            'error_caching_min_ttl': 10
                        }
                        for error_code in [403, 404]
                    ]
                }
            },
            'aws_cloudfront_origin_access_control': {
                bucket: {
                    'name': bucket_origin_id(bucket),
                    'description': '',  # becomes 'Managed by Terraform' if omitted
                    'origin_access_control_origin_type': 's3',
                    'signing_behavior': 'always',
                    'signing_protocol': 'sigv4'
                }
                for bucket in buckets
            },
            'aws_cloudfront_origin_request_policy': {
                'google_search': {
                    'depends_on': ['google_project_service.customsearch'],
                    'name': config.qualified_resource_name('portal_search'),
                    'headers_config': {
                        'header_behavior': 'whitelist',
                        'headers': {
                            'items': ['Referer']
                        }
                    },
                    'query_strings_config': {
                        'query_string_behavior': 'all'
                    },
                    'cookies_config': {
                        'cookie_behavior': 'none'
                    }
                }
            },
            'aws_cloudfront_function': {
                script.stem: cloudfront_function(script)
                for script in Path(__file__).parent.glob('*.js')
            },
            'aws_acm_certificate': {
                'portal': {
                    'domain_name': config.domain_name,
                    'validation_method': 'DNS',
                    'lifecycle': {
                        'create_before_destroy': True
                    }
                }
            },
            'aws_acm_certificate_validation': {
                'portal': {
                    'certificate_arn': '${aws_acm_certificate.portal.arn}',
                    'validation_record_fqdns': '${[for r in aws_route53_record.portal_validation : r.fqdn]}',
                }
            },
            'aws_route53_record': {
                'portal': {
                    'zone_id': '${data.aws_route53_zone.portal.id}',
                    'name': config.domain_name,
                    'type': 'A',
                    'alias': {
                        'name': '${aws_cloudfront_distribution.portal.domain_name}',
                        'zone_id': '${aws_cloudfront_distribution.portal.hosted_zone_id}',
                        'evaluate_target_health': False
                    }
                },
                'portal_validation': {
                    'for_each': '${{'
                                'for o in aws_acm_certificate.portal.domain_validation_options : '
                                'o.domain_name => o'
                                '}}',
                    'name': '${each.value.resource_record_name}',
                    'type': '${each.value.resource_record_type}',
                    'zone_id': '${data.aws_route53_zone.portal.id}',
                    'records': [
                        '${each.value.resource_record_value}',
                    ],
                    'ttl': 60
                }
            },
            'google_project_service': {
                api: {
                    'service': f'{api}.googleapis.com',
                    'disable_dependent_services': False,
                    'disable_on_destroy': False,
                } for api in ['apikeys', 'customsearch']
            },
            'google_apikeys_key': {
                'google_search': {
                    'depends_on': ['google_project_service.apikeys'],
                    **{k: config.qualified_resource_name('portal') for k in ['name', 'display_name']},
                    'project': '${local.google_project}',
                    'restrictions': {
                        'api_targets': [
                            {
                                'service': 'customsearch.googleapis.com'
                            }
                        ],
                        'browser_key_restrictions': {
                            'allowed_referrers': list(flatten(
                                [f'https://{domain}', f'https://{domain}/*']
                                for domain in {
                                    'prod': [
                                        'data-browser.lungmap.net',
                                        config.domain_name
                                    ],
                                }.get(config.deployment_stage, [config.domain_name])
                            ))
                        }
                    }
                }
            },
            'aws_s3_object': {
                # The site deployment below needs to be triggered whenever the
                # content changes but also when the bucket has been deleted and is
                # being recreated, requiring some sort of ability to detect the
                # creation of a bucket. The `lifecycle.replace_triggered_by`
                # property does not cause replacement when the upstream resource is
                # first created, only when there is a change to an already existing
                # resource, so it of no use in this case. We could work around this
                # if the bucket had an identifying property that would let us
                # distinguish two distinct incarnations. Unfortunately, S3 doesn't
                # offer that either. As a workaround, we create a special object in
                # the bucket and set its content to a pseudo random value. We use
                # `life_cycle.ignore_changes` to make sure that the object is not
                # overwritten with a new random value on every deployment. That
                # object, or rather its `etag` attribute (some hash of the content)
                # then serves as a stand-in for a bucket identifier which we can
                # then use to trigger site deployment and CloudFront invalidation.
                bucket + '_bucket_id': {
                    'bucket': '${aws_s3_bucket.%s.id}' % bucket,
                    'key': bucket_id_key,
                    'content': str(uuid.uuid4()),
                    'lifecycle': {
                        'ignore_changes': ['content']
                    }
                }
                for bucket in buckets
            },
            'null_resource': {
                **{
                    f'deploy_site_{i}': {
                        'triggers': {
                            'tarball_hash': gitlab_helper.tarball_hash(project, branch, site_name),
                            'bucket_id': '${aws_s3_object.%s_bucket_id.etag}' % site['bucket']
                        },
                        'provisioner': {
                            'local-exec': {
                                'when': 'create',
                                'interpreter': ['/bin/bash', '-c'],
                                'command': ' && '.join([
                                    # TF uses concurrent workers so we need to keep the directories
                                    # separate between the null_resource resources.
                                    f'rm -rf out_{i}',
                                    f'mkdir out_{i}',
                                    ' | '.join([
                                        ' '.join([
                                            'curl',
                                            '--fail',
                                            '--silent',
                                            gitlab_helper.curl_auth_flags(),
                                            quote(gitlab_helper.tarball_url(project, branch, site_name))
                                        ]),
                                        ' '.join([
                                            # --transform is specific to GNU Tar, which, on macOS must be installed
                                            # separately (via Homebrew, for example) and is called `gtar` there
                                            '$(type -p gtar tar | head -1)',
                                            '-xvjf -',
                                            f'--transform s#^{site["tarball_path"]}/#{site["real_path"]}/#',
                                            '--show-transformed-names',
                                            f'-C out_{i}'
                                        ])
                                    ]),
                                    ' '.join([
                                        'aws', 's3', 'sync',
                                        '--exclude', bucket_id_key,
                                        '--delete',
                                        f'out_{i}/',
                                        's3://${aws_s3_bucket.%s.id}/' % site['bucket']
                                    ]),
                                    f'rm -rf out_{i}',
                                ])
                            }
                        }
                    }
                    for i, (project, branches) in enumerate(config.browser_sites.items())
                    for branch, sites in branches.items()
                    for site_name, site in sites.items()
                },
                'invalidate_cloudfront': {
                    'depends_on': [
                        f'null_resource.deploy_site_{i}'
                        for i, _ in enumerate(config.browser_sites)
                    ],
                    'triggers': {
                        f'{trigger}_{i}': '${null_resource.deploy_site_%i.triggers.%s}' % (i, trigger)
                        for i, _ in enumerate(config.browser_sites)
                        for trigger in ['tarball_hash', 'bucket_id']
                    },
                    'provisioner': {
                        'local-exec': {
                            'when': 'create',
                            'command': ' '.join([
                                'aws',
                                'cloudfront create-invalidation',
                                '--distribution-id ${aws_cloudfront_distribution.portal.id}',
                                '--paths "/*"'
                            ])
                        }
                    }
                }
            }
        }
    }
    tf_config = enable_s3_bucket_inventory(tf_config)
    tf_config = block_public_s3_bucket_access(tf_config)
    tf_config = set_empty_s3_bucket_lifecycle_config(tf_config)
    emit_tf(tf_config)


def bucket_behaviour(origin, *, path_pattern: str = None, **functions: bool) -> JSON:
    return adict(
        path_pattern=path_pattern,
        allowed_methods=['GET', 'HEAD'],
        cached_methods=['GET', 'HEAD'],
        cache_policy_id='${data.aws_cloudfront_cache_policy.caching_optimized.id}',
        viewer_protocol_policy='redirect-to-https',
        target_origin_id=bucket_origin_id(origin),
        compress=True,
        function_association=[
            {
                'event_type': 'viewer-request' if is_request else 'viewer-response',
                'function_arn': '${aws_cloudfront_function.%s.arn}' % name
            }
            for name, is_request in functions.items()
        ]
    )


def bucket_origin_id(bucket):
    return bucket


def bucket_regional_domain_name(bucket):
    if False:
        return '${aws_s3_bucket.%s.bucket_regional_domain_name}' % bucket  # noqa
    else:
        assert config.region == 'us-east-1'
        # FIXME: Remove workaround for
        #        https://github.com/hashicorp/terraform-provider-aws/issues/15102
        #        https://github.com/DataBiosphere/azul/issues/5257
        return buckets[bucket] + '.s3.us-east-1.amazonaws.com'


def cloudfront_function(script: Path):
    prefix = '//'

    def predicate(s):
        s = s.strip()
        return not s or s.startswith(prefix)

    with script.open() as f:
        comment, source = before_and_after(predicate, f)
        comment = list(filter(None, map(str.strip, comment)))
        comment = comment[0].removeprefix(prefix).strip() if comment else None
        source = ''.join(source)

    return dict(name=config.qualified_resource_name(script.stem),
                comment=comment,
                runtime='cloudfront-js-1.0',
                code=source,
                # publish=False would update the function so that it can be used
                # with the TestFunction API but wouldn't affect what's live.
                publish=True)


def google_search_origin():
    return adict(
        origin_id=google_search_origin_id(),
        domain_name='www.googleapis.com',
        connection_attempts=3,
        connection_timeout=10,
        custom_origin_config=adict(
            origin_protocol_policy='https-only',
            origin_ssl_protocols=['TLSv1.2'],
            https_port=443,
            http_port=80,
            origin_keepalive_timeout=5,
            origin_read_timeout=30,
        )
    )


def google_search_behavior():
    return adict(
        path_pattern='/customsearch*',
        target_origin_id=google_search_origin_id(),
        allowed_methods=['GET', 'HEAD', 'OPTIONS'],
        cached_methods=['GET', 'HEAD'],
        compress=True,
        default_ttl=0,
        max_ttl=0,
        min_ttl=0,
        smooth_streaming=False,
        cache_policy_id='${data.aws_cloudfront_cache_policy.caching_disabled.id}',
        origin_request_policy_id='${aws_cloudfront_origin_request_policy.google_search.id}',
        response_headers_policy_id='${data.aws_cloudfront_response_headers_policy.security_headers.id}',
        viewer_protocol_policy='https-only',
        trusted_key_groups=[],
        trusted_signers=[],
        function_association=adict(
            event_type='viewer-request',
            function_arn='${aws_cloudfront_function.add_google_search_api_key.arn}',
        )
    )


def google_search_origin_id():
    if config.deployment_stage == 'anvildev':
        return 'Google Programmable Search Engine'
    else:
        return 'google_search'


class GitLabHelper:
    gitlab_url = 'https://gitlab.' + config.domain_name

    @cached_property
    def client(self) -> gitlab.Gitlab:
        token = config.gitlab_access_token
        if token is None:
            kwargs = {'job_token': os.environ['CI_JOB_TOKEN']}
        else:
            kwargs = {'private_token': token}
        return gitlab.Gitlab(url=self.gitlab_url, **kwargs)

    def curl_auth_flags(self) -> str:
        token_type, token = 'PRIVATE', config.gitlab_access_token
        if token is None:
            token_type, token = 'JOB', os.environ['CI_JOB_TOKEN']
        header = quote(f'{token_type}-TOKEN: {token}')
        return '--header ' + header

    def tarball_hash(self, project_path: str, branch: str, site_name: str) -> str:
        project = self.client.projects.get(project_path, lazy=True)
        packages = project.packages.list(iterator=True, package_type='generic')
        version = self.tarball_version(branch)
        package = one(p for p in packages if p.version == version)
        package_files = (
            pf
            for pf in package.package_files.list(iterator=True)
            if pf.file_name == self.tarball_file_name(project_path, site_name)
        )
        package_file = max(package_files, key=attrgetter('created_at'))
        return package_file.file_sha256

    def tarball_url(self, project_path: str, branch: str, site_name: str) -> str:
        # GET /projects/:id/packages/generic/:package_name/:package_version/:file_name
        return str(furl(self.gitlab_url,
                        path=[
                            'api', 'v4', 'projects', project_path,
                            'packages', 'generic', 'tarball',
                            self.tarball_version(branch),
                            self.tarball_file_name(project_path, site_name)
                        ]))

    def tarball_file_name(self, project_path: str, site_name: str) -> str:
        return '_'.join([
            project_path.split('/')[-1],  # just the project name
            config.deployment_stage,
            site_name,
            'distribution',
        ]) + '.tar.bz2'

    def tarball_version(self, branch: str) -> str:
        # package_version can't contain slashes
        return branch.replace('/', '.')


def quote(s):
    assert '"' not in s, s
    return '"' + s + '"'


gitlab_helper = GitLabHelper()
del GitLabHelper

if __name__ == '__main__':
    emit()
