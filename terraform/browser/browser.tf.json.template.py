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
    iif,
)
from azul.collections import (
    adict,
    dict_merge,
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

sites = config.browser_sites

#: Whether to emit a Google custom search instance and a CF origin for it
provision_custom_search = False


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
                name: {
                    'name': site['zone'] + '.',
                    'private_zone': False
                }
                for name, site in sites.items()
            }
        },
        'resource': {
            'aws_s3_bucket': {
                name: {
                    'bucket': aws.qualified_bucket_name(name),
                    'force_destroy': True,
                    'lifecycle': {
                        'prevent_destroy': False
                    }
                }
                for name in sites.keys()
            },
            'aws_s3_bucket_logging': {
                name: {
                    'bucket': '${aws_s3_bucket.%s.id}' % name,
                    'target_bucket': '${data.aws_s3_bucket.logs.id}',
                    # Other S3 log deliveries, like ELB, implicitly put a slash
                    # after the prefix. S3 doesn't, so we add one explicitly.
                    'target_prefix': config.s3_access_log_path_prefix(name) + '/'
                }
                for name in sites.keys()
            },
            'aws_s3_bucket_policy': {
                name: {
                    'bucket': '${aws_s3_bucket.%s.id}' % name,
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
                                'Action': [
                                    's3:GetObject',
                                    's3:ListBucket'
                                ],
                                'Resource': [
                                    '${aws_s3_bucket.%s.arn}' % name,
                                    '${aws_s3_bucket.%s.arn}/*' % name
                                ],
                                'Condition': {
                                    'StringEquals': {
                                        'AWS:SourceArn': '${aws_cloudfront_distribution.%s.arn}' % name
                                    }
                                }
                            }
                        ]
                    })
                }
                for name in sites.keys()
            },
            'aws_cloudfront_distribution': {
                name: {
                    'enabled': True,
                    'restrictions': {
                        'geo_restriction': {
                            'locations': [],
                            'restriction_type': 'none'
                        }
                    },
                    'price_class': 'PriceClass_100',
                    'aliases': [site['domain']],
                    'default_root_object': 'index.html',
                    'is_ipv6_enabled': True,
                    'ordered_cache_behavior': [
                        *iif(provision_custom_search, [google_search_behavior()])
                    ],
                    'default_cache_behavior':
                        bucket_behaviour(name,
                                         bucket_path_mapper=True,
                                         add_response_headers=False),
                    'viewer_certificate': {
                        'acm_certificate_arn': '${aws_acm_certificate.%s.arn}' % name,
                        'minimum_protocol_version': 'TLSv1.2_2021',
                        'ssl_support_method': 'sni-only'
                    },
                    'origin': [
                        *(
                            {
                                'origin_id': bucket_origin_id(name),
                                'domain_name': bucket_regional_domain_name(name),
                                'origin_access_control_id':
                                    '${aws_cloudfront_origin_access_control.%s.id}' % name
                            }
                            for name in sites.keys()
                        ),
                        *iif(provision_custom_search, [google_search_origin()])
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
                for name, site in sites.items()
            },
            'aws_cloudfront_origin_access_control': {
                name: {
                    'name': bucket_origin_id(name),
                    'description': '',  # becomes 'Managed by Terraform' if omitted
                    'origin_access_control_origin_type': 's3',
                    'signing_behavior': 'always',
                    'signing_protocol': 'sigv4'
                }
                for name in sites.keys()
            },
            'aws_cloudfront_function': {
                script.stem: cloudfront_function(script)
                for script in Path(__file__).parent.glob('*.js')
            },
            'aws_cloudfront_response_headers_policy': {
                name: {
                    'name': name,
                    'security_headers_config': {
                        'content_security_policy': {
                            'override': True,
                            'content_security_policy': content_security_policy()
                        },
                        'content_type_options': {
                            'override': True
                        },
                        'frame_options': {
                            'override': False,
                            'frame_option': 'DENY'
                        },
                        'referrer_policy': {
                            'override': False,
                            'referrer_policy': 'strict-origin-when-cross-origin'
                        },
                        'strict_transport_security': {
                            'override': False,
                            'access_control_max_age_sec': 63072000,
                            'include_subdomains': True,
                            'preload': True

                        },
                        'xss_protection': {
                            'override': False,
                            'protection': True,
                            'mode_block': True
                        }
                    }
                }
                for name in sites.keys()
            },
            'aws_acm_certificate': {
                name: {
                    'domain_name': site['domain'],
                    'validation_method': 'DNS',
                    'lifecycle': {
                        'create_before_destroy': True
                    }
                }
                for name, site in sites.items()
            },
            'aws_acm_certificate_validation': {
                name: {
                    'certificate_arn': '${aws_acm_certificate.%s.arn}' % name,
                    'validation_record_fqdns': '${[for r in aws_route53_record.%s_validation : r.fqdn]}' % name,
                }
                for name in sites.keys()
            },
            'aws_route53_record': dict_merge(
                {
                    name: {
                        'zone_id': '${data.aws_route53_zone.%s.id}' % name,
                        'name': site['domain'],
                        'type': 'A',
                        'alias': {
                            'name': '${aws_cloudfront_distribution.%s.domain_name}' % name,
                            'zone_id': '${aws_cloudfront_distribution.%s.hosted_zone_id}' % name,
                            'evaluate_target_health': False
                        }
                    },
                    name + '_validation': {
                        'for_each': '${{'
                                    'for o in aws_acm_certificate.%s.domain_validation_options : '
                                    'o.domain_name => o'
                                    '}}' % name,
                        'name': '${each.value.resource_record_name}',
                        'type': '${each.value.resource_record_type}',
                        'zone_id': '${data.aws_route53_zone.%s.id}' % name,
                        'records': [
                            '${each.value.resource_record_value}',
                        ],
                        'ttl': 60
                    }
                }
                for name, site in sites.items()
            ),
            **iif(provision_custom_search, {
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
                }
            }),
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
                name + '_bucket_id': {
                    'bucket': '${aws_s3_bucket.%s.id}' % name,
                    'key': bucket_id_key,
                    'content': str(uuid.uuid4()),
                    'lifecycle': {
                        'ignore_changes': ['content']
                    }
                }
                for name in sites.keys()
            },
            'null_resource': {
                **{
                    f'deploy_site_{name}': {
                        'triggers': {
                            'tarball_hash': gitlab_helper.tarball_hash(site),
                            'bucket_id': '${aws_s3_object.%s_bucket_id.etag}' % name,
                            'tarball_path': site['tarball_path'],
                            'real_path': site['real_path']
                        },
                        'provisioner': {
                            'local-exec': {
                                'when': 'create',
                                'interpreter': ['/bin/bash', '-c'],
                                'command': ' && '.join([
                                    # TF uses concurrent workers so we need to keep the directories
                                    # separate between the null_resource resources.
                                    f'rm -rf out_{name}',
                                    f'mkdir out_{name}',
                                    ' | '.join([
                                        ' '.join([
                                            'curl',
                                            '--fail',
                                            '--silent',
                                            gitlab_helper.curl_auth_flags(),
                                            quote(gitlab_helper.tarball_url(site))
                                        ]),
                                        ' '.join([
                                            # --transform is specific to GNU Tar, which, on macOS must be installed
                                            # separately (via Homebrew, for example) and is called `gtar` there
                                            '$(type -p gtar tar | head -1)',
                                            '-xvjf -',
                                            f'--transform s#^{site["tarball_path"]}/#{site["real_path"]}/#',
                                            '--show-transformed-names',
                                            f'-C out_{name}'
                                        ])
                                    ]),
                                    ' '.join([
                                        'aws', 's3', 'sync',
                                        '--exclude', bucket_id_key,
                                        '--delete',
                                        f'out_{name}/',
                                        's3://${aws_s3_bucket.%s.id}/' % name
                                    ]),
                                    f'rm -rf out_{name}',
                                ])
                            }
                        }
                    }
                    for name, site in sites.items()
                },
                **{
                    'invalidate_cloudfront_' + name: {
                        'depends_on': [
                            f'null_resource.deploy_site_{name}'
                        ],
                        'triggers': {
                            f'{trigger}_{name}': '${null_resource.deploy_site_%s.triggers.%s}' % (name, trigger)
                            for trigger in ['tarball_hash', 'bucket_id']
                        },
                        'provisioner': {
                            'local-exec': {
                                'when': 'create',
                                'command': ' '.join([
                                    'aws',
                                    'cloudfront create-invalidation',
                                    '--distribution-id ${aws_cloudfront_distribution.%s.id}' % name,
                                    '--paths "/*"'
                                ])
                            }
                        }
                    }
                    for name in sites.keys()
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
        response_headers_policy_id=(
            '${aws_cloudfront_response_headers_policy.%s.id}' % origin
        ),
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
        return aws.qualified_bucket_name(bucket) + '.s3.us-east-1.amazonaws.com'


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
        # I tried Terrafornm's try() but it won't catch undefined references
        if provision_custom_search:
            source = source.replace('{GOOGLE_SEARCH_API_KEY}',
                                    '${google_apikeys_key.google_search.key_string}')

    return dict(name=config.qualified_resource_name(script.stem),
                comment=comment,
                runtime='cloudfront-js-1.0',
                code=source,
                # publish=False would update the function so that it can be used
                # with the TestFunction API but wouldn't affect what's live.
                publish=True)


def content_security_policy() -> str:
    def q(s: str) -> str:
        return f"'{s}'"

    def s(*args: str) -> str:
        return ' '.join(args)

    self = q('self')
    none = q('none')
    unsafe_inline = q('unsafe-inline')
    unsafe_eval = q('unsafe-eval')

    return ';'.join([
        s('default-src', self),
        s('object-src', none),
        s('frame-src', none),
        s('frame-ancestors', none),
        s('child-src', none),
        s('img-src',
          self,
          'data:',
          'https://lh3.googleusercontent.com',
          'https://www.google-analytics.com',
          'https://www.googletagmanager.com'),
        s('script-src',
          self,
          unsafe_inline,
          unsafe_eval,
          'https://accounts.google.com/gsi/client',
          'https://www.google-analytics.com',
          'https://www.googletagmanager.com'),
        s('style-src',
          self,
          unsafe_inline,
          'https://fonts.googleapis.com',
          'https://p.typekit.net',
          'https://use.typekit.net'),
        s('font-src',
          self,
          'data:',
          'https://fonts.gstatic.com',
          'https://use.typekit.net/af/'),
        s('connect-src',
          self,
          'https://www.google-analytics.com',
          'https://www.googleapis.com/oauth2/v3/userinfo',
          'https://www.googletagmanager.com',
          'https://support.terra.bio/api/v2/',
          str(furl(config.sam_service_url,
                   path='/register/user/v1')),
          str(furl(config.sam_service_url,
                   path='/register/user/v2/self/termsOfServiceDetails')),
          str(furl(config.terra_service_url,
                   path='/api/nih/status')),
          str(config.service_endpoint))
    ])


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

    def tarball_hash(self, site: config.BrowserSite) -> str:
        project = self.client.projects.get(site['project'], lazy=True)
        packages = project.packages.list(iterator=True, package_type='generic')
        version = self.tarball_version(site['branch'])
        package = one(p for p in packages if p.version == version)
        package_files = (
            pf
            for pf in package.package_files.list(iterator=True)
            if pf.file_name == self.tarball_file_name(site)
        )
        package_file = max(package_files, key=attrgetter('created_at'))
        return package_file.file_sha256

    def tarball_url(self, site: config.BrowserSite) -> str:
        # GET /projects/:id/packages/generic/:package_name/:package_version/:file_name
        return str(furl(self.gitlab_url,
                        path=[
                            'api', 'v4', 'projects', site['project'],
                            'packages', 'generic', 'tarball',
                            self.tarball_version(site['branch']),
                            self.tarball_file_name(site)
                        ]))

    def tarball_file_name(self, site: config.BrowserSite) -> str:
        return '_'.join([
            site['project'].split('/')[-1],  # just the project name
            config.deployment_stage,
            site['tarball_name'],
            'distribution',
        ]) + '.tar.bz2'

    def tarball_version(self, branch: str) -> str:
        # package_version can't contain slashes
        return branch.replace('/', '_')


def quote(s):
    assert '"' not in s, s
    return '"' + s + '"'


gitlab_helper = GitLabHelper()
del GitLabHelper

if __name__ == '__main__':
    emit()
