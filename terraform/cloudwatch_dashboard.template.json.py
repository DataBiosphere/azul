from textwrap import (
    dedent,
)

from more_itertools import (
    flatten,
)

from azul import (
    config,
)
from azul.lib import (
    R,
)
from azul.deployment import (
    aws,
)
from azul.lib.functions import (
    iif,
)
from azul.lib.types import (
    JSON,
)

es_instance_count = (
    aws.opensearch_instance_count
    if config.share_opensearch_domain else
    config.opensearch_instance_count
)


def dashboard_body(name: str):
    valid_names = ['indexer', *iif(config.enable_mirroring, ['mirror'])]
    assert name in valid_names, R('Invalid dashboard name', name)

    def coordinates(**kwargs: tuple[int, int]) -> JSON:
        """
        Return a dashboard widget's coordinates. Argument keys are dashboard
        names, and values are (col, row) pairs, with (0, 0) representing the
        first cell in the top-left corner of the dashboard.
        """
        try:
            x, y = kwargs[name]
        except KeyError:
            # Since coordinates() is called as part of a call to non-lazy iif()
            # we need to handle the case where coordinates aren't provided for a
            # dashboard that won't include the widget.
            return {}
        else:
            assert x >= 0 and y >= 0, (x, y)
            return {'x': x * 12, 'y': y * 6, 'width': 12, 'height': 6}

    is_indexer = name == 'indexer'
    return {
        'widgets': [
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(1, 1)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                            | filter @message like /Attempt \\d+ of handling \\d+ contribution\\(s\\) for entity/
                                  or @message like /Deferring aggregation of \\d+ contribution\\(s\\) to entity/
                                  or @message like /Successfully aggregated \\d+ contribution\\(s\\) to entity/
                            | parse 'of handling * contribution(s) for entity' as attempts
                            | parse 'Deferring aggregation of * contribution(s) to entity' as deferrals
                            | parse 'Successfully aggregated * contribution(s) to entity' as successes
                            | stats sum(successes) as Successes,
                                    sum(attempts) - sum(successes) - sum(deferrals) as Failures,
                                    sum(deferrals) as Deferrals
                                    by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': True,
                        'title': 'Aggregation outcomes in # of contributions',
                        'view': 'timeSeries'
                    }
                }
            ]),
            {
                **coordinates(indexer=(0, 2), mirror=(0, 2)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                {
                                    'expression': 'nv+ni+nd',
                                    'label': 'notifications',
                                    'id': 'n',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.name,
                                {
                                    'id': 'nv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'ni',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'nrv+nri+nrd',
                                    'label': 'notifications_retry',
                                    'id': 'nr',
                                    'region': config.region,
                                    'color': '#ff7f0e'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.to_retry.name,
                                {
                                    'id': 'nrv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'nri',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nrd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'nfv+nfi+nfd',
                                    'label': 'notifications_fail',
                                    'id': 'nf',
                                    'region': config.region,
                                    'color': '#9467bd'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.to_fail.name,
                                {
                                    'id': 'nfv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'nfi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nfd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'tv+ti+td',
                                    'label': 'tallies',
                                    'id': 't',
                                    'region': config.region,
                                    'color': '#2ca02c'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.name,
                                {
                                    'id': 'tv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'ti',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'td',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'trv+tri+trd',
                                    'label': 'tallies_retry',
                                    'id': 'tr',
                                    'region': config.region,
                                    'color': '#d62728'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.to_retry.name,
                                {
                                    'id': 'trv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'tri',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'trd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'tfv+tfi+tfd',
                                    'label': 'tallies_fail',
                                    'id': 'tf',
                                    'region': config.region,
                                    'color': '#f7b6d2'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.to_fail.name,
                                {
                                    'id': 'tfv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'tfi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'tfd',
                                    'visible': False
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                {
                                    'expression': 'mv+mi+md',
                                    'label': 'mirror',
                                    'id': 'm',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.mirror_queue.name,
                                {
                                    'id': 'mv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'mi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'md',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'mfv+mfi+mfd',
                                    'label': 'mirror_fail',
                                    'id': 'mf',
                                    'region': config.region,
                                    'color': '#9467bd'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.mirror_queue.to_fail.name,
                                {
                                    'id': 'mfv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'mfi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'mfd',
                                    'visible': False
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'title': 'Queue lengths',
                    'period': 300,
                    'stat': 'Maximum'
                }
            },
            {
                **coordinates(indexer=(0, 0), mirror=(0, 0)),
                'type': 'log',
                'properties': {
                    'query': (
                        f"SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'" +
                        f"| SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'"
                        if is_indexer else
                        f"SOURCE '/aws/lambda/{config.indexer_function_name('mirror')}'"
                    ) + dedent(f'''\
                        | fields strcontains(@message, 'Worker successfully handled') as success,
                                 strcontains(@message,'Worker failed to handle message') as failure,
                                 strcontains(@message,'Task timed out after') as timeout
                        | filter failure > 0 or success > 0 or timeout > 0
                        | stats sum(success) as Successes,
                                sum(failure + timeout) as Failures
                                by bin(5min)
                    '''),
                    'region': config.region,
                    'stacked': True,
                    'title': (
                        'Contribution outcomes in # of notifications'
                        if is_indexer else
                        'Mirror outcomes in # of messages'
                    ),
                    'view': 'timeSeries'
                }
            },
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(1, 4)),
                    'type': 'metric',
                    'properties': {
                        'metrics': [
                            [
                                {
                                    'expression': ' + '.join(f'm{2 + i * 2}' for i in range(es_instance_count)),
                                    'label': 'Primary',
                                    'id': 'e1',
                                    'region': config.region,
                                    'color': '#2ca02c'
                                }
                            ],
                            [
                                {
                                    'expression': ' + '.join(f'm{3 + i * 2}' for i in range(es_instance_count)),
                                    'label': 'Replica',
                                    'id': 'e2',
                                    'region': config.region,
                                    'color': '#1f77b4'
                                }
                            ],
                            [
                                'AWS/ES',
                                'Shards.unassigned',
                                'DomainName',
                                config.opensearch_domain,
                                'ClientId',
                                config.aws_account_id,
                                {
                                    'id': 'm1',
                                    'label': 'Unassigned',
                                    'color': '#d62728'
                                }
                            ],
                            [
                                '.',
                                'ShardCount',
                                'ShardRole',
                                'Primary',
                                'DomainName',
                                config.opensearch_domain,
                                'NodeId',
                                '${local.nodes[0]}',
                                'ClientId',
                                config.aws_account_id,
                                {
                                    'id': 'm2',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                'Replica',
                                '.',
                                '.',
                                '.',
                                '.',
                                '.',
                                '.',
                                {
                                    'id': 'm3',
                                    'visible': False
                                }
                            ],
                            *flatten((
                                [
                                    [
                                        '...',
                                        'Primary',
                                        '.',
                                        '.',
                                        '.',
                                        '${local.nodes[%d]}' % i,
                                        '.',
                                        '.',
                                        {
                                            'id': f'm{2 + i * 2}',
                                            'visible': False
                                        }
                                    ],
                                    [
                                        '...',
                                        'Replica',
                                        '.',
                                        '.',
                                        '.',
                                        '.',
                                        '.',
                                        '.',
                                        {
                                            'id': f'm{3 + i * 2}',
                                            'visible': False
                                        }
                                    ]
                                ]
                                for i in range(1, es_instance_count)
                            ))
                        ],
                        'view': 'timeSeries',
                        'stacked': False,
                        'region': config.region,
                        'period': 300,
                        'stat': 'Maximum',
                        'title': 'ES shards'
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(1, 2)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | filter @message like 'TransportError'
                            | fields strcontains(@log, 'contribute') as contribute, 1 - contribute as aggregate
                            | stats sum(contribute) as Contribution, sum(aggregate) as Aggregation by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': False,
                        'title': 'ES TransportErrors',
                        'view': 'timeSeries'
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(1, 5)),
                    'type': 'metric',
                    'properties': {
                        'view': 'timeSeries',
                        'stacked': True,
                        'metrics': [
                            [
                                'AWS/ES',
                                'JVMMemoryPressure',
                                'DomainName',
                                config.opensearch_domain,
                                'NodeId',
                                '${local.nodes[0]}',
                                'ClientId',
                                config.aws_account_id
                            ],
                            *(
                                [
                                    '...',
                                    '${local.nodes[%d]}' % i,
                                    '.',
                                    '.'
                                ]
                                for i in range(1, es_instance_count)
                            )
                        ],
                        'region': config.region,
                        'title': 'ES JVM memory pressure [%]',
                        'period': 300
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(1, 6)),
                    'type': 'metric',
                    'properties': {
                        'metrics': [
                            [
                                {
                                    'expression': 'DIFF(%s)/4/1000/60/5*100' %
                                                  '+'.join(f'm{i + 1}' for i in range(es_instance_count)),
                                    'label': 'Old generation',
                                    'id': 'e2',
                                    'region': config.region,
                                    'stat': 'Maximum'
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(%s)/4/1000/60/5*100' % '+'.join(
                                        f'm{i + es_instance_count + 1}'
                                        for i in range(es_instance_count)
                                    ),
                                    'label': 'Young generation',
                                    'id': 'e1',
                                    'region': config.region,
                                    'stat': 'Maximum',
                                    'yAxis': 'left'
                                }
                            ],
                            [
                                'AWS/ES',
                                'JVMGCOldCollectionTime',
                                'DomainName',
                                config.opensearch_domain,
                                'NodeId',
                                '${local.nodes[0]}',
                                'ClientId',
                                config.aws_account_id,
                                {
                                    'id': 'm1',
                                    'visible': False
                                }
                            ],
                            *(
                                [
                                    '...',
                                    '${local.nodes[%d]}' % i,
                                    '.',
                                    '.',
                                    {
                                        'id': f'm{i + 1}',
                                        'visible': False
                                    }
                                ]
                                for i in range(1, es_instance_count)
                            ),
                            [
                                '.',
                                'JVMGCYoungCollectionTime',
                                '.',
                                '.',
                                '.',
                                '${local.nodes[0]}',
                                '.',
                                '.',
                                {
                                    'id': f'm{es_instance_count + 1}',
                                    'visible': False
                                }
                            ],
                            *(
                                [
                                    '...',
                                    '${local.nodes[%d]}' % i,
                                    '.',
                                    '.',
                                    {
                                        'id': f'm{i + es_instance_count + 1}',
                                        'visible': False
                                    }
                                ]
                                for i in range(1, es_instance_count)
                            )
                        ],
                        'view': 'timeSeries',
                        'stacked': True,
                        'region': config.region,
                        'period': 300,
                        'stat': 'Maximum',
                        'title': 'ES JVM garbage collection time',
                        'yAxis': {
                            'left': {
                                'label': '% of wall clock time',
                                'showUnits': False
                            },
                            'right': {
                                'showUnits': False
                            }
                        }
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(0, 5)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | fields @log
                            | parse 'It took *s to download' as duration
                            | filter ispresent(duration)
                            | fields strcontains(@log, '_retry') as is_retry
                            | stats avg(duration * (1 - is_retry)) as Initial,
                                    avg(duration * is_retry) as Retry
                                    by bin(5m)
                        '''),
                        'region': config.region,
                        'stacked': False,
                        'title': 'Subgraph download time, average [s]',
                        'view': 'timeSeries'
                    }
                }
            ]),
            {
                **coordinates(indexer=(1, 12), mirror=(1, 4)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                'AWS/Lambda',
                                'Throttles',
                                'FunctionName',
                                config.indexer_function_name('contribute'),
                                {
                                    'label': 'contribute'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'label': 'contribute_retry'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate'),
                                {
                                    'label': 'aggregate'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'label': 'aggregate_retry'
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                'AWS/Lambda',
                                'Throttles',
                                'FunctionName',
                                config.indexer_function_name('mirror'),
                                {
                                    'label': 'mirror'
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'stat': 'Sum',
                    'period': 300,
                    'title': 'Lambda throttles'
                }
            },
            {
                **coordinates(indexer=(1, 10), mirror=(1, 5)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                'AWS/Lambda',
                                'Errors',
                                'FunctionName',
                                config.indexer_function_name('contribute'),
                                {
                                    'label': 'contribute',
                                    'region': config.region
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'label': 'contribute_retry',
                                    'region': config.region
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate'),
                                {
                                    'label': 'aggregate',
                                    'region': config.region
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'label': 'aggregate_retry',
                                    'region': config.region
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                'AWS/Lambda',
                                'Errors',
                                'FunctionName',
                                config.indexer_function_name('mirror'),
                                {
                                    'label': 'mirror',
                                    'region': config.region
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'stat': 'Sum',
                    'period': 300,
                    'title': 'Lambda errors'
                }
            },
            {
                **coordinates(indexer=(1, 8), mirror=(1, 2)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                'AWS/Lambda',
                                'Invocations',
                                'FunctionName',
                                config.indexer_function_name('contribute'),
                                {
                                    'label': 'contribute'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'label': 'contribute_retry'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate'),
                                {
                                    'label': 'aggregate'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'label': 'aggregate_retry'
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                'AWS/Lambda',
                                'Invocations',
                                'FunctionName',
                                config.indexer_function_name('mirror'),
                                {
                                    'label': 'mirror'
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'stat': 'Sum',
                    'period': 300,
                    'title': 'Lambda invocations'
                }
            },
            {
                **coordinates(indexer=(1, 9), mirror=(1, 3)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                'AWS/Lambda',
                                'ConcurrentExecutions',
                                'FunctionName',
                                config.indexer_function_name('contribute'),
                                {
                                    'label': 'contribute'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'label': 'contribute_retry'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate'),
                                {
                                    'label': 'aggregate'
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'label': 'aggregate_retry'
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                'AWS/Lambda',
                                'ConcurrentExecutions',
                                'FunctionName',
                                config.indexer_function_name('mirror'),
                                {
                                    'label': 'mirror'
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'stat': 'Maximum',
                    'period': 300,
                    'title': 'Concurrent Lambda executions'
                }
            },
            {
                **coordinates(indexer=(1, 7), mirror=(1, 1)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                {
                                    'expression': 'm3 / 1000',
                                    'label': 'contribute',
                                    'id': 'e1',
                                    'stat': 'Average',
                                    'region': config.region
                                }
                            ],
                            [
                                {
                                    'expression': 'm4 / 1000',
                                    'label': 'contribute_retry',
                                    'id': 'e2',
                                    'stat': 'Average',
                                    'region': config.region
                                }
                            ],
                            [
                                {
                                    'expression': 'm1 / 1000',
                                    'label': 'aggregate',
                                    'id': 'e3',
                                    'stat': 'Average',
                                    'region': config.region
                                }
                            ],
                            [
                                {
                                    'expression': 'm2 / 1000',
                                    'label': 'aggregate_retry',
                                    'id': 'e4',
                                    'stat': 'Average',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/Lambda',
                                'Duration',
                                'FunctionName',
                                config.indexer_function_name('aggregate'),
                                {
                                    'id': 'm1',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'id': 'm2',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute'),
                                {
                                    'id': 'm3',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'id': 'm4',
                                    'visible': False
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                {
                                    'expression': 'm1 / 1000',
                                    'label': 'mirror',
                                    'id': 'e1',
                                    'stat': 'Average',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/Lambda',
                                'Duration',
                                'FunctionName',
                                config.indexer_function_name('mirror'),
                                {
                                    'id': 'm1',
                                    'visible': False
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'stat': 'Average',
                    'period': 300,
                    'title': 'Lambda duration [s]',
                    'yAxis': {
                        'left': {
                            'showUnits': False
                        }
                    }
                }
            },
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(0, 6)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | filter @message like 'Exceeded rate limits'
                            | sort @timestamp desc
                            | stats count(@requestId) as trips by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': False,
                        'title': 'BQ rate limit trips',
                        'view': 'timeSeries'
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(0, 4)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | filter ispresent(stats.totalSlotMs)
                            | stats sum(stats.totalSlotMs) / 1000 / 3600 * 12 as `slot hours` by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': False,
                        'title': 'BQ slot-hours (pro-rated)',
                        'view': 'timeSeries'
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(0, 7)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | filter @message like 'Exceeded rate limits'
                            | parse 'BigQuery job error during attempt *. Retrying in *s.' as a, d
                            | filter ispresent(d)
                            | stats avg(d) as Delay by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': False,
                        'title': 'BQ rate limit back-off, average [s]',
                        'view': 'timeSeries'
                    }
                }
            ]),
            {
                **coordinates(indexer=(0, 3), mirror=(0, 3)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                {
                                    'expression': 'DIFF(nv+ni+nd)',
                                    'label': 'notifications',
                                    'id': 'n',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.name,
                                {
                                    'id': 'nv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'ni',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(nrv+nri+nrd)',
                                    'label': 'notifications_retry',
                                    'id': 'nr',
                                    'region': config.region,
                                    'color': '#ff7f0e'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.to_retry.name,
                                {
                                    'id': 'nrv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'nri',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nrd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(nfv+nfi+nfd)',
                                    'label': 'notifications_fail',
                                    'id': 'nf',
                                    'region': config.region,
                                    'color': '#9467bd'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.to_fail.name,
                                {
                                    'id': 'nfv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'nfi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nfd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(tv+ti+td)',
                                    'label': 'tallies',
                                    'id': 't',
                                    'region': config.region,
                                    'color': '#2ca02c'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.name,
                                {
                                    'id': 'tv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'ti',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'td',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(trv+tri+trd)',
                                    'label': 'tallies_retry',
                                    'id': 'tr',
                                    'region': config.region,
                                    'color': '#d62728'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.to_retry.name,
                                {
                                    'id': 'trv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'tri',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'trd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(tfv+tfi+tfd)',
                                    'label': 'tallies_fail',
                                    'id': 'tf',
                                    'region': config.region,
                                    'color': '#f7b6d2'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.to_fail.name,
                                {
                                    'id': 'tfv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'tfi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'tfd',
                                    'visible': False
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                {
                                    'expression': 'DIFF(mv+mi+md)',
                                    'label': 'mirror',
                                    'id': 'm',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.mirror_queue.name,
                                {
                                    'id': 'mv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'mi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'md',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'DIFF(mfv+mfi+mfd)',
                                    'label': 'mirror_fail',
                                    'id': 'mf',
                                    'region': config.region,
                                    'color': '#9467bd'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.mirror_queue.to_fail.name,
                                {
                                    'id': 'mfv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'mfi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'mfd',
                                    'visible': False
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'title': 'Queue length Δ',
                    'period': 300,
                    'stat': 'Maximum',
                    'annotations': {
                        'horizontal': [
                            {
                                'color': '#aec7e8',
                                'value': 0
                            }
                        ]
                    }
                }
            },
            {
                **coordinates(indexer=(0, 9), mirror=(0, 4)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                {
                                    'expression': 'CEIL(m1 * 20 / PERIOD(m1))',
                                    'label': 'notifications',
                                    'id': 'e1',
                                    'region': config.region
                                }
                            ],
                            [
                                {
                                    'expression': 'CEIL(m2 * 20 / PERIOD(m2))',
                                    'label': 'notifications_retry',
                                    'id': 'e2',
                                    'region': config.region
                                }
                            ],
                            [
                                {
                                    'expression': 'CEIL(m3 * 20 / PERIOD(m3))',
                                    'label': 'tallies.fifo',
                                    'id': 'e3',
                                    'region': config.region
                                }
                            ],
                            [
                                {
                                    'expression': 'CEIL(m4 * 20 / PERIOD(m4))',
                                    'label': 'tallies_retry.fifo',
                                    'id': 'e4',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'NumberOfEmptyReceives',
                                'QueueName',
                                config.notifications_queue.name,
                                {
                                    'id': 'm1',
                                    'visible': False,
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'NumberOfEmptyReceives',
                                'QueueName',
                                config.notifications_queue.to_retry.name,
                                {
                                    'id': 'm2',
                                    'visible': False,
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'NumberOfEmptyReceives',
                                'QueueName',
                                config.tallies_queue.name,
                                {
                                    'id': 'm3',
                                    'visible': False,
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'NumberOfEmptyReceives',
                                'QueueName',
                                config.tallies_queue.to_retry.name,
                                {
                                    'id': 'm4',
                                    'visible': False,
                                    'region': config.region
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                {
                                    'expression': 'CEIL(m1 * 20 / PERIOD(m1))',
                                    'label': 'mirror',
                                    'id': 'e1',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'NumberOfEmptyReceives',
                                'QueueName',
                                config.mirror_queue.name,
                                {
                                    'id': 'm1',
                                    'visible': False,
                                    'region': config.region
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'title': 'Idle queue polling threads',
                    'period': 300,
                    'stat': 'Sum'
                }
            },
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(1, 0)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                            | filter @message like /Attempt \\d+ of handling \\d+ contribution\\(s\\) for entity/
                                  or @message like /Deferring \\d+ tallies/
                                  or @message like /Successfully referred \\d+ tallies/
                            | field strcontains(@message,'Attempt') and strcontains(@message,'contribution(s) for entity') as attempts
                            | parse 'Deferring * tallies' as deferrals
                            | parse 'Successfully referred * tallies' as successes
                            | stats sum(successes) as Successes,
                                    sum(attempts) - sum(successes) - sum(deferrals) as Failures,
                                    sum(deferrals) as Deferrals
                                    by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': True,
                        'title': 'Aggregation outcomes in # of tallies',
                        'view': 'timeSeries'
                    }
                }
            ]),
            *iif(is_indexer, [
                {
                    **coordinates(indexer=(0, 8)),
                    'type': 'log',
                    'properties': {
                        'query': dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | fields stats.cacheHit, strcontains(@log, 'retry') as is_retry
                            | filter @message like 'Job info: '
                            | sort @timestamp desc
                            | stats sum(stats.cacheHit * (1 - is_retry)) / sum(1 - is_retry) * 100 as Initial,
                                    sum(stats.cacheHit * is_retry ) / sum(is_retry) * 100 as Retry
                                    by bin(5min)
                        '''),
                        'region': config.region,
                        'stacked': False,
                        'title': 'BQ cache utilization [%]',
                        'view': 'timeSeries'
                    }
                }
            ]),
            {
                **coordinates(indexer=(0, 1), mirror=(0, 1)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                {
                                    'expression': 'IF(DIFF(nv+ni+nd+nrv+nri+nrd) < 0, ((nv+ni+nd+nrv+nri+nrd) / -DIFF(nv+ni+nd+nrv+nri+nrd)) * DIFF_TIME(nv+ni+nd+nrv+nri+nrd) / 3600)',
                                    'label': 'notifications',
                                    'id': 'n',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.notifications_queue.name,
                                {
                                    'id': 'nv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'ni',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nd',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesVisible',
                                '.',
                                config.notifications_queue.to_retry.name,
                                {
                                    'id': 'nrv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'nri',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'nrd',
                                    'visible': False
                                }
                            ],
                            [
                                {
                                    'expression': 'IF(DIFF(tv+ti+td+trv+tri+trd) < 0, ((tv+ti+td+trv+tri+trd) / -DIFF(tv+ti+td+trv+tri+trd)) * DIFF_TIME(tv+ti+td+trv+tri+trd) / 3600)',
                                    'label': 'tallies',
                                    'id': 't',
                                    'region': config.region,
                                    'color': '#2ca02c'
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.tallies_queue.name,
                                {
                                    'id': 'tv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'ti',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'td',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesVisible',
                                '.',
                                config.tallies_queue.to_retry.name,
                                {
                                    'id': 'trv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'tri',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'trd',
                                    'visible': False
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                {
                                    'expression': 'IF(DIFF(mv+mi+md) < 0, ((mv+mi+md) / -DIFF(mv+mi+md)) * DIFF_TIME(mv+mi+md) / 3600)',
                                    'label': 'mirror',
                                    'id': 'm',
                                    'region': config.region
                                }
                            ],
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesVisible',
                                'QueueName',
                                config.mirror_queue.name,
                                {
                                    'id': 'mv',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesNotVisible',
                                '.',
                                '.',
                                {
                                    'id': 'mi',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'ApproximateNumberOfMessagesDelayed',
                                '.',
                                '.',
                                {
                                    'id': 'md',
                                    'visible': False
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'title': 'ETA [h]',
                    'period': 300,
                    'stat': 'Maximum'
                }
            },
            {
                **coordinates(indexer=(1, 11), mirror=(1, 6)),
                'type': 'log',
                'properties': {
                    'query': (
                        dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | filter @message like 'Status: timeout'
                            | fields strcontains(@log, 'contribute') == 1 and strcontains(@log, 'retry') == 0 as c
                            | fields strcontains(@log, 'contribute_retry') == 1 as cr
                            | fields strcontains(@log, 'aggregate') == 1 and strcontains(@log, 'retry') == 0 as a
                            | fields strcontains(@log, 'aggregate_retry') == 1  as ar
                            | stats sum(c) as contribute,
                                    sum(cr) as contribute_retry,
                                    sum(a) as aggregate,
                                    sum(ar) as aggregate_retry
                                    by bin(5min)
                        ''')
                        if is_indexer else
                        dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('mirror')}'
                            | filter @message like 'Status: timeout'
                            | fields strcontains(@log, 'mirror') == 1 as m
                            | stats sum(m) as mirror
                                    by bin(5min)
                        ''')
                    ),
                    'region': config.region,
                    'stacked': False,
                    'title': 'Lambda timeouts',
                    'view': 'timeSeries'
                }
            },
            {
                **coordinates(indexer=(1, 3), mirror=(1, 0)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesNotVisible',
                                'QueueName',
                                config.notifications_queue.name,
                                {
                                    'label': 'notifications'
                                }
                            ],
                            [
                                '...',
                                config.notifications_queue.to_retry.name,
                                {
                                    'label': 'notifications_retry'
                                }
                            ],
                            [
                                '...',
                                config.tallies_queue.name,
                                {
                                    'label': 'tallies'
                                }
                            ],
                            [
                                '...',
                                config.tallies_queue.to_retry.name,
                                {
                                    'label': 'tallies_retry'
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                'AWS/SQS',
                                'ApproximateNumberOfMessagesNotVisible',
                                'QueueName',
                                config.mirror_queue.name,
                                {
                                    'label': 'mirror'
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'title': 'In-flight messages',
                    'period': 300,
                    'stat': 'Average'
                }
            },
            {
                **coordinates(indexer=(0, 10), mirror=(0, 5)),
                'type': 'metric',
                'properties': {
                    'metrics': (
                        [
                            [
                                {
                                    'expression': 'm1 * 100 / m5',
                                    'label': 'contribute',
                                    'id': 'e1'
                                }
                            ],
                            [
                                {
                                    'expression': 'm2 * 100 / m6',
                                    'label': 'contribute_retry',
                                    'id': 'e2'
                                }
                            ],
                            [
                                {
                                    'expression': 'm3 * 100 / m7',
                                    'label': 'aggregate',
                                    'id': 'e3'
                                }
                            ],
                            [
                                {
                                    'expression': 'm4 * 100 / m8',
                                    'label': 'aggregate_retry',
                                    'id': 'e4'
                                }
                            ],
                            [
                                'AWS/Lambda',
                                'Errors',
                                'FunctionName',
                                config.indexer_function_name('contribute'),
                                {
                                    'label': 'contribute',
                                    'id': 'm1',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'label': 'contribute_retry',
                                    'id': 'm2',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate'),
                                {
                                    'label': 'aggregate',
                                    'id': 'm3',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'label': 'aggregate_retry',
                                    'id': 'm4',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'Invocations',
                                '.',
                                config.indexer_function_name('contribute'),
                                {
                                    'id': 'm5',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('contribute_retry'),
                                {
                                    'id': 'm6',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate'),
                                {
                                    'id': 'm7',
                                    'visible': False
                                }
                            ],
                            [
                                '...',
                                config.indexer_function_name('aggregate_retry'),
                                {
                                    'id': 'm8',
                                    'visible': False
                                }
                            ]
                        ]
                        if is_indexer else
                        [
                            [
                                {
                                    'expression': 'm1 * 100 / m2',
                                    'label': 'mirror',
                                    'id': 'e1'
                                }
                            ],
                            [
                                'AWS/Lambda',
                                'Errors',
                                'FunctionName',
                                config.indexer_function_name('mirror'),
                                {
                                    'label': 'mirror',
                                    'id': 'm1',
                                    'visible': False
                                }
                            ],
                            [
                                '.',
                                'Invocations',
                                '.',
                                config.indexer_function_name('mirror'),
                                {
                                    'id': 'm2',
                                    'visible': False
                                }
                            ]
                        ]
                    ),
                    'view': 'timeSeries',
                    'stacked': False,
                    'region': config.region,
                    'stat': 'Sum',
                    'period': 300,
                    'title': 'Lambda error rate [%]'
                }
            },
            {
                **coordinates(indexer=(0, 11), mirror=(0, 6)),
                'type': 'log',
                'properties': {
                    'query': (
                        dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                            | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                            | filter @message like 'Status: timeout' or @message like 'START'
                            | fields strcontains(@message, 'Status: timeout') == 1 as timeout
                            | fields strcontains(@message, 'START') == 1 as attempt
                            | fields strcontains(@log, 'contribute') == 1 and strcontains(@log, 'retry') == 0 as c
                            | fields strcontains(@log, 'contribute_retry') == 1 as cr
                            | fields strcontains(@log, 'aggregate') == 1 and strcontains(@log, 'retry') == 0 as a
                            | fields strcontains(@log, 'aggregate_retry') == 1  as ar
                            | stats sum(c*timeout) * 100 / sum(c*attempt) as contribute,
                                    sum(cr*timeout) * 100 / sum(cr*attempt) as contribute_retry,
                                    sum(a*timeout) * 100 / sum(a*attempt) as aggregate,
                                    sum(ar*timeout) * 100 / sum(ar*attempt) as aggregate_retry
                                    by bin(5min)
                        ''')
                        if is_indexer else
                        dedent(f'''\
                            SOURCE '/aws/lambda/{config.indexer_function_name('mirror')}'
                            | filter @message like 'Status: timeout' or @message like 'START'
                            | fields strcontains(@message, 'Status: timeout') == 1 as timeout
                            | fields strcontains(@message, 'START') == 1 as attempt
                            | fields strcontains(@log, 'mirror') == 1 as m
                            | stats sum(m*timeout) * 100 / sum(m*attempt) as mirror
                                    by bin(5min)
                        ''')
                    ),
                    'region': config.region,
                    'stacked': False,
                    'title': 'Lambda timeout rate [%]',
                    'view': 'timeSeries'
                }
            }
        ]
    }
