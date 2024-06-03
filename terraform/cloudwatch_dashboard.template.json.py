from textwrap import (
    dedent,
)

from more_itertools import (
    flatten,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)

dashboard_body = {
    'widgets': [
        {
            'height': 6,
            'width': 12,
            'y': 6,
            'x': 12,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 12,
            'x': 0,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                        config.notifications_queue_name(),
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
                        config.notifications_queue_name(retry=True),
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
                        config.notifications_queue_name(fail=True),
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
                        config.tallies_queue_name(),
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
                        config.tallies_queue_name(retry=True),
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
                        config.tallies_queue_name(fail=True),
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
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'title': 'Queue lengths',
                'period': 300,
                'stat': 'Maximum'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 0,
            'x': 0,
            'type': 'log',
            'properties': {
                'query': dedent(f'''\
                    SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
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
                'title': 'Contribution outcomes in # of notifications',
                'view': 'timeSeries'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 24,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
                    [
                        {
                            'expression': ' + '.join(f'm{2 + i * 2}' for i in range(aws.es_instance_count)),
                            'label': 'Primary',
                            'id': 'e1',
                            'region': config.region,
                            'color': '#2ca02c'
                        }
                    ],
                    [
                        {
                            'expression': ' + '.join(f'm{3 + i * 2}' for i in range(aws.es_instance_count)),
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
                        config.es_domain,
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
                        config.es_domain,
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
                        for i in range(1, aws.es_instance_count)
                    ))
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'period': 300,
                'stat': 'Maximum',
                'title': 'ES shards'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 12,
            'x': 12,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 30,
            'x': 12,
            'type': 'metric',
            'properties': {
                'view': 'timeSeries',
                'stacked': True,
                'metrics': [
                    [
                        'AWS/ES',
                        'JVMMemoryPressure',
                        'DomainName',
                        config.es_domain,
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
                        for i in range(1, aws.es_instance_count)
                    )
                ],
                'region': config.region,
                'title': 'ES JVM memory pressure [%]',
                'period': 300
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 36,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
                    [
                        {
                            'expression': 'DIFF(%s)/4/1000/60/5*100' %
                                          '+'.join(f'm{i + 1}' for i in range(aws.es_instance_count)),
                            'label': 'Old generation',
                            'id': 'e2',
                            'region': config.region,
                            'stat': 'Maximum'
                        }
                    ],
                    [
                        {
                            'expression': 'DIFF(%s)/4/1000/60/5*100' % '+'.join(
                                f'm{i + aws.es_instance_count + 1}'
                                for i in range(aws.es_instance_count)
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
                        config.es_domain,
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
                        for i in range(1, aws.es_instance_count)
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
                            'id': f'm{aws.es_instance_count + 1}',
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
                                'id': f'm{i + aws.es_instance_count + 1}',
                                'visible': False
                            }
                        ]
                        for i in range(1, aws.es_instance_count)
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 30,
            'x': 0,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 72,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'stat': 'Sum',
                'period': 300,
                'title': 'Lambda throttles'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 60,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                        'AWS/Lambda',
                        'Errors',
                        'FunctionName',
                        config.indexer_function_name('contribute_retry'),
                        {
                            'label': 'contribute_retry',
                            'region': config.region
                        }
                    ],
                    [
                        'AWS/Lambda',
                        'Errors',
                        'FunctionName',
                        config.indexer_function_name('aggregate'),
                        {
                            'label': 'aggregate',
                            'region': config.region
                        }
                    ],
                    [
                        'AWS/Lambda',
                        'Errors',
                        'FunctionName',
                        config.indexer_function_name('aggregate_retry'),
                        {
                            'label': 'aggregate_retry',
                            'region': config.region
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'stat': 'Sum',
                'period': 300,
                'title': 'Lambda errors'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 48,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'stat': 'Sum',
                'period': 300,
                'title': 'Lambda invocations'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 54,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'stat': 'Maximum',
                'period': 300,
                'title': 'Concurrent Lambda executions'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 42,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                ],
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
        {
            'height': 6,
            'width': 12,
            'y': 36,
            'x': 0,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 24,
            'x': 0,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 42,
            'x': 0,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 18,
            'x': 0,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                        config.notifications_queue_name(),
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
                        config.notifications_queue_name(retry=True),
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
                        config.notifications_queue_name(fail=True),
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
                        config.tallies_queue_name(),
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
                        config.tallies_queue_name(retry=True),
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
                        config.tallies_queue_name(fail=True),
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
                ],
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
            'height': 6,
            'width': 12,
            'y': 54,
            'x': 0,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                        config.notifications_queue_name(),
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
                        config.notifications_queue_name(retry=True),
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
                        config.tallies_queue_name(),
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
                        config.tallies_queue_name(retry=True),
                        {
                            'id': 'm4',
                            'visible': False,
                            'region': config.region
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'title': 'Idle queue polling threads',
                'period': 300,
                'stat': 'Sum'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 0,
            'x': 12,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 48,
            'x': 0,
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
        },
        {
            'height': 6,
            'width': 12,
            'y': 6,
            'x': 0,
            'type': 'metric',
            'properties': {
                'metrics': [
                    [
                        {
                            'expression': '(nv+ni+nd+nrv+nri+nrd) / IF(0<DIFF(nv+ni+nd+nrv+nri+nrd),0,-DIFF(nv+ni+nd+nrv+nri+nrd)) * DIFF_TIME(nv+ni+nd+nrv+nri+nrd) / 3600',
                            'label': 'notifications',
                            'id': 'n',
                            'region': config.region
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        config.notifications_queue_name(),
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
                        config.notifications_queue_name(retry=True),
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
                            'expression': '(tv+ti+td+trv+tri+trd) / IF(0<DIFF(tv+ti+td+trv+tri+trd),0,-DIFF(tv+ti+td+trv+tri+trd)) * DIFF_TIME(tv+ti+td+trv+tri+trd) / 3600',
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
                        config.tallies_queue_name(),
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
                        config.tallies_queue_name(retry=True),
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
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'title': 'ETA [h]',
                'period': 300,
                'stat': 'Maximum'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 66,
            'x': 12,
            'type': 'log',
            'properties': {
                'query': dedent(f'''\
                    SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                    | filter @message like 'Task timed out'
                    | fields strcontains(@log, 'aggregate') == 0 and strcontains(@log, 'retry') == 0 as c
                    | fields strcontains(@log, 'aggregate') == 0 and strcontains(@log, 'retry') == 1 as cr
                    | fields strcontains(@log, 'aggregate') == 1 and strcontains(@log, 'retry') == 0 as a
                    | fields strcontains(@log, 'aggregate') == 1 and strcontains(@log, 'retry') == 1 as ar
                    | stats sum(c) as contribute,
                            sum(cr) as contribute_retry,
                            sum(a) as aggregate,
                            sum(ar) as aggregate_retry
                            by bin(5min)
                '''),
                'region': config.region,
                'stacked': False,
                'title': 'Lambda timeouts',
                'view': 'timeSeries'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 18,
            'x': 12,
            'type': 'metric',
            'properties': {
                'metrics': [
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesNotVisible',
                        'QueueName',
                        config.notifications_queue_name(),
                        {
                            'label': 'notifications'
                        }
                    ],
                    [
                        '...',
                        config.notifications_queue_name(retry=True),
                        {
                            'label': 'notifications_retry'
                        }
                    ],
                    [
                        '...',
                        config.tallies_queue_name(),
                        {
                            'label': 'tallies'
                        }
                    ],
                    [
                        '...',
                        config.tallies_queue_name(retry=True),
                        {
                            'label': 'tallies_retry'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'title': 'In-flight messages',
                'period': 300,
                'stat': 'Average'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 60,
            'x': 0,
            'type': 'metric',
            'properties': {
                'metrics': [
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
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': config.region,
                'stat': 'Sum',
                'period': 300,
                'title': 'Lambda error rate [%]'
            }
        },
        {
            'height': 6,
            'width': 12,
            'y': 66,
            'x': 0,
            'type': 'log',
            'properties': {
                'query': dedent(f'''\
                    SOURCE '/aws/lambda/{config.indexer_function_name('aggregate')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('aggregate_retry')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('contribute')}'
                    | SOURCE '/aws/lambda/{config.indexer_function_name('contribute_retry')}'
                    | filter @message like 'Task timed out' or @message like 'START'
                    | fields strcontains(@message, 'Task timed out') == 1 as timeout
                    | fields strcontains(@message, 'START') == 1 as attempt
                    | fields strcontains(@log, 'aggregate') == 0 and strcontains(@log, 'retry') == 0 as c
                    | fields strcontains(@log, 'aggregate') == 0 and strcontains(@log, 'retry') == 1 as cr
                    | fields strcontains(@log, 'aggregate') == 1 and strcontains(@log, 'retry') == 0 as a
                    | fields strcontains(@log, 'aggregate') == 1 and strcontains(@log, 'retry') == 1 as ar
                    | stats sum(c*timeout) * 100 / sum(c*attempt) as contribute,
                            sum(cr*timeout) * 100 / sum(cr*attempt) as contribute_retry,
                            sum(a*timeout) * 100 / sum(a*attempt) as aggregate,
                            sum(ar*timeout) * 100 / sum(ar*attempt) as aggregate_retry
                            by bin(5min)
                '''),
                'region': config.region,
                'stacked': False,
                'title': 'Lambda timeout rate [%]',
                'view': 'timeSeries'
            }
        }
    ]
}

