from textwrap import (
    dedent,
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-aggregate_retry'
                    | SOURCE '/aws/lambda/azul-indexer-prod-aggregate'
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
                'region': 'us-east-1',
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
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications-prod',
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
                            'region': 'us-east-1',
                            'color': '#ff7f0e'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications_retry-prod',
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
                            'region': 'us-east-1',
                            'color': '#9467bd'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications_fail-prod',
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
                            'region': 'us-east-1',
                            'color': '#2ca02c'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies-prod.fifo',
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
                            'region': 'us-east-1',
                            'color': '#d62728'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies_retry-prod.fifo',
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
                            'region': 'us-east-1',
                            'color': '#f7b6d2'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies_fail-prod.fifo',
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
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | fields strcontains(@message, 'Worker successfully handled') as success,
                             strcontains(@message,'Worker failed to handle message') as failure,
                             strcontains(@message,'Task timed out after') as timeout
                    | filter failure > 0 or success > 0 or timeout > 0
                    | stats sum(success) as Successes,
                            sum(failure + timeout) as Failures
                            by bin(5min)
                '''),
                'region': 'us-east-1',
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
                            'expression': 'm2 + m4 + m6 + m8',
                            'label': 'Primary',
                            'id': 'e1',
                            'region': 'us-east-1',
                            'color': '#2ca02c'
                        }
                    ],
                    [
                        {
                            'expression': 'm3 + m5 + m7 + m9',
                            'label': 'Replica',
                            'id': 'e2',
                            'region': 'us-east-1',
                            'color': '#1f77b4'
                        }
                    ],
                    [
                        'AWS/ES',
                        'Shards.unassigned',
                        'DomainName',
                        'azul-index-prod',
                        'ClientId',
                        '542754589326',
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
                        'azul-index-prod',
                        'NodeId',
                        '${local.nodes[0]}',
                        'ClientId',
                        '542754589326',
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
                    [
                        '...',
                        'Primary',
                        '.',
                        '.',
                        '.',
                        '${local.nodes[1]}',
                        '.',
                        '.',
                        {
                            'id': 'm4',
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
                            'id': 'm5',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'Primary',
                        '.',
                        '.',
                        '.',
                        '${local.nodes[2]}',
                        '.',
                        '.',
                        {
                            'id': 'm6',
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
                            'id': 'm7',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'Primary',
                        '.',
                        '.',
                        '.',
                        '${local.nodes[3]}',
                        '.',
                        '.',
                        {
                            'id': 'm8',
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
                            'id': 'm9',
                            'visible': False
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-aggregate'
                    | SOURCE '/aws/lambda/azul-indexer-prod-aggregate_retry'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | filter @message like 'TransportError'
                    | fields strcontains(@log, 'contribute') as contribute, 1 - contribute as aggregate
                    | stats sum(contribute) as Contribution, sum(aggregate) as Aggregation by bin(5min)
                '''),
                'region': 'us-east-1',
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
                        'azul-index-prod',
                        'NodeId',
                        '${local.nodes[0]}',
                        'ClientId',
                        '542754589326'
                    ],
                    [
                        '...',
                        '${local.nodes[1]}',
                        '.',
                        '.'
                    ],
                    [
                        '...',
                        '${local.nodes[2]}',
                        '.',
                        '.'
                    ],
                    [
                        '...',
                        '${local.nodes[3]}',
                        '.',
                        '.'
                    ]
                ],
                'region': 'us-east-1',
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
                            'expression': 'DIFF(m1+m2+m3+m4)/4/1000/60/5*100',
                            'label': 'Old generation',
                            'id': 'e2',
                            'region': 'us-east-1',
                            'stat': 'Maximum'
                        }
                    ],
                    [
                        {
                            'expression': 'DIFF(m5+m6+m7+m8)/4/1000/60/5*100',
                            'label': 'Young generation',
                            'id': 'e1',
                            'region': 'us-east-1',
                            'stat': 'Maximum',
                            'yAxis': 'left'
                        }
                    ],
                    [
                        'AWS/ES',
                        'JVMGCOldCollectionTime',
                        'DomainName',
                        'azul-index-prod',
                        'NodeId',
                        '${local.nodes[0]}',
                        'ClientId',
                        '542754589326',
                        {
                            'id': 'm1',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        '${local.nodes[1]}',
                        '.',
                        '.',
                        {
                            'id': 'm2',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        '${local.nodes[2]}',
                        '.',
                        '.',
                        {
                            'id': 'm3',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        '${local.nodes[3]}',
                        '.',
                        '.',
                        {
                            'id': 'm4',
                            'visible': False
                        }
                    ],
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
                            'id': 'm5',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        '${local.nodes[1]}',
                        '.',
                        '.',
                        {
                            'id': 'm6',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        '${local.nodes[2]}',
                        '.',
                        '.',
                        {
                            'id': 'm7',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        '${local.nodes[3]}',
                        '.',
                        '.',
                        {
                            'id': 'm8',
                            'visible': False
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': True,
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | fields @log
                    | parse 'It took *s to download' as duration
                    | filter ispresent(duration)
                    | fields strcontains(@log, '_retry') as is_retry
                    | stats avg(duration * (1 - is_retry)) as Initial,
                            avg(duration * is_retry) as Retry
                            by bin(5m)
                '''),
                'region': 'us-east-1',
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
                        'azul-indexer-prod-contribute',
                        {
                            'label': 'contribute'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'label': 'contribute_retry'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate',
                        {
                            'label': 'aggregate'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate_retry',
                        {
                            'label': 'aggregate_retry'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                        'azul-indexer-prod-contribute',
                        {
                            'label': 'contribute',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/Lambda',
                        'Errors',
                        'FunctionName',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'label': 'contribute_retry',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/Lambda',
                        'Errors',
                        'FunctionName',
                        'azul-indexer-prod-aggregate',
                        {
                            'label': 'aggregate',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/Lambda',
                        'Errors',
                        'FunctionName',
                        'azul-indexer-prod-aggregate_retry',
                        {
                            'label': 'aggregate_retry',
                            'region': 'us-east-1'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                        'azul-indexer-prod-contribute',
                        {
                            'label': 'contribute'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'label': 'contribute_retry'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate',
                        {
                            'label': 'aggregate'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate_retry',
                        {
                            'label': 'aggregate_retry'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                        'azul-indexer-prod-contribute',
                        {
                            'label': 'contribute'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'label': 'contribute_retry'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate',
                        {
                            'label': 'aggregate'
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate_retry',
                        {
                            'label': 'aggregate_retry'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        {
                            'expression': 'm4 / 1000',
                            'label': 'contribute_retry',
                            'id': 'e2',
                            'stat': 'Average',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        {
                            'expression': 'm1 / 1000',
                            'label': 'aggregate',
                            'id': 'e3',
                            'stat': 'Average',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        {
                            'expression': 'm2 / 1000',
                            'label': 'aggregate_retry',
                            'id': 'e4',
                            'stat': 'Average',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/Lambda',
                        'Duration',
                        'FunctionName',
                        'azul-indexer-prod-aggregate',
                        {
                            'id': 'm1',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate_retry',
                        {
                            'id': 'm2',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute',
                        {
                            'id': 'm3',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'id': 'm4',
                            'visible': False
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | filter @message like 'Exceeded rate limits'
                    | sort @timestamp desc
                    | stats count(@requestId) as trips by bin(5min)
                '''),
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | filter ispresent(stats.totalSlotMs)
                    | stats sum(stats.totalSlotMs) / 1000 / 3600 * 12 as `slot hours` by bin(5min)
                '''),
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | filter @message like 'Exceeded rate limits'
                    | parse 'BigQuery job error during attempt *. Retrying in *s.' as a, d
                    | filter ispresent(d)
                    | stats avg(d) as Delay by bin(5min)
                '''),
                'region': 'us-east-1',
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
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications-prod',
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
                            'region': 'us-east-1',
                            'color': '#ff7f0e'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications_retry-prod',
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
                            'region': 'us-east-1',
                            'color': '#9467bd'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications_fail-prod',
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
                            'region': 'us-east-1',
                            'color': '#2ca02c'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies-prod.fifo',
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
                            'region': 'us-east-1',
                            'color': '#d62728'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies_retry-prod.fifo',
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
                            'region': 'us-east-1',
                            'color': '#f7b6d2'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies_fail-prod.fifo',
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
                'region': 'us-east-1',
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
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        {
                            'expression': 'CEIL(m2 * 20 / PERIOD(m2))',
                            'label': 'notifications_retry',
                            'id': 'e2',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        {
                            'expression': 'CEIL(m3 * 20 / PERIOD(m3))',
                            'label': 'tallies.fifo',
                            'id': 'e3',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        {
                            'expression': 'CEIL(m4 * 20 / PERIOD(m4))',
                            'label': 'tallies_retry.fifo',
                            'id': 'e4',
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'NumberOfEmptyReceives',
                        'QueueName',
                        'azul-notifications-prod',
                        {
                            'id': 'm1',
                            'visible': False,
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'NumberOfEmptyReceives',
                        'QueueName',
                        'azul-notifications_retry-prod',
                        {
                            'id': 'm2',
                            'visible': False,
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'NumberOfEmptyReceives',
                        'QueueName',
                        'azul-tallies-prod.fifo',
                        {
                            'id': 'm3',
                            'visible': False,
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'NumberOfEmptyReceives',
                        'QueueName',
                        'azul-tallies_retry-prod.fifo',
                        {
                            'id': 'm4',
                            'visible': False,
                            'region': 'us-east-1'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-aggregate_retry'
                    | SOURCE '/aws/lambda/azul-indexer-prod-aggregate'
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
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
                    | fields stats.cacheHit, strcontains(@log, 'retry') as is_retry
                    | filter @message like 'Job info: '
                    | sort @timestamp desc
                    | stats sum(stats.cacheHit * (1 - is_retry)) / sum(1 - is_retry) * 100 as Initial,
                            sum(stats.cacheHit * is_retry ) / sum(is_retry) * 100 as Retry
                            by bin(5min)
                '''),
                'region': 'us-east-1',
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
                            'region': 'us-east-1'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-notifications-prod',
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
                        'azul-notifications_retry-prod',
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
                            'region': 'us-east-1',
                            'color': '#2ca02c'
                        }
                    ],
                    [
                        'AWS/SQS',
                        'ApproximateNumberOfMessagesVisible',
                        'QueueName',
                        'azul-tallies-prod.fifo',
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
                        'azul-tallies_retry-prod.fifo',
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
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-aggregate'
                    | SOURCE '/aws/lambda/azul-indexer-prod-aggregate_retry'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
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
                'region': 'us-east-1',
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
                        'azul-notifications-prod',
                        {
                            'label': 'notifications'
                        }
                    ],
                    [
                        '...',
                        'azul-notifications_retry-prod',
                        {
                            'label': 'notifications_retry'
                        }
                    ],
                    [
                        '...',
                        'azul-tallies-prod.fifo',
                        {
                            'label': 'tallies'
                        }
                    ],
                    [
                        '...',
                        'azul-tallies_retry-prod.fifo',
                        {
                            'label': 'tallies_retry'
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                        'azul-indexer-prod-contribute',
                        {
                            'label': 'contribute',
                            'id': 'm1',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'label': 'contribute_retry',
                            'id': 'm2',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate',
                        {
                            'label': 'aggregate',
                            'id': 'm3',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate_retry',
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
                        'azul-indexer-prod-contribute',
                        {
                            'id': 'm5',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-contribute_retry',
                        {
                            'id': 'm6',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate',
                        {
                            'id': 'm7',
                            'visible': False
                        }
                    ],
                    [
                        '...',
                        'azul-indexer-prod-aggregate_retry',
                        {
                            'id': 'm8',
                            'visible': False
                        }
                    ]
                ],
                'view': 'timeSeries',
                'stacked': False,
                'region': 'us-east-1',
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
                'query': dedent('''\
                    SOURCE '/aws/lambda/azul-indexer-prod-aggregate'
                    | SOURCE '/aws/lambda/azul-indexer-prod-aggregate_retry'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute'
                    | SOURCE '/aws/lambda/azul-indexer-prod-contribute_retry'
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
                'region': 'us-east-1',
                'stacked': False,
                'title': 'Lambda timeout rate [%]',
                'view': 'timeSeries'
            }
        }
    ]
}

