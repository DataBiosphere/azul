import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
    emit_tf,
)

emit_tf(None if config.disable_monitoring else {
    "output": {
        "grafana_dashboard_azul": {
            "sensitive": True,
            "value": json.dumps({
                "annotations": {
                    "list": [
                        {
                            "builtIn": 1,
                            "datasource": "-- Grafana --",
                            "enable": True,
                            "hide": True,
                            "iconColor": "rgba(0, 211, 255, 1)",
                            "name": "Annotations & Alerts",
                            "type": "dashboard"
                        }
                    ]
                },
                "editable": True,
                "gnetId": None,
                "graphTooltip": 0,
                "id": None,
                "links": [],
                "panels": [
                    {
                        "cacheTimeout": None,
                        "colorBackground": True,
                        "colorValue": False,
                        "colors": [
                            "#bf1b00",
                            "#629e51",
                            "rgba(237, 129, 40, 0.89)"
                        ],
                        "datasource": "account-cloudwatch",
                        "format": "none",
                        "gauge": {
                            "maxValue": 100,
                            "minValue": 0,
                            "show": False,
                            "thresholdLabels": False,
                            "thresholdMarkers": True
                        },
                        "gridPos": {
                            "h": 10,
                            "w": 6,
                            "x": 0,
                            "y": 0
                        },
                        "id": 4,
                        "interval": None,
                        "links": [],
                        "mappingType": 1,
                        "mappingTypes": [
                            {
                                "name": "value to text",
                                "value": 1
                            },
                            {
                                "name": "range to text",
                                "value": 2
                            }
                        ],
                        "maxDataPoints": 100,
                        "NonePointMode": "connected",
                        "NoneText": None,
                        "postfix": "",
                        "postfixFontSize": "50%",
                        "prefix": "AZUL",
                        "prefixFontSize": "120%",
                        "rangeMaps": [
                            {
                                "from": "None",
                                "text": "N/A",
                                "to": "None"
                            }
                        ],
                        "sparkline": {
                            "fillColor": "rgba(31, 118, 189, 0.18)",
                            "full": False,
                            "lineColor": "rgb(31, 120, 193)",
                            "show": True
                        },
                        "tableColumn": "",
                        "targets": [
                            {
                                "dimensions": {
                                    "HealthCheckId":
                                        "${aws_route53_health_check.composite-azul.id}",
                                },
                                "highResolution": False,
                                "metricName": "HealthCheckStatus",
                                "namespace": "AWS/Route53",
                                "period": "",
                                "refId": "A",
                                "region": aws.region_name,
                                "statistics": [
                                    "Minimum"
                                ]
                            }
                        ],
                        "thresholds": "0.5",
                        "title": "Azul Health",
                        "type": "singlestat",
                        "valueFontSize": "120%",
                        "valueMaps": [
                            {
                                "op": "=",
                                "text": "OK",
                                "value": "1"
                            },
                            {
                                "op": "=",
                                "text": "ERR",
                                "value": "0"
                            }
                        ],
                        "valueName": "current"
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 6,
                            "x": 6,
                            "y": 0
                        },
                        "id": 14,
                        "legend": {
                            "alignAsTable": False,
                            "avg": False,
                            "current": False,
                            "max": True,
                            "min": True,
                            "show": True,
                            "total": False,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "dimensions": {
                                    "ClientId": aws.account,
                                    "DomainName": config.es_domain
                                },
                                "highResolution": True,
                                "metricName": "CPUUtilization",
                                "namespace": "AWS/ES",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            },
                            {
                                "dimensions": {
                                    "ClientId": aws.account,
                                    "DomainName": config.es_domain
                                },
                                "highResolution": True,
                                "metricName": "JVMMemoryPressure",
                                "namespace": "AWS/ES",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "ES - CPU",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "percent",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 6,
                            "x": 12,
                            "y": 0
                        },
                        "id": 16,
                        "legend": {
                            "alignAsTable": True,
                            "avg": False,
                            "current": False,
                            "max": True,
                            "min": True,
                            "show": True,
                            "total": False,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "dimensions": {
                                    "ClientId": aws.account,
                                    "DomainName": config.es_domain
                                },
                                "highResolution": True,
                                "metricName": "ReadLatency",
                                "namespace": "AWS/ES",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            },
                            {
                                "dimensions": {
                                    "ClientId": aws.account,
                                    "DomainName": config.es_domain
                                },
                                "highResolution": True,
                                "metricName": "WriteLatency",
                                "namespace": "AWS/ES",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "ES - I/O Latency",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "s",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "alert": {
                            "conditions": [
                                {
                                    "evaluator": {
                                        "params": [
                                            1073741824
                                        ],
                                        "type": "lt"
                                    },
                                    "operator": {
                                        "type": "and"
                                    },
                                    "query": {
                                        "params": [
                                            "A",
                                            "5m",
                                            "now"
                                        ]
                                    },
                                    "reducer": {
                                        "params": [],
                                        "type": "avg"
                                    },
                                    "type": "query"
                                }
                            ],
                            "executionErrorState": "alerting",
                            "frequency": "15m",
                            "handler": 1,
                            "name": "ES - Free Storage Space alert",
                            "noDataState": "no_data",
                            "notifications": []
                        },
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 6,
                            "x": 18,
                            "y": 0
                        },
                        "id": 18,
                        "legend": {
                            "alignAsTable": True,
                            "avg": False,
                            "current": False,
                            "max": True,
                            "min": True,
                            "show": True,
                            "total": False,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "dimensions": {
                                    "ClientId": aws.account,
                                    "DomainName": config.es_domain
                                },
                                "highResolution": True,
                                "metricName": "FreeStorageSpace",
                                "namespace": "AWS/ES",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Minimum"
                                ]
                            }
                        ],
                        "thresholds": [
                            {
                                "colorMode": "critical",
                                "fill": True,
                                "line": True,
                                "op": "lt",
                                "value": 1073741824
                            }
                        ],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "ES - Free Storage Space",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "mbytes",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "decimals": None,
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 12,
                            "x": 0,
                            "y": 10
                        },
                        "id": 6,
                        "legend": {
                            "alignAsTable": True,
                            "avg": True,
                            "current": True,
                            "hideEmpty": False,
                            "hideZero": False,
                            "max": True,
                            "min": True,
                            "show": True,
                            "sortDesc": True,
                            "total": True,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 1,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": True,
                        "targets": [
                            {
                                "alias": "Total Requests",
                                "dimensions": {
                                    "ApiName": config.service_name,
                                    "Stage": config.deployment_stage
                                },
                                "highResolution": True,
                                "metricName": "Count",
                                "namespace": "AWS/ApiGateway",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            },
                            {
                                "alias": "HTTP 4XX",
                                "dimensions": {
                                    "ApiName": config.service_name,
                                    "Stage": config.deployment_stage
                                },
                                "highResolution": True,
                                "metricName": "4XXError",
                                "namespace": "AWS/ApiGateway",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            },
                            {
                                "alias": "HTTP 5XX",
                                "dimensions": {
                                    "ApiName": config.service_name,
                                    "Stage": config.deployment_stage
                                },
                                "highResolution": True,
                                "metricName": "5XXError",
                                "namespace": "AWS/ApiGateway",
                                "period": "",
                                "refId": "C",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "Web Service API Request/Error Rates",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "decimals": None,
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 12,
                            "x": 12,
                            "y": 10
                        },
                        "id": 8,
                        "legend": {
                            "alignAsTable": True,
                            "avg": True,
                            "current": True,
                            "hideEmpty": False,
                            "hideZero": False,
                            "max": True,
                            "min": True,
                            "show": True,
                            "total": True,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 1,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": True,
                        "targets": [
                            {
                                "alias": "Total Requests",
                                "dimensions": {
                                    "ApiName": config.indexer_name,
                                    "Stage": config.deployment_stage
                                },
                                "highResolution": True,
                                "metricName": "Count",
                                "namespace": "AWS/ApiGateway",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            },
                            {
                                "alias": "HTTP 4XX",
                                "dimensions": {
                                    "ApiName": config.indexer_name,
                                    "Stage": config.deployment_stage
                                },
                                "highResolution": True,
                                "metricName": "4XXError",
                                "namespace": "AWS/ApiGateway",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            },
                            {
                                "alias": "HTTP 5XX",
                                "dimensions": {
                                    "ApiName": config.indexer_name,
                                    "Stage": config.deployment_stage
                                },
                                "highResolution": True,
                                "metricName": "5XXError",
                                "namespace": "AWS/ApiGateway",
                                "period": "",
                                "refId": "C",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "Indexer API Request/Error Rates",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 12,
                            "x": 0,
                            "y": 20
                        },
                        "id": 10,
                        "legend": {
                            "alignAsTable": True,
                            "avg": True,
                            "current": True,
                            "max": True,
                            "min": True,
                            "show": True,
                            "total": True,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "alias": "Web Service - Total Invocation Count",
                                "bucketAggs": [
                                    {
                                        "field": "@timestamp",
                                        "id": "2",
                                        "settings": {
                                            "interval": "auto",
                                            "min_doc_count": 0,
                                            "trimEdges": 0
                                        },
                                        "type": "date_histogram"
                                    }
                                ],
                                "dimensions": {
                                    "FunctionName": config.service_name
                                },
                                "highResolution": True,
                                "metricName": "Invocations",
                                "metrics": [
                                    {
                                        "field": "select field",
                                        "id": "1",
                                        "type": "count"
                                    }
                                ],
                                "namespace": "AWS/Lambda",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ],
                                "timeField": "@timestamp"
                            },
                            {
                                "alias": "Web Service - Total Error Count",
                                "dimensions": {
                                    "FunctionName": config.service_name
                                },
                                "highResolution": True,
                                "metricName": "Errors",
                                "namespace": "AWS/Lambda",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "Web Service Lambda",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 12,
                            "x": 12,
                            "y": 20
                        },
                        "id": 12,
                        "legend": {
                            "alignAsTable": True,
                            "avg": True,
                            "current": True,
                            "max": True,
                            "min": True,
                            "show": True,
                            "total": True,
                            "values": True
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "alias": "Indexer - Total Invocation Count",
                                "bucketAggs": [
                                    {
                                        "field": "@timestamp",
                                        "id": "2",
                                        "settings": {
                                            "interval": "auto",
                                            "min_doc_count": 0,
                                            "trimEdges": 0
                                        },
                                        "type": "date_histogram"
                                    }
                                ],
                                "dimensions": {
                                    "FunctionName": config.indexer_name
                                },
                                "highResolution": True,
                                "metricName": "Invocations",
                                "metrics": [
                                    {
                                        "field": "select field",
                                        "id": "1",
                                        "type": "count"
                                    }
                                ],
                                "namespace": "AWS/Lambda",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ],
                                "timeField": "@timestamp"
                            },
                            {
                                "alias": "Indexer - Total Error Count",
                                "dimensions": {
                                    "FunctionName": config.indexer_name
                                },
                                "highResolution": True,
                                "metricName": "Errors",
                                "namespace": "AWS/Lambda",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Sum"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "Indexer Lambda",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 12,
                            "x": 0,
                            "y": 30
                        },
                        "id": 20,
                        "legend": {
                            "avg": False,
                            "current": False,
                            "max": False,
                            "min": False,
                            "show": True,
                            "total": False,
                            "values": False
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "dimensions": {
                                    "TableName": config.dynamo_cart_table_name
                                },
                                "highResolution": True,
                                "metricName": "ConsumedReadCapacityUnits",
                                "namespace": "AWS/DynamoDB",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            },
                            {
                                "dimensions": {
                                    "TableName": config.dynamo_cart_table_name
                                },
                                "highResolution": True,
                                "metricName": "ConsumedWriteCapacityUnits",
                                "namespace": "AWS/DynamoDB",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "DynamoDB - Carts",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    },
                    {
                        "aliasColors": {},
                        "bars": False,
                        "dashLength": 10,
                        "dashes": False,
                        "datasource": "account-cloudwatch",
                        "fill": 1,
                        "gridPos": {
                            "h": 10,
                            "w": 12,
                            "x": 12,
                            "y": 30
                        },
                        "id": 22,
                        "legend": {
                            "avg": False,
                            "current": False,
                            "max": False,
                            "min": False,
                            "show": True,
                            "total": False,
                            "values": False
                        },
                        "lines": True,
                        "linewidth": 1,
                        "links": [],
                        "NonePointMode": "None as zero",
                        "percentage": False,
                        "pointradius": 5,
                        "points": False,
                        "renderer": "flot",
                        "seriesOverrides": [],
                        "spaceLength": 10,
                        "stack": False,
                        "steppedLine": False,
                        "targets": [
                            {
                                "dimensions": {
                                    "TableName": config.dynamo_cart_item_table_name
                                },
                                "highResolution": True,
                                "metricName": "ConsumedReadCapacityUnits",
                                "namespace": "AWS/DynamoDB",
                                "period": "",
                                "refId": "A",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            },
                            {
                                "dimensions": {
                                    "TableName": config.dynamo_cart_item_table_name
                                },
                                "highResolution": True,
                                "metricName": "ConsumedWriteCapacityUnits",
                                "namespace": "AWS/DynamoDB",
                                "period": "",
                                "refId": "B",
                                "region": "default",
                                "statistics": [
                                    "Average"
                                ]
                            }
                        ],
                        "thresholds": [],
                        "timeFrom": None,
                        "timeRegions": [],
                        "timeShift": None,
                        "title": "DynamoDB - Cart Items",
                        "tooltip": {
                            "shared": True,
                            "sort": 0,
                            "value_type": "individual"
                        },
                        "type": "graph",
                        "xaxis": {
                            "buckets": None,
                            "mode": "time",
                            "name": None,
                            "show": True,
                            "values": []
                        },
                        "yaxes": [
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            },
                            {
                                "format": "short",
                                "label": None,
                                "logBase": 1,
                                "max": None,
                                "min": None,
                                "show": True
                            }
                        ],
                        "yaxis": {
                            "align": False,
                            "alignLevel": None
                        }
                    }
                ],
                "refresh": "1m",
                "schemaVersion": 16,
                "style": "dark",
                "tags": [],
                "templating": {
                    "list": []
                },
                "time": {
                    "from": "now-6h",
                    "to": "now"
                },
                "timepicker": {
                    "refresh_intervals": [
                        "5s",
                        "10s",
                        "30s",
                        "1m",
                        "5m",
                        "15m",
                        "30m",
                        "1h",
                        "2h",
                        "1d"
                    ],
                    "time_options": [
                        "5m",
                        "15m",
                        "1h",
                        "6h",
                        "12h",
                        "24h",
                        "2d",
                        "7d",
                        "30d"
                    ]
                },
                "timezone": "utc",
                "title": f"Azul [{config.deployment_stage.upper()}]",
                "uid": f'azul-{config.deployment_stage}',
                "version": 1
            })
        },
        "grafana_dashboard_data_portal": {
            "sensitive": True,
            "value": json.dumps({
                "annotations": {
                    "list": [
                        {
                            "builtIn": 1,
                            "datasource": "-- Grafana --",
                            "enable": True,
                            "hide": True,
                            "iconColor": "rgba(0, 211, 255, 1)",
                            "name": "Annotations & Alerts",
                            "type": "dashboard"
                        }
                    ]
                },
                "editable": True,
                "gnetId": None,
                "graphTooltip": 0,
                "id": None,
                "links": [],
                "panels": [
                    {
                        "cacheTimeout": None,
                        "colorBackground": True,
                        "colorValue": False,
                        "colors": [
                            "#d44a3a",
                            "#629e51",
                            "#c15c17"
                        ],
                        "datasource": "account-cloudwatch",
                        "format": "none",
                        "gauge": {
                            "maxValue": 100,
                            "minValue": 0,
                            "show": False,
                            "thresholdLabels": False,
                            "thresholdMarkers": True
                        },
                        "gridPos": {
                            "h": 9,
                            "w": 24,
                            "x": 0,
                            "y": 0
                        },
                        "id": 2,
                        "interval": None,
                        "links": [],
                        "mappingType": 1,
                        "mappingTypes": [
                            {
                                "name": "value to text",
                                "value": 1
                            },
                            {
                                "name": "range to text",
                                "value": 2
                            }
                        ],
                        "maxDataPoints": 100,
                        "nullPointMode": "connected",
                        "nullText": None,
                        "postfix": "",
                        "postfixFontSize": "50%",
                        "prefix": "DATA BROWSER & PORTAL",
                        "prefixFontSize": "120%",
                        "rangeMaps": [
                            {
                                "from": "null",
                                "text": "N/A",
                                "to": "null"
                            }
                        ],
                        "sparkline": {
                            "fillColor": "rgba(31, 118, 189, 0.18)",
                            "full": False,
                            "lineColor": "rgb(31, 120, 193)",
                            "show": True
                        },
                        "tableColumn": "",
                        "targets": [
                            {
                                "dimensions": {
                                    "HealthCheckId":
                                        "${aws_route53_health_check.composite-portal.id}"
                                },
                                "expression": "",
                                "highResolution": False,
                                "id": "",
                                "metricName": "HealthCheckStatus",
                                "namespace": "AWS/Route53",
                                "period": "",
                                "refId": "A",
                                "region": "us-east-1",
                                "returnData": False,
                                "statistics": [
                                    "Minimum"
                                ]
                            }
                        ],
                        "thresholds": "0.5",
                        "title": "Data Browser & Portal Health",
                        "type": "singlestat",
                        "valueFontSize": "120%",
                        "valueMaps": [
                            {
                                "op": "=",
                                "text": "OK",
                                "value": "1"
                            },
                            {
                                "op": "=",
                                "text": "ERR",
                                "value": "0"
                            }
                        ],
                        "valueName": "current"
                    }
                ],
                "schemaVersion": 16,
                "style": "dark",
                "tags": [],
                "templating": {
                    "list": []
                },
                "time": {
                    "from": "now-6h",
                    "to": "now"
                },
                "timepicker": {
                    "refresh_intervals": [
                        "5s",
                        "10s",
                        "30s",
                        "1m",
                        "5m",
                        "15m",
                        "30m",
                        "1h",
                        "2h",
                        "1d"
                    ],
                    "time_options": [
                        "5m",
                        "15m",
                        "1h",
                        "6h",
                        "12h",
                        "24h",
                        "2d",
                        "7d",
                        "30d"
                    ]
                },
                "timezone": "",
                "title": f"Data Browser & Portal [{config.deployment_stage.upper()}]",
                "uid": f"data-portal-{config.deployment_stage}",
                "version": 1
            })
        }
    }
}
        )
