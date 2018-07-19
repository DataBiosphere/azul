from .config import IndexProperties
from .indexer import Indexer

dss_subscription_query = {
    "query": {
        "bool": {
            "must_not": [
                {
                    "term": {
                        "admin_deleted": True
                    }
                }
            ],
            "must": [
                {
                    "exists": {
                        "field": "files.biomaterial_json"
                    }
                }
            ]
        }
    }
}
