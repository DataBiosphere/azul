from azul.project.hca.config import IndexProperties
from azul.project.hca.indexer import Indexer
from azul import config

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
                        # Remove conditional when prod bundles are converted to vx structure
                        "field": ("files.donor_organism_json"
                                  if config.dss_endpoint != "https://dss.data.humancellatlas.org/v1"
                                  else "files.biomaterial_json")
                    }
                }
            ]
        }
    }
}

dss_deletion_subscription_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "admin_deleted": True
                    }
                }
            ]
        }
    }
}
