from itertools import (
    chain,
)
import logging

from azul import (
    config,
    require,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terra import (
    TDRClient,
    TDRSource,
)

log = logging.getLogger(__name__)


def main():
    configure_script_logging(log)
    tdr = TDRClient()
    tdr.register_with_sam()

    tdr_catalogs = (
        catalog.name
        for catalog in config.catalogs.values()
        if catalog.plugins['repository'] == 'tdr'
    )
    for source in set(chain(*map(config.tdr_sources, tdr_catalogs))):
        source = TDRSource.parse(source)
        api_project = tdr.project_for_source(source)
        require(api_project == source.project,
                'Actual Google project of TDR source differs from configured '
                'one',
                api_project, source)
        tdr.check_api_access(source)
        tdr.check_bigquery_access(source)


if __name__ == '__main__':
    main()
