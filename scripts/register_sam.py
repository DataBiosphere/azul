import logging

from azul import (
    config,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins.repository.tdr import (
    Plugin,
)
from azul.tdr import (
    TDRClient,
    TDRSource,
)

log = logging.getLogger(__name__)


def main():
    configure_script_logging(log)
    tdr = TDRClient()
    tdr.register_with_sam()
    tdr.verify_authorization()

    tdr_catalogs = (
        catalog
        for catalog, plugins in config.catalogs.items()
        if plugins['repository'] == 'tdr'
    )
    for source in set(map(config.tdr_source, tdr_catalogs)):
        plugin = Plugin(TDRSource.parse(source))
        plugin.verify_authorization()


if __name__ == '__main__':
    main()
