import logging

from azul import config
from azul.logging import (
    configure_script_logging,
)
from azul.tdr import (
    AzulTDRClient,
    BigQueryDataset,
    TDRClient,
)

log = logging.getLogger(__name__)


def main():
    configure_script_logging(log)
    tdr = TDRClient()
    tdr.register_with_sam()
    tdr.verify_authorization()
    azul_tdr = AzulTDRClient(BigQueryDataset.parse(config.tdr_target))
    azul_tdr.verify_authorization()


if __name__ == '__main__':
    main()
