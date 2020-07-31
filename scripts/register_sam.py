import logging

from azul.logging import (
    configure_script_logging,
)
from azul.tdr import (
    TDRClient,
)

log = logging.getLogger(__name__)


def main():
    configure_script_logging(log)
    tdr = TDRClient()
    tdr.register_with_sam()
    tdr.verify_authorization()


if __name__ == '__main__':
    main()
