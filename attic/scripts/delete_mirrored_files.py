from collections.abc import (
    Iterable,
    Sequence,
)
import csv
import logging
import sys

from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.azulclient import (
    AzulClient,
)
from azul.indexer.mirror_service import (
    MirrorService,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)
from azul.service.storage_service import (
    StorageService,
)

log = logging.getLogger(__name__)


def delete_files(catalog: CatalogName, diff: Iterable[tuple[str, str]]):
    assert False
    checksums, sizes = zip(*diff)
    mirror_service: MirrorService = AzulClient().mirror_service(catalog)
    service: StorageService = mirror_service._storage
    keys = set()
    assert config.is_anvil_enabled(catalog)
    for checksum in checksums:
        keys.add(f'file/{checksum}.md5')
        keys.add(f'info/{checksum}.json')

    assert len(keys) == 2 * len(checksums), R('There are duplicate checksums')
    total_size = sum(map(int, sizes))
    print('This will permanently delete', len(checksums), 'files from',
          service.bucket_name, 'totalling', total_size, 'bytes. Proceed? (y/N)')
    if input() == 'y':
        service.delete_objects(object_keys=keys)
    else:
        print('Cancelled.')


def main(argv: Sequence[str]):
    path = one(argv)
    with open(path) as f:
        reader = csv.reader(f, delimiter='\t')
        delete_files(config.default_catalog, reader)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
