"""
Export parquet files from TDR and download them to local storage.
"""
from argparse import (
    ArgumentParser,
)
import logging
from pathlib import (
    Path,
)
import sys
from typing import (
    Iterator,
)
from uuid import (
    UUID,
)

import attrs
from furl import (
    furl,
)

from azul import (
    cached_property,
    config,
    reject,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terra import (
    TDRClient,
    TerraStatusException,
)

log = logging.getLogger(__name__)


@attrs.frozen
class ParquetDownloader(HasCachedHttpClient):
    snapshot_id: str

    @cached_property
    def tdr(self) -> TDRClient:
        return TDRClient.for_indexer()

    def get_download_urls(self) -> dict[str, list[furl]]:
        urls = self.tdr.export_parquet_urls(self.snapshot_id)
        reject(urls is None,
               'No parquet access information is available for snapshot %r', self.snapshot_id)
        return urls

    def get_data(self, parquet_urls: list[furl]) -> Iterator[bytes]:
        for url in parquet_urls:
            response = self._http_client.request('GET', str(url))
            if response.status != 200:
                raise TerraStatusException(url, response)
            if response.headers.get('x-ms-resource-type') == 'directory':
                log.info('Skipping Azure directory URL')
            else:
                yield response.data

    def download_table(self,
                       table_name: str,
                       download_urls: list[furl],
                       location: Path):
        data = None
        for i, data in enumerate(self.get_data(download_urls)):
            output_path = location / f'{self.snapshot_id}_{table_name}_{i}.parquet'
            log.info('Writing to %s', output_path)
            with open(output_path, 'wb') as f:
                f.write(data)
        reject(data is None,
               'No parquet files found for snapshot %r. Tried URLs: %r',
               self.snapshot_id, download_urls)


def main(argv):
    parser = ArgumentParser(add_help=True, description=__doc__)
    parser.add_argument('snapshot_id',
                        type=UUID,
                        help='The UUID of the snapshot')
    parser.add_argument('-O',
                        '--output-dir',
                        type=Path,
                        default=Path(config.project_root) / 'parquet',
                        help='Where to save the downloaded files')
    args = parser.parse_args(argv)

    downloader = ParquetDownloader(args.snapshot_id)

    urls_by_table = downloader.get_download_urls()
    for table_name, urls in urls_by_table.items():
        downloader.download_table(table_name, urls, args.output_dir)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
