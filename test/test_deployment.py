import os
from pathlib import (
    Path,
)
from unittest import (
    mock,
)

from azul import (
    Config,
    config,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    R,
)
from azul.modules import (
    load_module,
)
from azul.plugins.repository.tdr_anvil import (
    Plugin,
)
from azul.terra import (
    TDRSourceSpec,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class TestDeploymentAWS(AzulUnitTestCase):

    def test_qualified_bucket_name(self):
        self.assertEqual(f'edu-ucsc-gi-{self._aws_account_name}-foo.us-gov-west-1',
                         aws.qualified_bucket_name('foo'))
        for invalid in ['', 'x', '1foo']:
            with self.subTest(invalid=invalid):
                with self.assertRaises(AssertionError) as cm:
                    aws.qualified_bucket_name(invalid)
                self.assertTrue(R.caused(cm.exception))


class TestAnvilSnapshotNames(AzulUnitTestCase):
    """
    The AnVIL repository plugin infers the version of the AnVIL schema a
    snapshot was ingested under from the snapshot's name. A snapshot whose name
    doesn't follow the convention can still be listed and partitioned, so the
    plugin only rejects it once it fetches the snapshot's first bundle, by
    which time a reindex is well under way. Validating the configured snapshot
    names here fails the build instead, long before a deployment is reindexed.
    """

    def test_snapshot_names(self):
        num_snapshots = 0
        for catalog in self._anvil_catalogs():
            for source in catalog.sources:
                spec = TDRSourceSpec.parse(source)
                with self.subTest(catalog=catalog.name, snapshot=spec.name):
                    self.assertIsNotNone(Plugin._snapshot_name_re.fullmatch(spec.name), spec.name)
                num_snapshots += 1
        # Guard against this test silently passing without examining anything
        self.assertGreater(num_snapshots, 0)

    def _anvil_catalogs(self) -> list[Config.Catalog]:
        result = []
        for path in sorted(Path(config.project_root).glob('deployments/*/environment.py')):
            deployment = path.parent.name
            module_name = 'environment_' + deployment.replace('.', '_')
            catalogs = load_module(str(path), module_name).env().get('AZUL_CATALOGS')
            if catalogs is not None:
                # Reuse the parsing, and decompression, of the real thing
                with mock.patch.dict(os.environ, AZUL_CATALOGS=catalogs):
                    result.extend(
                        catalog
                        for catalog in Config().catalogs.values()
                        if catalog.plugins['repository'].name == 'tdr_anvil'
                    )
        return result
