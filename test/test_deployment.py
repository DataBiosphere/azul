from collections.abc import (
    Iterator,
)
import os
from pathlib import (
    Path,
)
from types import (
    ModuleType,
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
    SnapshotName,
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
    Ensure that the snapshot name parsing in every AnVIL-based deployment's
    environment.py matches the same kind of parsing in the AnVIL plugin. These
    environment.py files can't import Azul code, otherwise we wouldn't need
    this test to ensure consistency between essentially duplicated code.
    """

    def test_dataset_names(self):
        num_snapshots = 0
        for module, catalog, spec in self._anvil_sources():
            with self.subTest(catalog=catalog.name, snapshot=spec.name):
                # Parsing a name also asserts that it follows the convention
                name = SnapshotName.parse(spec.name)
                # Every `source` prepends the prefix, and some of them require
                # it to be absent from their argument
                snapshot = spec.name.removeprefix(name.prefix + '_')
                # Only the first element of the result, the dataset name,
                # matters here, so the flags are left at their default
                dataset, _ = module.source(spec.subdomain[-8:], snapshot)
                # Both sides lower-case the name, the configurations in their
                # `delta` function, the plugin in `Plugin._entity_id`
                self.assertEqual(name.dataset.lower(), dataset.lower())
            num_snapshots += 1
        # Guard against this test silently passing without examining anything
        self.assertGreater(num_snapshots, 0)

    def _anvil_sources(self) -> Iterator[tuple[ModuleType, Config.Catalog, TDRSourceSpec]]:
        """
        The sources of every AnVIL catalog of every deployment, together with
        the catalog and the module the deployment's configuration was loaded
        from.
        """
        for path in sorted(Path(config.project_root).glob('deployments/*/environment.py')):
            deployment = path.parent.name
            module_name = 'environment_' + deployment.replace('.', '_')
            module = load_module(str(path), module_name)
            catalogs = module.env().get('AZUL_CATALOGS')
            if catalogs is not None:
                # Reuse the parsing, and decompression, of the real thing
                with mock.patch.dict(os.environ, AZUL_CATALOGS=catalogs):
                    catalogs = [
                        catalog
                        for catalog in Config().catalogs.values()
                        if catalog.plugins['repository'].name == 'tdr_anvil'
                    ]
                for catalog in catalogs:
                    for source in catalog.sources:
                        yield module, catalog, TDRSourceSpec.parse(source)
