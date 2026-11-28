from collections import (
    defaultdict,
)
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
    Deployment,
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


class TestBlueGreenMode(AzulUnitTestCase):

    def test_instances_in_sync(self):
        """
        The `environment.py` files of the two instances of a deployment in
        blue-green mode must be identical, except for the name of the instance
        and the template of its domain names. Otherwise, a forgotten change to
        one of them would break that instance the next time it becomes live.
        """
        instances: dict[str, dict[str, Deployment]] = defaultdict(dict)
        for deployment in Deployment.all():
            alias = deployment.alias
            if alias is not None:
                number = deployment.name.removeprefix(alias.name)
                instances[alias.name][number] = deployment
        # Guard against this test silently passing without examining anything
        self.assertIn('tempdev', instances)
        for alias, deployments in instances.items():
            with self.subTest(alias=alias):
                self.assertEqual({'1', '2'}, deployments.keys())
                # Raises if the alias has no directory
                Deployment(alias).directory
                env1, env2 = self._env(deployments['1']), self._env(deployments['2'])
                self.assertEqual(env1, env2)

    def _env(self, deployment: Deployment) -> dict[str, str | None]:
        path = deployment.directory / 'environment.py'
        module_name = 'environment_' + deployment.name
        env = dict(load_module(str(path), module_name).env())
        for variable in ['AZUL_DEPLOYMENT_STAGE', 'AZUL_SUBDOMAIN_TEMPLATE']:
            del env[variable]
        return env

    def test_names_of_shared_resources(self):
        for stage, mode, alias_stage in [
            ('tempdev1', '1', 'tempdev'),
            ('tempdev', '1', 'tempdev'),
            ('tempdev1', '0', 'tempdev1'),
            ('hannes2', '0', 'hannes2')
        ]:
            with self.subTest(stage=stage, mode=mode):
                with (
                    mock.patch.object(type(config),
                                      'deployment_stage',
                                      new_callable=mock.PropertyMock,
                                      return_value=stage),
                    mock.patch.dict(os.environ, azul_blue_green_mode=mode)
                ):
                    # The cached properties of the global config would retain
                    # the key's alias between subtests
                    apat_kms_key = Config().apat_kms_key
                    self.assertEqual(alias_stage, config.alias_stage)
                    self.assertEqual(config.qualified_resource_name('users', stage=alias_stage),
                                     config.dynamo_user_table_name)
                    self.assertEqual(config.qualified_resource_name('access_tokens', stage=alias_stage),
                                     config.dynamo_access_token_table_name)
                    self.assertEqual('alias/' + config.qualified_resource_name('apat', stage=alias_stage),
                                     apat_kms_key.alias)
                    self.assertEqual(f'dcp/azul/{alias_stage}/service/google_oauth2_client_secret',
                                     config.oauth2_client_secret_path())
                    # The manifest key remains specific to the instance
                    self.assertEqual('alias/' + config.qualified_resource_name('manifest'),
                                     Config().manifest_kms_key.alias)
