import json
import os
from unittest.mock import (
    patch,
)

from azul.modules import (
    load_script,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class TestCheckBranch(AzulUnitTestCase):

    def test(self):
        script = load_script('check_branch')
        check_branch = script.check_branch

        def expect_exception(branch, deployment, message):
            with self.assertRaises(script.BranchDeploymentMismatch) as e:
                check_branch(branch, deployment)
            self.assertEqual((message,), e.exception.args)

        default = {
            'develop': ['dev', 'sandbox'],
            'prod': ['prod']
        }
        with patch.dict(os.environ, azul_main_deployments=json.dumps(default)):
            check_branch('develop', 'dev')
            check_branch('develop', 'sandbox')

            expect_exception('feature/foo', 'prod',
                             "Branch 'feature/foo' cannot be deployed to 'prod', "
                             "only personal deployments.")
            expect_exception(None, 'prod',
                             "Detached head cannot be deployed to 'prod', "
                             "only personal deployments.")

            check_branch('prod', 'hannes.local')
            check_branch('develop', 'hannes.local')

            expect_exception('prod', 'dev',
                             "Branch 'prod' cannot be deployed to 'dev', "
                             "only one of {'prod'} or personal deployments.")

            expect_exception(None, 'dev',
                             "Detached head cannot be deployed to 'dev', "
                             "only personal deployments.")

            expect_exception('feature/foo', 'sandbox',
                             "Branch 'feature/foo' cannot be deployed to 'sandbox', "
                             "only personal deployments.")

            expect_exception(None, 'sandbox',
                             "Detached head cannot be deployed to 'sandbox', "
                             "only personal deployments.")

        # GitLab overrides the configuration to allow for the deployment of
        # feature branches to the sandbox.
        gitlab = {
            **default,
            '': ['sandbox']
        }
        with patch.dict(os.environ, azul_main_deployments=json.dumps(gitlab)):
            check_branch('feature/foo', 'sandbox')
            check_branch(None, 'sandbox')
            expect_exception('feature/foo',
                             'prod',
                             "Branch 'feature/foo' cannot be deployed to 'prod', "
                             "only one of {'sandbox'} or personal deployments.")
