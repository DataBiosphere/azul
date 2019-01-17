import unittest

from health_check_test_case import HealthCheckTestCase


class TestHealthCheck(HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'indexer'


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
