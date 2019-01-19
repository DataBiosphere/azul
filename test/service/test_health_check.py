import logging
import unittest

from health_check_test_case import HealthCheckTestCase


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestHealthCheck(HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
