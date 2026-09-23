from azul.indexer.aggregate import (
    Accumulator,
    SetAccumulator,
    SimpleAggregator,
)
from azul.logging import (
    configure_test_logging,
)
from azul_test_case import (
    AzulUnitTestCase,
    patch_config,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class SingleValueAggregator(SimpleAggregator):
    """
    An aggregator that retains one value per field, so that a second, distinct
    value overflows the accumulator. Overflow is tolerated for one of the two
    fields and enforced for the other.
    """

    def _accumulator(self, field: str) -> Accumulator | None:
        return SetAccumulator(max_size=1, allow_overflow=field == 'tolerated')


class TestAccumulatorOverflow(AzulUnitTestCase):
    log_name = 'azul.indexer.aggregate'

    entities = [
        {'enforced': 'a', 'tolerated': 'x'},
        {'enforced': 'b', 'tolerated': 'y'}
    ]

    def _aggregator(self) -> SingleValueAggregator:
        return SingleValueAggregator(outer_entity_type='files',
                                     entity_type='biosamples')

    def test_enforced(self):
        with self.assertRaises(AssertionError) as context:
            self._aggregator().aggregate(self.entities)
        self.assertIn('Values were dropped 1 times while aggregating '
                      'biosamples.enforced into files',
                      str(context.exception))

    def test_tolerated(self):
        entities = [{'tolerated': v} for v in ('x', 'y')]
        with self.assertLogs(self.log_name, level='WARNING') as logs:
            aggregate = self._aggregator().aggregate(entities)
        self.assertEqual([{'tolerated': ['x']}], list(aggregate))
        self.assertEqual(['WARNING:azul.indexer.aggregate:Values were dropped '
                          '1 times while aggregating biosamples.tolerated into '
                          'files'],
                         logs.output)

    @patch_config('allow_overflow', True)
    def test_allowed_by_config(self):
        with self.assertLogs(self.log_name, level='WARNING') as logs:
            aggregate = self._aggregator().aggregate(self.entities)
        self.assertEqual([{'enforced': ['a'], 'tolerated': ['x']}],
                         list(aggregate))
        self.assertEqual(['WARNING:azul.indexer.aggregate:Values were dropped '
                          '1 times while aggregating biosamples.enforced into '
                          'files',
                          'WARNING:azul.indexer.aggregate:Values were dropped '
                          '1 times while aggregating biosamples.tolerated into '
                          'files'],
                         logs.output)
