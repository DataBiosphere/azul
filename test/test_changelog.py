from operator import (
    itemgetter,
)
import sys
from tempfile import (
    TemporaryDirectory,
)
from unittest import (
    mock,
)

import azul.changelog
from azul.logging import (
    configure_test_logging,
)
from azul_test_case import (
    AzulUnitTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestChangeLog(AzulUnitTestCase):

    def test_changelog(self):
        changelog = azul.changelog.changelog()
        with TemporaryDirectory() as tmpdir:
            azul.changelog.write_changes(tmpdir)
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                changes = azul.changelog.changes()
                all_compact_changes = azul.changelog.compact_changes()
                one_compact_change = azul.changelog.compact_changes(limit=1)
            self.assertEqual(len(changes), len(changelog['changes']))
            self.assertEqual(len(all_compact_changes), len(changes))
            title_getter = itemgetter('title')
            for _changes in all_compact_changes, changes:
                self.assertEqual(list(map(title_getter, _changes)),
                                 list(map(title_getter, changelog['changes'])))
            self.assertEqual(one_compact_change[0], all_compact_changes[0])
            self.assertTrue(all('title' == next(iter(change.keys())) for change in all_compact_changes))
