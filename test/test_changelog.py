from operator import itemgetter
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import azul.changelog


class TestChangeLog(TestCase):
    def test_changelog(self):
        with TemporaryDirectory() as tmpdir:
            changelog = azul.changelog.changelog()
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                azul.changelog.write_changes(tmpdir)
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
