import copy
from itertools import (
    chain,
    groupby,
)
import json
import logging
from operator import (
    itemgetter,
)
from random import (
    Random,
)
from typing import (
    Any,
    Optional,
    Tuple,
)
import unittest
import uuid

import attr
from more_itertools import (
    flatten,
    one,
    unzip,
)
import requests

from azul import (
    config,
)
from azul.logging import (
    configure_test_logging,
)
from azul.types import (
    JSONs,
)
from service import (
    WebServiceTestCase,
    patch_dss_endpoint,
    patch_source_cache,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@patch_dss_endpoint
@patch_source_cache
class PaginationTestCase(WebServiceTestCase):
    templates: JSONs
    random: Random

    def setUp(self):
        super().setUp()
        self.random = Random(42)
        self._setup_indices()
        hits = self._get_all_hits()
        self.templates = [hit['_source'] for hit in hits]
        self._delete_all_hits()

    def tearDown(self):
        self._teardown_indices()
        super().tearDown()

    @property
    def _index_name(self):
        return config.es_index_name(catalog=self.catalog,
                                    entity_type='files',
                                    aggregate=True)

    query = {
        'query': {
            'match_all': {}
        }
    }

    def _get_all_hits(self):
        response = self.es_client.search(index=self._index_name,
                                         body=self.query)
        return response['hits']['hits']

    def _delete_all_hits(self):
        self.es_client.delete_by_query(index=self._index_name,
                                       body=self.query,
                                       refresh=True)

    def _clone_doc(self, doc):
        """
        Duplicate the given `files` document with new identifiers.
        """
        doc = copy.deepcopy(doc)
        entity_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        doc['entity_id'] = entity_id
        file = one(doc['contents']['files'])
        file['document_id'] = entity_id
        file['uuid'] = file_id
        return doc

    def _add_docs(self, num_docs):
        """
        Make the given number of copies of a randomly selected template
        document from the `files` index.
        """
        if num_docs > 0:
            log.info('Adding %i documents to index', num_docs)
            template = self.random.choice(self.templates)
            docs = [self._clone_doc(template) for _ in range(num_docs)]
            body = '\n'.join(
                flatten(
                    (
                        json.dumps({'create': {}}),
                        json.dumps(doc)
                    )
                    for doc in docs
                )
            )
            self.es_client.bulk(body, index=self._index_name, refresh=True)

    def test_pagination(self):

        index_to_page_size = {
            (page_size * num_pages + num_excess_doc, page_size)
            for page_size in (1, 2, 5)
            for num_pages in (0, 1, 2, 3)
            for num_excess_doc in (-1, 0, 1)
            if page_size * num_pages + num_excess_doc > 0
        }

        page_sizes_by_index_size = {
            i: list(unzip(page_sizes)[1])
            for i, page_sizes in groupby(sorted(index_to_page_size), key=itemgetter(0))
        }

        index_size_ = 0
        for index_size, page_sizes in page_sizes_by_index_size.items():
            self._add_docs(index_size - index_size_)
            for page_size in page_sizes:
                for sort_field, sort_path, sort_unique in [
                    ('entryId', ['entryId'], True),
                    ('fileId', ['files', 0, 'uuid'], True),
                    ('fileName', ['files', 0, 'name'], False)
                ]:
                    for reverse in False, True:
                        kwargs = dict(index_size=index_size,
                                      page_size=page_size,
                                      sort_field=sort_field,
                                      reverse=reverse)
                        with self.subTest(**kwargs):
                            self._test_pagination(**kwargs,
                                                  sort_path=sort_path,
                                                  sort_unique=sort_unique)
            index_size_ = index_size

    @attr.s(frozen=True, kw_only=True, auto_attribs=True)
    class Page:
        #: The link to the previous page
        previous: Optional[str]
        #: The value of the sort field in each hit on the page
        values: Tuple[str, ...]
        #: The link to the next page
        next: Optional[str]

    def _test_pagination(self,
                         *,
                         index_size: int,
                         page_size: int,
                         sort_field: str,
                         sort_path: Tuple[Any, ...],
                         sort_unique: bool,
                         reverse: bool):
        num_pages = (index_size + page_size - 1) // page_size
        order = 'desc' if reverse else 'asc'
        unique = set if sort_unique else lambda _: _

        def sort_field_value(doc):
            value = doc
            for key in sort_path:
                value = value[key]
            return value

        def fetch(url):
            response = requests.get(url)
            response.raise_for_status()
            response = response.json()
            values = tuple(map(sort_field_value, response['hits']))
            self.assertEqual(values, tuple(sorted(unique(values), reverse=reverse)))
            pagination = response['pagination']
            previous, next = map(pagination.pop, ['previous', 'next'])
            expected_pagination = {
                'pages': num_pages,
                'size': page_size,
                'count': len(values),
                'order': order,
                'total': index_size,
                'sort': sort_field,
            }
            self.assertEqual(expected_pagination, pagination)
            return self.Page(previous=previous, values=values, next=next)

        args = dict(catalog=self.catalog, sort=sort_field, size=page_size, order=order)
        url = str(self.base_url.set(path='/index/files', args=args))

        pages = []
        while url is not None:
            page = fetch(url)
            if page.previous is None:
                self.assertEqual([], pages)
            else:
                previous = fetch(page.previous)
                self.assertEqual(pages[-1], previous)
            pages.append(page)
            url = page.next

        self.assertEqual(num_pages, len(pages))
        page_lengths = [len(page.values) for page in pages]
        expected_lengths = num_pages * [page_size]
        if index_size % page_size:
            expected_lengths[-1] = index_size % page_size
        self.assertEqual(expected_lengths, page_lengths)
        self.assertEqual(index_size, sum(page_lengths))
        values = list(chain.from_iterable(page.values for page in pages))
        self.assertEqual(values, list(sorted(unique(values), reverse=reverse)))


if __name__ == '__main__':
    unittest.main()
