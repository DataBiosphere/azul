import logging.config

from chalice import (
    UnauthorizedError,
)

from azul import (
    config,
)
from azul.auth import (
    OAuth2,
)
from azul.health import (
    HealthApp,
)
from azul.lib import (
    cached_property,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
)
from azul.logging import (
    configure_app_logging,
)
from azul.plugins import (
    ManifestFormat,
)
from azul.service.catalog_controller import (
    CatalogController,
)
from azul.service.drs_controller import (
    DRSController,
)
from azul.service.index_controller import (
    IndexController,
)
from azul.service.manifest_controller import (
    ManifestController,
)
from azul.service.repository_controller import (
    RepositoryController,
)

log = logging.getLogger(__name__)

spec = {
    'openapi': '3.0.1',
    'info': {
        'title': config.service_name,
        # The version property should be updated in any PR connected to an issue
        # labeled `API`. Increment the major version for backwards incompatible
        # changes and reset the minor version to zero. Otherwise, increment only
        # the minor version for backwards compatible changes. A backwards
        # compatible change is one that does not require updates to clients.
        'version': '16.1',
        'description': fd(f'''
            # Overview

            Azul is a REST web service for querying metadata associated with
            both experimental and analysis data from a data repository. In order
            to deliver response times that make it suitable for interactive use
            cases, the set of metadata properties that it exposes for sorting,
            filtering, and aggregation is limited. Azul provides a uniform view
            of the metadata over a range of diverse schemas, effectively
            shielding clients from changes in the schemas as they occur over
            time. It does so, however, at the expense of detail in the set of
            metadata properties it exposes and in the accuracy with which it
            aggregates them.

            Azul denormalizes and aggregates metadata into several different
            indices for selected entity types. Metadata entities can be queried
            using the [Index](#operations-tag-Index) endpoints.

            A set of indices forms a catalog. There is a default catalog called
            `{config.default_catalog}` which will be used unless a
            different catalog name is specified using the `catalog` query
            parameter. Metadata from different catalogs is completely
            independent: a response obtained by querying one catalog does not
            necessarily correlate to a response obtained by querying another
            one. Two catalogs can contain metadata from the same sources or
            different sources. It is only guaranteed that the body of a
            response by any given endpoint adheres to one schema,
            independently of which catalog was specified in the request.

            Azul provides the ability to download data and metadata via the
            [Manifests](#operations-tag-Manifests) endpoints. The
            `{ManifestFormat.curl.value}` format manifests can be used to
            download data files. Other formats provide various views of the
            metadata. Manifests can be generated for a selection of files using
            filters. These filters are interchangeable with the filters used by
            the [Index](#operations-tag-Index) endpoints.

            Azul also provides a [summary](#operations-Index-get_index_summary)
            view of indexed data.

            ## Data model

            Any index, when queried, returns a JSON array of hits. Each hit
            represents a metadata entity. Nested in each hit is a summary of the
            properties of entities associated with the hit. An entity is
            associated either by a direct edge in the original metadata graph,
            or indirectly as a series of edges. The nested properties are
            grouped by the type of the associated entity. The properties of all
            data files associated with a particular sample, for example, are
            listed under `hits[*].files` in a `/index/samples` response. It is
            important to note that while each _hit_ represents a discrete
            entity, the properties nested within that hit are the result of an
            aggregation over potentially many associated entities.

            To illustrate this, consider a data file that is part of two
            projects (a project is a group of related experiments, typically by
            one laboratory, institution or consortium). Querying the `files`
            index for this file yields a hit looking something like:

            ```
            {{
                "projects": [
                    {{
                        "projectTitle": "Project One"
                        "laboratory": ...,
                        ...
                    }},
                    {{
                        "projectTitle": "Project Two"
                        "laboratory": ...,
                        ...
                    }}
                ],
                "files": [
                    {{
                        "format": "pdf",
                        "name": "Team description.pdf",
                        ...
                    }}
                ]
            }}
            ```

            This example hit contains two kinds of nested entities (a hit in an
            actual Azul response will contain more): There are the two projects
            entities, and the file itself. These nested entities contain
            selected metadata properties extracted in a consistent way. This
            makes filtering and sorting simple.

            Also notice that there is only one file. When querying a particular
            index, the corresponding entity will always be a singleton like
            this.
        ''')
    },
    'tags': [
        {
            'name': 'Index',
            'description': fd('''
                Query the indices for entities of interest
            ''')
        },
        {
            'name': 'Manifests',
            'description': fd('''
                Complete listing of files matching a given filter in TSV and
                other formats
            ''')
        },
        {
            'name': 'Repository',
            'description': fd('''
                Access to data files in the underlying repository
            ''')
        },
        {
            'name': 'DSS',
            'description': fd('''
                Access to files maintained in the Data Store
            ''')
        },
        {
            'name': 'DRS',
            'description': fd('''
                DRS-compliant proxy of the underlying repository
            ''')
        },
        {
            'name': 'Auxiliary',
            'description': fd('''
                Describes various aspects of the Azul service
            ''')
        },
        {
            'name': 'Deprecated',
            'description': fd('''
                Endpoints that should not be used and that will be removed
            ''')
        }
    ]
}


class ServiceApp(HealthApp):

    def spec(self) -> JSON:
        return {
            **super().spec(),
            **self._oauth2_spec()
        }

    def _oauth2_spec(self) -> JSON:
        scopes = ('email',)
        return {
            'components': {
                'securitySchemes': {
                    self.app_name: {
                        'type': 'oauth2',
                        'flows': {
                            'implicit': {
                                'authorizationUrl': 'https://accounts.google.com/o/oauth2/auth',
                                'scopes': {scope: scope for scope in scopes}
                            }
                        }
                    }
                }
            },
            'security': [
                {},
                {self.app_name: scopes}
            ]
        }

    @property
    def drs_controller(self) -> DRSController:
        return DRSController(app=self)

    @cached_property
    def catalog_controller(self) -> CatalogController:
        return CatalogController(app=self)

    @cached_property
    def index_controller(self) -> IndexController:
        return IndexController(app=self)

    @cached_property
    def repository_controller(self) -> RepositoryController:
        return RepositoryController(app=self)

    @cached_property
    def manifest_controller(self) -> ManifestController:
        return ManifestController(app=self)

    def __init__(self):
        super().__init__(app_name=config.service_name,
                         globals=globals(),
                         spec=spec)

    def _authenticate(self) -> OAuth2 | None:
        try:
            header = self.current_request.headers['Authorization']
        except KeyError:
            return None
        else:
            try:
                auth_type, auth_token = header.split()
            except ValueError:
                raise UnauthorizedError(header)
            else:
                if auth_type.lower() == 'bearer':
                    return OAuth2(auth_token)
                else:
                    raise UnauthorizedError(header)


app = ServiceApp()
configure_app_logging(app, log)

globals().update(app.default_routes())

globals().update(app.catalog_controller.handlers())

globals().update(app.index_controller.handlers())

globals().update(app.manifest_controller.handlers())

globals().update(app.repository_controller.handlers())

globals().update(app.drs_controller.handlers())
