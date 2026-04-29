"""
Update the GitHub Project titled "Azul" with a ZenHub workspace. This is not a
script we plan on documenting or maintaining long-term. There are plenty of
hard-coded assumptions, e.g., the fact the the current user has only one ZenHub
workspace. To invoke the script, the following variables need to be set:

- GITHUB_TOKEN: A classic GitHub PAT with scopes project, read:org, repo

- ZENHUB_TOKEN: A ZenHub GraphQL Personal API Key, not a REST API token

- GITHUB_COOKIE: A cookie for github.com. See Main._link_prs below

On the first few invocations, the script will probably bail out hitting a rate
limit. It is idempotent and can be safely restarted.

It is recommended to run the script at AZUL_DEBUG 0 or it will produce copious
amounts of logs. Only increase AZUL_DEBUG in order to diagnose errors.
"""

import json
import logging
import os
import time
from typing import (
    AbstractSet,
    Mapping,
    Sequence,
    overload,
)

from more_itertools import (
    batched,
    one,
    only,
)

from azul.http import (
    http_client,
)
from azul.lib import (
    cached_property,
)
from azul.lib.json import (
    dig,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    JSONs,
    PrimitiveJSON,
    json_bool,
    json_element_mappings,
    json_int,
    json_mapping,
    json_sequence_of_mappings,
    json_str,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


class Main:
    owner = 'DataBiosphere'

    #: Only issues with this label will be migrated
    label = 'orange'

    project_title = 'Azul'

    status_field_name = 'Status'
    points_field_name = 'Points'
    priority_field_name = 'Priority'
    linked_prs_field_name = 'Linked pull requests'

    status_by_pipeline = {
        'Triage': 'Triage',
        'Triage later': 'Triage later',
        'Icebox': 'Icebox',
        'Epics': 'Backlog',
        'Parked': 'Parked',
        'Debt': 'Backlog',
        'Compliance controls': 'Backlog',
        'Compliance': 'Backlog',
        'Backlog': 'Backlog',
        'Up next': 'Up next',
        'Sprint': 'Sprint',
        'Spike': 'Spike',
        'In Progress': 'In Progress',
        'Testing': 'In Progress',
        'Review requested': 'Review requested',
        'In review': 'In review',
        'Approved': 'Approved',
        'Merged lower': 'Merged lower',
        'Lower': 'Lower',
        'Merged stable': 'Merged stable',
        'Stable': 'Stable',
        'Closed': 'Done'
    }

    priority_labels = {'--', '-', '+', '++'}

    archived_repos = {
        'https://github.com/DataBiosphere/hca-metadata-api'
    }

    pr_limit = 10
    label_limit = 10
    field_limit = 16
    blocker_limit = 100
    issue_batch_size = 100

    @cached_property
    def http(self):
        # Non-Azul logger to avoid logging individual requests at AZUL_DEBUG 0
        return http_client(logging.getLogger('http_client'))

    def main(self):
        workspaces = self._zenhub_workspaces()
        log.info('Workspaces: %r', workspaces)
        workspace_id, workspace_name = one(workspaces.items())

        pipelines = self._zenhub_pipelines(workspace_id)
        log.info('Pipelines: %r', pipelines)

        project_id, fields = self._github_project()
        log.info('Project %r, fields: %r', project_id, fields)
        fields_by_name = {field['name']: field for field in fields.values()}
        assert len(fields_by_name) == len(fields)
        status_field = fields_by_name[self.status_field_name]
        assert status_field['dataType'] == 'SINGLE_SELECT'

        for pipeline_id, pipeline in pipelines.items():
            pipeline_name = json_str(pipeline['name'])
            log.info('Getting issues for pipeline %r', pipeline_name)
            zh_issues = self._zenhub_issues(workspace_id, pipeline)
            gh_issues = self._github_issues(zh_issues.keys())

            issues_yet_to_process = set(gh_issues.keys())
            for issue_id, gh_issue in gh_issues.items():
                issues_yet_to_process.remove(issue_id)
                log.info('Looking at issue %s, %i left to do in pipeline %r',
                         gh_issue['url'], len(issues_yet_to_process), pipeline_name)

                zh_issue = zh_issues[issue_id]

                zh_status = self.status_by_pipeline[pipeline_name]
                zh_estimate = dig(zh_issue, 'estimate', 'value')
                zh_connected_prs = {
                    json_str(pr['ghNodeId']): json_str(pr['htmlUrl'])
                    for pr in nodes(zh_issue, 'connectedPrs')
                }
                gh_labels = {label['name'] for label in nodes(gh_issue, 'labels')}
                zh_priority = only(self.priority_labels & gh_labels)

                gh_priority, gh_status, gh_points, _gh_linked_prs = None, None, None, {}
                project_item = only(nodes(gh_issue, 'projectItems'))
                if project_item is None:
                    project_item = self._add_project_item_by_id(issue_id, project_id)

                for field_value in nodes(project_item, 'fieldValues'):
                    field_name = dig(field_value, 'field', 'name')
                    match field_name:
                        case self.status_field_name:
                            gh_status = field_value['name']
                        case self.points_field_name:
                            gh_points = field_value['number']
                        case self.priority_field_name:
                            gh_priority = field_value['name']
                        case self.linked_prs_field_name:
                            assert node_count(field_value, 'pullRequests') <= self.pr_limit
                            _gh_linked_prs = {
                                json_str(pr['id']): json_str(pr['url'])
                                for pr in nodes(field_value, 'pullRequests')
                            }

                prs = nodes(gh_issue, 'closedByPullRequestsReferences')
                gh_closing_prs = {json_str(pr['id']): json_str(pr['url']) for pr in prs}
                assert gh_closing_prs == _gh_linked_prs

                if zh_priority != gh_priority:
                    log.info('Need to update priority of issue %s from %r to %r',
                             gh_issue['url'], gh_priority, zh_priority)
                    self._update_project_item_field_value(project_id=project_id,
                                                          item_id=json_str(project_item['id']),
                                                          field=fields_by_name[self.priority_field_name],
                                                          value=zh_priority)

                if gh_points != zh_estimate:
                    log.info('Need to update points for issue %s from %r to %r',
                             gh_issue['url'], gh_points, zh_estimate)
                    self._update_project_item_field_value(project_id=project_id,
                                                          item_id=json_str(project_item['id']),
                                                          field=fields_by_name[self.points_field_name],
                                                          value=zh_estimate)

                if gh_status != zh_status:
                    log.info('Need to update status of issue %s from %r to %r',
                             gh_issue['url'], gh_status, zh_status)
                    self._update_project_item_field_value(project_id=project_id,
                                                          item_id=json_str(project_item['id']),
                                                          field=fields_by_name[self.status_field_name],
                                                          value=zh_status)

                # GitHub: blockedBy - A list of issues that are blocking this issue
                # ZenHub: blockingIssues - Issues that are blocking this issue
                # GitHub: blocking: A list of issues that this issue is blocking
                # ZenHub: blockedIssues - Issues that are blocked by this Issue

                gh_blockers = {
                    json_str(issue['id']): json_str(issue['url'])
                    for issue in nodes(gh_issue, 'blockedBy')
                }
                zh_blockers = {}
                for blocker in nodes(zh_issue, 'blockingIssues'):
                    if json_str(blocker['ghNodeId']).startswith('PR_'):
                        log.warning('GitHub cannot block issues on PRs. '
                                    'Ignoring blocker %s', blocker['htmlUrl'])
                    elif any(
                        json_str(blocker['htmlUrl']).startswith(repo)
                        for repo in self.archived_repos
                    ):
                        log.warning('GitHub cannot block on issues in archived repositories. '
                                    'Ignoring blocker %s', blocker['htmlUrl'])
                    else:
                        zh_blockers[json_str(blocker['ghNodeId'])] = json_str(blocker['htmlUrl'])

                if gh_blockers != zh_blockers:
                    assert gh_blockers.keys() != zh_blockers.keys()
                    log.info('Need to update blockers on issue %s from %s to %r',
                             gh_issue['url'],
                             sorted(gh_blockers.values()),
                             sorted(zh_blockers.values()))
                    blockers_to_add = zh_blockers.keys() - gh_blockers.keys()
                    blockers_to_remove = gh_blockers.keys() - zh_blockers.keys()
                    assert not blockers_to_add & blockers_to_remove
                    blockers_to_add -= issues_yet_to_process
                    blockers_to_remove -= issues_yet_to_process
                    for blocker_id in blockers_to_add:
                        self._add_blocker(issue_id, blocker_id)
                    for blocker_id in blockers_to_remove:
                        self._remove_blocker(issue_id, blocker_id)

                gh_blockees = {
                    json_str(issue['id']): json_str(issue['url'])
                    for issue in nodes(gh_issue, 'blocking')
                }
                zh_blockees = {}
                for blockee in nodes(zh_issue, 'blockedIssues'):
                    if json_str(blockee['ghNodeId']).startswith('PR_'):
                        log.warning('GitHub cannot block PRs on issues. '
                                    'Ignoring blockee %s', blockee['htmlUrl'])
                    elif any(json_str(blockee['htmlUrl']).startswith(repo) for repo in self.archived_repos):
                        log.warning('GitHub cannot block issues in archived repositories. '
                                    'Ignoring blockee %s', blockee['htmlUrl'])
                    else:
                        zh_blockees[json_str(blockee['ghNodeId'])] = json_str(blockee['htmlUrl'])

                if gh_blockees != zh_blockees:
                    assert gh_blockees.keys() != zh_blockees.keys()
                    log.info('Need to update blockees on issue %s from %r to %r',
                             gh_issue['url'],
                             sorted(gh_blockees.values()),
                             sorted(zh_blockees.values()))
                    blockees_to_add = zh_blockees.keys() - gh_blockees.keys()
                    blockees_to_remove = gh_blockees.keys() - zh_blockees.keys()
                    blockees_to_add -= issues_yet_to_process
                    blockees_to_remove -= issues_yet_to_process
                    for blockee_id in blockees_to_add:
                        self._add_blocker(blockee_id, issue_id)
                    for blockee_id in blockees_to_remove:
                        self._remove_blocker(blockee_id, issue_id)

                if gh_closing_prs != zh_connected_prs:
                    assert gh_closing_prs.keys() != zh_connected_prs.keys()
                    log.info('Need to update PRs connected to issue %s from %r to %r',
                             gh_issue['url'],
                             sorted(gh_closing_prs.values()),
                             sorted(zh_connected_prs.values()))
                    self._link_prs(gh_issue, zh_connected_prs.keys())

    def _zenhub(self, body: JSON) -> JSON:
        return self._request(url='https://api.zenhub.com/public/graphql',
                             token=os.environ['ZENHUB_TOKEN'],
                             body=body)

    def _github(self, body: JSON) -> JSON:
        return self._request(url='https://api.github.com/graphql',
                             token=os.environ['GITHUB_TOKEN'],
                             body=body)

    class DeprecatedLegacyId(Exception):
        pass

    def _request(self, *, url: str, token: str, body: JSON) -> JSON:
        while True:
            response = self.http.request(
                'POST',
                url,
                body=json.dumps(body),
                headers={
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    # https://github.blog/news-insights/product-news/graphql-global-id-migration-update/
                    'X-Github-Next-Global-ID': '1'
                }
            )
            assert response.status == 200
            json_response = json.loads(response.data)
            try:
                errors = json_response['errors']
            except KeyError:
                return json_response['data']
            else:
                if len(errors) == 1 and errors[0].get('type') == 'RATE_LIMITED':
                    log.warning('Hit rate limit, waiting 60s before retrying ...')
                    time.sleep(60)
                else:
                    raise RuntimeError(errors)

    def _body(self, query: str, **variables: AnyJSON) -> JSON:
        return {
            'query': query,
            'variables': variables
        }

    def _zenhub_workspaces(self) -> dict[str, str]:
        response = self._zenhub(self._body(
            query=fd('''
                query Viewer {
                    viewer {
                        workspaceFavorites {
                            nodes {
                                workspace {
                                    name
                                    id
                                }
                            }
                        }
                    }
                }
        ''')))
        workspaces = {}
        viewer = json_mapping(response['viewer'])
        for node in nodes(viewer, 'workspaceFavorites'):
            workspace = json_mapping(node['workspace'])
            workspaces[json_str(workspace['id'])] = json_str(workspace['name'])
        return workspaces

    def _zenhub_pipelines(self, workspace_id: str) -> dict[str, JSON]:
        response = self._zenhub(self._body(
            workspace_id=workspace_id,
            query=fd('''
                query Workspace($workspace_id: ID!) {
                    workspace(id: $workspace_id) {
                        pipelines(includeClosed: true) {
                            id
                            name
                        }
                    }
                }
            ''')
        ))
        workspace = json_mapping(response['workspace'])
        pipelines = {
            json_str(pipeline['id']): pipeline
            for pipeline in json_element_mappings(workspace['pipelines'])
        }
        return pipelines

    def _zenhub_issues(self, workspace_id: str, pipeline: JSON) -> dict[str, JSON]:
        if pipeline['name'] == 'Closed':
            query_name = 'searchClosedIssues'
            container_id = workspace_id
            arg = 'workspaceId: $container_id'
        else:
            query_name = 'searchIssuesByPipeline'
            container_id = json_str(pipeline['id'])
            arg = 'pipelineId: $container_id'
        query = fd('''
            query SearchIssues(
                $container_id: ID!,
                $label: String!,
                $cursor: String
            ) {
                %(query_name)s(
                    after: $cursor
                    %(arg)s
                    filters: { displayType: issues, labels: { in: [ $label ] } }
                ) {
                    totalCount
                    pageInfo { hasNextPage endCursor }
                    nodes {
                        type htmlUrl ghNodeId
                        estimate { value }
                        connectedPrs { nodes { type htmlUrl ghNodeId } }
                        blockingIssues { nodes { type htmlUrl ghNodeId } }
                        blockedIssues { nodes { type htmlUrl ghNodeId } }
                        parentIssue { type htmlUrl ghNodeId }
                    }
                }
            }
        ''' % locals())
        cursor, issues_by_id = None, {}
        while True:
            response = self._zenhub(self._body(
                container_id=container_id,
                label=self.label,
                cursor=cursor,
                query=query
            ))
            issues = nodes(response, query_name)
            issues = self._patch_legacy_ids(issues)
            for issue in issues:
                issues_by_id[json_str(issue['ghNodeId'])] = issue
            result = json_mapping(response[query_name])
            page_info = json_mapping(result['pageInfo'])
            if json_bool(page_info['hasNextPage']):
                log.info('Fetching another page of issues for pipeline %r',
                         pipeline['name'])
                cursor = page_info['endCursor']
            else:
                break
        assert len(issues_by_id) == result['totalCount'], len(issues_by_id)
        return issues_by_id

    @overload
    def _patch_legacy_ids(self, data: JSON) -> JSON:
        ...

    @overload
    def _patch_legacy_ids(self, data: JSONs) -> JSONs:
        ...

    def _patch_legacy_ids(self, data):
        legacy_ids: set[str] = set()

        def collect(v: AnyJSON):
            # A str is a Sequence, so need to check for str first
            if isinstance(v, str):
                pass
            elif isinstance(v, Sequence):
                for v in v:
                    collect(v)
            elif isinstance(v, Mapping):
                for k, v in v.items():
                    if k == 'ghNodeId':
                        node_id = json_str(v)
                        if not (node_id.startswith('I_') or node_id.startswith('PR_')):
                            legacy_ids.add(node_id)
                    else:
                        collect(v)

        collect(data)

        if legacy_ids:
            id_mapping = self._resolve_legacy_ids(legacy_ids)

            @overload
            def patch[T: PrimitiveJSON](data: T) -> T:
                ...

            @overload
            def patch(data: JSON) -> JSON:
                ...

            @overload
            def patch(data: JSONs) -> JSONs:
                ...

            def patch(data):
                # A str is a Sequence, so need to check for str first
                if isinstance(data, str):
                    return data
                elif isinstance(data, Sequence):
                    return list(map(patch, data))
                elif isinstance(data, Mapping):
                    return {
                        k: id_mapping.get(v, v) if k == 'ghNodeId' else patch(v)
                        for k, v in data.items()
                    }
                else:
                    return data

            data = patch(data)

        return data

    def _resolve_legacy_ids(self, legacy_ids: set[str]) -> dict[str, str]:
        legacy_ids = list(legacy_ids)
        query = 'query { ' + '\n'.join(
            'node_%i: node(id: "%s") { id }' % (i, legacy_ids[i])
            for i in range(len(legacy_ids))
        ) + ' }'
        data = self._github(self._body(query))
        return {
            legacy_ids[i]: json_str(json_mapping(data[f'node_{i}'])['id'])
            for i in range(len(legacy_ids))
        }

    def _github_project(self) -> tuple[str, dict[str, JSON]]:
        response = self._github(self._body(
            login=self.owner,
            project_title=self.project_title,
            field_limit=self.field_limit,
            query=fd('''
                query Organization(
                    $login:String!,
                    $project_title:String!,
                    $field_limit: Int!
                ) {
                    organization(login: $login) {
                        projectsV2(query: $project_title, first: 1) {
                            totalCount
                            nodes {
                                id
                                title
                                fields(first: $field_limit) {
                                    totalCount
                                    nodes {
                                        ... on ProjectV2Field { dataType id name }
                                        ... on ProjectV2IterationField { dataType id name }
                                        ... on ProjectV2SingleSelectField {
                                            dataType id name
                                            options {
                                                id name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ''')
        ))
        organization = json_mapping(response['organization'])
        assert node_count(organization, 'projectsV2') == 1
        project = one(nodes(organization, 'projectsV2'))
        assert project['title'] == self.project_title
        assert node_count(project, 'fields') <= self.field_limit
        fields = {
            json_str(field['id']): field
            for field in nodes(project, 'fields')
        }
        return json_str(project['id']), fields

    def _github_issues(self, issue_ids: AbstractSet[str]) -> dict[str, JSON]:
        issues = {}
        for batch in batched(issue_ids, self.issue_batch_size):
            log.info('Fetching batch of %i issue(s) from GitHub', len(batch))
            selections = self._project_item_selections(field_limit='$field_limit',
                                                       pr_limit='$pr_limit')
            response = self._github(self._body(
                issue_ids=batch,
                blocker_limit=self.blocker_limit,
                pr_limit=self.pr_limit,
                label_limit=self.label_limit,
                field_limit=self.field_limit,
                query=fd('''
                    query Nodes(
                        $issue_ids: [ID!]!,
                        $blocker_limit: Int!,
                        $pr_limit: Int!,
                        $label_limit: Int!,
                        $field_limit: Int!
                    ) {
                        nodes(ids: $issue_ids) {
                            ... on Issue {
                                id
                                url
                                issueType { id name }
                                labels(first:$label_limit) {
                                    totalCount
                                    nodes { id name }
                                }
                                blockedBy(first: $blocker_limit) { totalCount nodes { id url } }
                                blocking(first: $blocker_limit) { totalCount nodes { id url } }
                                closedByPullRequestsReferences(first: $pr_limit, includeClosedPrs: true) {
                                    totalCount
                                    nodes { id url }
                                }
                                projectItems(first: 1) {
                                    totalCount
                                    nodes {
                                        %(selections)s
                                    }
                                }
                            }
                        }
                    }
                ''' % locals()))
            )
            for issue in json_element_mappings(response['nodes']):
                assert node_count(issue, 'projectItems') <= 1
                assert node_count(issue, 'blockedBy') <= self.blocker_limit
                assert node_count(issue, 'blocking') <= self.blocker_limit
                assert node_count(issue, 'closedByPullRequestsReferences') <= self.pr_limit
                issues[json_str(issue['id'])] = issue
        return issues

    def _project_item_selections(self, field_limit: str, pr_limit: str) -> str:
        field_union = fd('''
                field {
                    ... on ProjectV2Field { name }
                    ... on ProjectV2IterationField { name }
                    ... on ProjectV2SingleSelectField { name }
                }
            ''')
        return fd('''
            id
            fieldValues(first: %(field_limit)s) {
                nodes {
                    ... on ProjectV2ItemFieldNumberValue {
                        number
                        %(field_union)s
                    }
                    ... on ProjectV2ItemFieldSingleSelectValue {
                        name
                        %(field_union)s
                    }
                    ... on ProjectV2ItemFieldTextValue {
                        text
                        %(field_union)s
                    }
                    ... on ProjectV2ItemFieldPullRequestValue {
                        pullRequests(first: %(pr_limit)s) {
                            totalCount
                            nodes { id url }
                        }
                        %(field_union)s
                    }
                }
            }
        ''' % locals())

    def _add_project_item_by_id(self, issue_id: str, project_id: str) -> JSON:
        selections = self._project_item_selections(field_limit='$field_limit',
                                                   pr_limit='$pr_limit')
        query = fd('''
            mutation AddProjectV2ItemById(
                $project_id:ID!,
                $content_id: ID!,
                $field_limit: Int!,
                $pr_limit: Int!) {
                addProjectV2ItemById(input: { projectId: $project_id, contentId: $content_id }) {
                    item {
                        %(selections)s
                    }
                }
            }
        ''' % locals())
        data = self._github(self._body(
            project_id=project_id,
            content_id=issue_id,
            field_limit=self.field_limit,
            pr_limit=self.pr_limit,
            query=query
        ))
        return json_mapping(json_mapping(data['addProjectV2ItemById'])['item'])

    def _update_project_item_field_value(self, *,
                                         project_id: str,
                                         item_id: str,
                                         field: JSON,
                                         value: AnyJSON
                                         ) -> None:
        if value is None:
            mutation = fd('''
                mutation UpdateProjectV2ItemFieldValue(
                    $project_id: ID!,
                    $item_id: ID!
                    $field_id: ID!
                ) {
                    clearProjectV2ItemFieldValue(
                        input: {
                            projectId: $project_id
                            itemId: $item_id
                            fieldId: $field_id
                        }
                    ) {
                        clientMutationId
                    }
                }
            ''')
        else:
            match field['dataType']:
                case 'SINGLE_SELECT':
                    assert isinstance(value, str)
                    options = {
                        json_str(option['name']): json_str(option['id'])
                        for option in json_element_mappings(field['options'])
                    }
                    option_id = options[value]
                    assert '"' not in option_id
                    assert '\\' not in option_id
                    value = f'singleSelectOptionId: "{option_id}"'
                case 'NUMBER':
                    assert isinstance(value, float)
                    value = f'number: {value}'
                case _:
                    assert False

            mutation = fd('''
                mutation UpdateProjectV2ItemFieldValue(
                    $project_id: ID!,
                    $item_id: ID!
                    $field_id: ID!
                ) {
                    updateProjectV2ItemFieldValue(
                        input: {
                            projectId: $project_id
                            itemId: $item_id
                            fieldId: $field_id
                            value: { %(value)s }
                        }
                    ) {
                        clientMutationId
                    }
                }
            ''' % locals())
        self._github(self._body(
            project_id=project_id,
            item_id=item_id,
            field_id=field['id'],
            query=mutation
        ))

    def _add_blocker(self, issue_id: str, blocker_id: str):
        query = fd('''
                mutation AddBlockedBy($issue_id: ID!, $blocker_id:ID!) {
                    addBlockedBy(input: { issueId: $issue_id, blockingIssueId: $blocker_id }) {
                        clientMutationId
                    }
                }
        ''')
        self._handle_blocker(blocker_id, issue_id, query)

    def _remove_blocker(self, issue_id: str, blocker_id: str):
        query = fd('''
                mutation RemoveBlockedBy($issue_id: ID!, $blocker_id: ID!) {
                    removeBlockedBy(input: { issueId: $issue_id, blockingIssueId: $blocker_id }) {
                        clientMutationId
                    }
                }
        ''')
        self._handle_blocker(blocker_id, issue_id, query)

    def _handle_blocker(self, blocker_id: str, issue_id: str, query: str):
        def request():
            self._github(self._body(issue_id=issue_id,
                                    blocker_id=blocker_id,
                                    query=query))

        try:
            request()
        except self.DeprecatedLegacyId as e:
            data = e.args[0]
            if blocker_id == data['legacy_global_id']:
                blocker_id = data['next_global_id']
            elif issue_id == data['legacy_global_id']:
                issue_id = data['next_global_id']
            else:
                raise
            request()

    def _link_prs(self, issue: JSON, pr_ids: AbstractSet[str]) -> None:
        # There is currently no officially documented means for programmatically
        # linking PRs to issues. The code below was reverse-engineered by
        # looking manually linking a PR in the GitHub web UI for an issue with
        # the browser's inspection UI open. The requests are made to a
        # GraphQL-like endpoint but the queries are obfuscated by going through
        # some sort of lookup mechanism. The responses are not. Look for
        # POST requests whose body resembles the one assigned to the `body`
        # variable below.
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:142.0) Gecko/20100101 Firefox/142.0',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': issue['url'],
            'X-Requested-With': 'XMLHttpRequest',
            'X-Fetch-Nonce': 'v2:abca485e-3c0c-1223-d38b-aca2527b8b61',  # probably brittle
            'X-GitHub-Client-Version': 'a84319411c6b6585fddc61079261dcc934e04166',
            'GitHub-Verified-Fetch': 'true',
            'Content-Type': 'text/plain;charset=UTF-8',
            'Origin': 'https://github.com',
            'Sec-GPC': '1',
            'Connection': 'keep-alive',
            'Cookie': os.environ['GITHUB_COOKIE'],
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Priority': 'u=0'
        }
        body = {
            'query': '6d318b0319f7be8dff2d1bc9c2882ffc',  # probably very brittle
            'variables': {
                'baseIssueOrPullRequestId': issue['id'],
                'linkingIds': list(pr_ids)
            }
        }
        response = self.http.request('POST',
                                     url='https://github.com/_graphql',
                                     headers=headers, body=json.dumps(body))
        assert response.status == 200
        data = json.loads(response.data)
        result = data['data']['linkIssueOrPullRequest']['baseIssueOrPullRequest']
        if result['id'].startswith('I_'):
            # Some responses don't use "next" IDs for some reason so we can't
            # assert the response in those cases
            assert result['id'] == issue['id'], (issue, result)
            prs = nodes(result, 'closedByPullRequestsReferences')
            result_pr_ids = {pr['id'] for pr in prs}
            # Also, if a commit in the PR branch has a 'fixes #1234' reference,
            # we will not be able to remove the reference.
            assert pr_ids <= result_pr_ids
        log.info('Sleeping 3s to avoid rate limit')
        time.sleep(3)


def node_count(d: JSON, k: str) -> int:
    return json_int(json_mapping(d[k])['totalCount'])


def nodes(d: JSON, k: str) -> JSONs:
    return json_sequence_of_mappings(json_mapping(d[k])['nodes'])


if __name__ == '__main__':
    configure_script_logging(log)
    Main().main()
