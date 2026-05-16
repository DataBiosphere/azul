"""
Update Claude Code's internal metadata (the context) in preparation of renaming
or moving projects. Move Claude Code sessions between projects.
"""
from abc import (
    ABCMeta,
    abstractmethod,
)
import argparse
import json
import logging
from pathlib import (
    Path,
    PurePath,
)
import re
import shutil
import sys
from typing import (
    ClassVar,
    Mapping,
    Sequence,
)
import uuid

import attrs

from azul.lib import (
    R,
)
from azul.lib.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
    MutableJSON,
    json_list_of_dicts,
    json_str,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True
    for command_cls in Command.__subclasses__():
        command_cls.add_subparser(subparsers)
    args = parser.parse_args(argv)
    command = args.command_class(args)
    command.execute()


@attrs.define
class Command(metaclass=ABCMeta):
    _claude_dir: ClassVar[Path] = Path.home() / '.claude'
    _contexts_dir: ClassVar[Path] = _claude_dir / 'projects'
    _args: argparse.Namespace

    @classmethod
    @abstractmethod
    def add_subparser(cls, subparsers: argparse._SubParsersAction) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute(self) -> None:
        raise NotImplementedError

    def _encode_path(self, path: PurePath) -> str:
        return re.sub(r'[/.]', '-', str(path))

    def _rewrite_session(self,
                         session_file: Path,
                         old_dir: Path,
                         new_dir: Path
                         ) -> None:
        old, new = str(old_dir), str(new_dir)
        text = session_file.read_text()
        if old in text:
            def rewrite(message: str) -> str:
                message = json.loads(message)
                message = self._replace_paths(message, old, new)
                return json.dumps(message, ensure_ascii=False, separators=(',', ':'))

            lines = list(map(rewrite, text.splitlines()))
            session_file.write_text('\n'.join(lines) + '\n')

    def _replace_paths(self, obj: AnyJSON, old: str, new: str) -> AnyMutableJSON:
        if isinstance(obj, str):
            return obj.replace(old, new)
        elif isinstance(obj, Mapping):
            return {k: self._replace_paths(v, old, new) for k, v in obj.items()}
        elif isinstance(obj, Sequence):
            return [self._replace_paths(v, old, new) for v in obj]
        else:
            return obj


class MoveProjectCommand(Command):
    """
    Update Claude Code's context in preparation of renaming or moving a project
    directory. Does not actually move the project directory.
    """

    @classmethod
    def add_subparser(cls, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser('project', help=cls.__doc__)
        parser.add_argument('old_project',
                            metavar='OLD',
                            help='The old path of the project directory')
        parser.add_argument('new_project',
                            metavar='NEW',
                            help='The new path of the project directory')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--merge',
                           action='store_true',
                           default=False,
                           help='In the unexpected event that a context '
                                'already exists at the new location, combine '
                                'it with the one from the old location. This '
                                'action is not reversible.')
        group.add_argument('--clobber',
                           action='store_true',
                           default=False,
                           help='In the unexpected event that a context '
                                'already exists at the new location, discard '
                                'it and replace it with the one from the old '
                                'location. This action is not reversible.')
        parser.set_defaults(command_class=cls)

    def execute(self) -> None:
        old_dir = Path(self._args.old_project).resolve()
        new_dir = Path(self._args.new_project).resolve()
        assert old_dir != new_dir, R(
            'Old and new project locations are the same', old_dir)

        old_key = self._encode_path(old_dir)
        new_key = self._encode_path(new_dir)
        old_context = self._contexts_dir / old_key
        new_context = self._contexts_dir / new_key

        conflicts = self._find_conflicts(new_key)
        if conflicts:
            assert self._args.merge or self._args.clobber, R(
                'A context for the new project location already exists. '
                'Use --merge or --clobber to resolve.', conflicts)

        if self._args.clobber:
            log.info('Removing existing context for %r at %r', str(new_dir), str(new_context))
            self._remove_context(new_key)

        log.info('Moving context for %r at %r to %r', str(old_dir), str(old_context), str(new_context))
        self._move_context(old_key, new_key)
        log.info('Rewriting session paths')
        self._rewrite_sessions(new_key, old_dir, new_dir)

    _context_subdirs = ['projects']

    def _find_conflicts(self, key: str) -> list[Path]:
        conflicts = []
        for subdir in self._context_subdirs:
            dst = self._claude_dir / subdir / key
            if dst.exists():
                conflicts.append(dst)
        return conflicts

    def _remove_context(self, key: str) -> None:
        for subdir in self._context_subdirs:
            dst = self._claude_dir / subdir / key
            if dst.is_dir():
                shutil.rmtree(dst)
            elif dst.is_file():
                dst.unlink()

    def _move_context(self, old_key: str, new_key: str) -> None:
        for subdir in self._context_subdirs:
            src = self._claude_dir / subdir / old_key
            dst = self._claude_dir / subdir / new_key
            if src.is_dir():
                if dst.is_dir() and self._args.merge:
                    for child in src.iterdir():
                        child.rename(dst / child.name)
                    src.rmdir()
                else:
                    src.rename(dst)
            elif src.is_file():
                src.rename(dst)

    def _rewrite_sessions(self, key: str, old_dir: Path, new_dir: Path) -> None:
        context_dir = self._contexts_dir / key
        for session_file in context_dir.glob('*.jsonl'):
            self._rewrite_session(session_file, old_dir, new_dir)


class MoveSessionCommand(Command):
    """
    Move a session from one project to another.
    """

    @classmethod
    def add_subparser(cls, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser('session', help=cls.__doc__)
        parser.add_argument('session',
                            metavar='SESSION',
                            help='The UUID or name of the session to move')
        parser.add_argument('src_project',
                            metavar='SOURCE',
                            help='The path of the source project directory')
        parser.add_argument('dst_project',
                            metavar='DESTINATION',
                            help='The path of the destination project directory')
        parser.set_defaults(command_class=cls)

    def execute(self) -> None:
        src_project_dir = Path(self._args.src_project).resolve()
        dst_project_dir = Path(self._args.dst_project).resolve()
        assert src_project_dir.is_dir(), R(
            'Source project directory does not exist', src_project_dir)
        assert dst_project_dir.is_dir(), R(
            'Destination project directory does not exist', dst_project_dir)

        src_key = self._encode_path(src_project_dir)
        dst_key = self._encode_path(dst_project_dir)
        src_context_dir = self._contexts_dir / src_key
        dst_context_dir = self._contexts_dir / dst_key
        assert src_context_dir.is_dir(), R(
            'No Claude context for source project', src_project_dir)
        assert dst_context_dir.is_dir(), R(
            'No Claude context for destination project', dst_project_dir)
        assert src_context_dir != dst_context_dir, R(
            'Source and destination projects are the same', src_project_dir)

        session = self._args.session
        if self._is_uuid(session):
            session_id = session
        else:
            session_id = self._find_session_by_name(src_context_dir, session)
            log.info('Resolved session name %r to %r', session, session_id)
        session_base_name = session_id + '.jsonl'
        src_session_file = src_context_dir / session_base_name
        dst_session_file = dst_context_dir / session_base_name

        if src_session_file.exists():
            log.info('Moving session file %r', str(src_session_file))
            src_session_file.rename(dst_session_file)

            src_session_dir = src_context_dir / session_id
            if src_session_dir.is_dir():
                log.info('Moving session directory %r', str(src_session_dir))
                src_session_dir.rename(dst_context_dir / session_id)

            log.info('Updating session index')
            entry = self._remove_from_session_index(src_context_dir, session_id)
            if entry is not None:
                self._add_to_session_index(dst_context_dir, entry, dst_project_dir)
        else:
            assert dst_session_file.exists(), R(
                'Session not found in source or destination project', session_id)
            log.info('Session already moved, skipping to path rewriting')

        log.info('Rewriting session paths')
        self._rewrite_session(dst_session_file, src_project_dir, dst_project_dir)

    def _is_uuid(self, session: str) -> bool:
        try:
            uuid.UUID(session)
        except ValueError:
            return False
        else:
            return True

    def _find_session_by_name(self,
                              context_dir: Path,
                              name: str
                              ) -> str:
        matches: set[str] = set()
        for session_file in context_dir.glob('*.jsonl'):
            session_id = session_file.stem
            title = None
            for line in session_file.open():
                entry = json.loads(line)
                if entry.get('type') == 'custom-title':
                    title = json_str(entry['customTitle'])
            if title == name:
                matches.add(session_id)
        match tuple(matches):
            case (session_id, ):
                return session_id
            case ():
                assert False, R('No session with this name', name)
            case _:
                assert False, R('Multiple sessions with this name', name, matches)

    def _read_session_index(self, context_dir: Path) -> MutableJSON | None:
        index_file = context_dir / 'sessions-index.json'
        if index_file.exists():
            return json.loads(index_file.read_text())
        else:
            return None

    def _write_session_index(self, context_dir: Path, index: JSON) -> None:
        index_file = context_dir / 'sessions-index.json'
        index_file.write_text(json.dumps(index, ensure_ascii=False, separators=(',', ':')) + '\n')

    def _remove_from_session_index(self,
                                   context_dir: Path,
                                   session_id: str
                                   ) -> MutableJSON | None:
        index = self._read_session_index(context_dir)
        if index is None:
            return None
        else:
            try:
                entries = json_list_of_dicts(index['entries'])
            except KeyError:
                return None
            else:
                remove_at = None
                for i, entry in enumerate(entries):
                    if json_str(entry['sessionId']) == session_id:
                        assert remove_at is None, R(
                            'Duplicate session in index', session_id)
                        remove_at = i
                if remove_at is None:
                    return None
                else:
                    removed = entries.pop(remove_at)
                    self._write_session_index(context_dir, index)
                    return removed

    def _add_to_session_index(self,
                              context_dir: Path,
                              entry: MutableJSON,
                              dst_project: Path
                              ) -> None:
        index = self._read_session_index(context_dir)
        if index is not None:
            session_id = json_str(entry['sessionId'])
            entry['fullPath'] = str(context_dir / (session_id + '.jsonl'))
            entry['projectPath'] = str(dst_project)
            entries = json_list_of_dicts(index['entries'])
            for i, e in enumerate(entries):
                if json_str(e['sessionId']) == session_id:
                    entries[i] = entry
                    break
            else:
                entries.append(entry)
            self._write_session_index(context_dir, index)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
