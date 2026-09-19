"""
Load this project's environment before every command that Claude Code runs, by
registering this script as a `PreToolUse` hook on Claude Code's `Bash` tool.

The hook eliminates the need to run `claude` with an already populated Azul
environment, which can quickly become stale and require a restart of `claude`
followed by a resumption of the session. The hook works by prefixing every
command Claude wants to run with `source environment && _login_aws`, creating a
up-to-date environment with current AWS credentials.

An Azul worktree has to opt in to the hook by registering it. Run this script
with the `register` argument to add that registration, or the `unregister`
argument to remove it. Alternatively, use `make claudehook` or `make
claudeunhook` to the same effects. The registration is written to the local
project settings (`.claude/settings.local.json`) of the repository's main
worktree, because that is the file Claude Code reads for every worktree of a
repository. Each hook registration names the worktree it was made in, and the
hook runs only for a `claude` instance whose project directory is that worktree.

To then use the hook, start `claude` in the root of the worktree, with the
virtualenv activated, and without having sourced the Azul environment. The hook
reads the selected deployment from `environment.hook` in the worktree. The
`_select` shell function maintains that file. Selecting a different deployment
therefore takes effect on the very next command, without restarting `claude`.
Similarly, expired credentials in the command's environment are replaced with
fresh ones, as soon as the user runs `_login` in another shell.

More details can be found in README section 2.4 PyCharm and Claude Code.
"""
import argparse
from contextlib import (
    contextmanager,
)
from functools import (
    cached_property,
)
from hashlib import (
    sha256,
)
import importlib.util
import json
import os
from pathlib import (
    Path,
)
import shlex
import subprocess
import sys
import tempfile
from textwrap import (
    dedent,
)
from typing import (
    NoReturn,
)


# The only way to escalate a hook error is via exit status 2. Any other non-zero
# status merely warns, letting the command run without an environment.
#
def _excepthook(*args):
    sys.__excepthook__(*args)
    sys.exit(2)


sys.excepthook = _excepthook

assert __name__ == '__main__', __name__


class Main:
    _this_script = Path(__file__)

    _root_dir = _this_script.parent.parent

    def _error(self, message: str) -> NoReturn:
        raise Exception(self._dedent(message))

    # A `systemMessage` is always shown verbatim. Writing to stderr and exiting
    # non-zero also works but the user only sees the first line of the message.
    #
    def _warn(self, message: str) -> NoReturn:
        json.dump({'systemMessage': self._dedent(message)}, sys.stdout)
        sys.exit(0)

    def _dedent(self, message: str) -> str:
        return dedent(message[1:]) if message.startswith('\n') else message

    _hook_event = 'PreToolUse'

    def main(self, argv: list[str]) -> None:
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument(
            'action',
            choices=['register', 'hook', 'unregister'],
            help=self._dedent(f"""
                Register this script as a {self._hook_event} hook in the local project settings
                of this worktree, perform the hook, or unregister the hook.
                """)
        )
        options = parser.parse_args(argv)
        if options.action == 'hook':
            self.hook()
        elif options.action == 'register':
            self.register()
        elif options.action == 'unregister':
            self.unregister()
        else:
            assert False, options.action

    def hook(self) -> None:
        # Drain stdin and parse the JSON regardless of whether it will be used
        hook_input = json.load(sys.stdin)
        azul_env_vars = 'azul_env_vars'
        if 'VIRTUAL_ENV' not in os.environ:
            self._error("""
                This `claude` instance was started without an active virtualenv, which this hook
                needs in order to prepare the Azul environment.

                If you want to continue using the hook (recommended), exit this `claude`
                instance, run `source .venv/bin/activate` and then start `claude` again.

                If you want to continue using `claude` without a virtualenv (not recommended),
                exit this `claude` instance, run `source .venv/bin/activate`, `make
                claudeunhook`, `deactivate`, and start `claude` again.
                """)
        elif azul_env_vars in os.environ:
            self._error("""
                This `claude` instance inherited a populated Azul environment and the hook is
                registered. This mode of operation is not supported.

                If you want to continue using the hook (recommended), exit this `claude`
                instance and start it again from a new shell, without first sourcing Azul's
                environment.

                If you want to continue using `claude` from a populated environment (not
                recommended), exit this `claude` instance, run `make claudeunhook` and start
                `claude` again.
                """)
        else:
            tool_input = hook_input['tool_input']
            environment = shlex.quote(str(self._root_dir / 'environment'))
            command = '\n'.join([
                # The deployment selected for this worktree, and any other
                # variable recorded alongside it.
                *(
                    f'export {name}={shlex.quote(value)}'
                    for name, value in self._selected_env().items()
                ),
                # Suppress the diagnostic output from export_environment.py,
                # which would otherwise precede the output of every command.
                'export azul_env_quiet=1',
                # Propagate a failure to load the environment instead of running
                # the command without one.
                f'source {environment} || exit',
                # Best effort to inject the ambient AWS session credentials. If
                # they have expired, _login_aws will complain about the lack of
                # a TTY to enter the MFA token on.
                '_login_aws || true',
                # The original command
                tool_input['command']
            ])
            hook_output = {
                'hookSpecificOutput': {
                    'hookEventName': self._hook_event,
                    'updatedInput': {
                        **tool_input,
                        'command': command
                    }
                }
            }
            json.dump(hook_output, sys.stdout)

    # A hook registration is this worktree's if its command carries this
    # marker, a hash of the path to the worktree. The registrations of every
    # worktree share a file, so the marker has to tell them apart. Matching it
    # loosely lets the rest of the command change over time, which is its only
    # purpose, so the way it is derived must never change.
    #
    _hook_marker = sha256(str(_root_dir.resolve()).encode()).hexdigest()[:8]

    # The `envhook.py` site hook is mutually exclusive with this script so we
    # pass -S to python to disable it.
    #
    # All paths are absolute, naming the worktree this registration is for,
    # including the one the command compares `CLAUDE_PROJECT_DIR` against.
    # Claude Code hands the registration to every instance of the repository,
    # and only the instance whose project directory is that worktree is to act
    # on it.
    #
    # The interpreter is named explicitly because `python` may be absent outside
    # an active virtualenv, and `python3` may be too old to parse this script.
    # Either way the hook would die before it could report the very precondition
    # it is there to enforce.
    #
    # Should the worktree move, the script is no longer where the registration
    # says, and we skip rather than fail, leaving the command to run without an
    # environment.
    #
    def _hook_command(self) -> str:
        # We should not resolve the path to executable because in a virtualenv
        # it is usually a symbolic link to the real interpreter
        python = Path(sys.executable)
        if python.is_relative_to(self._root_dir):
            script = shlex.quote(str(self._this_script.resolve()))
            python = shlex.quote(str(python))
            worktree = shlex.quote(str(self._root_dir.resolve()))
            return '; '.join([
                f'script={script}',
                f'if [ "$CLAUDE_PROJECT_DIR" = {worktree} ] && [ -f "$script" ]',
                f'then exec {python} -S "$script" hook',
                'fi'
            ]) + f' # {self._hook_marker}'
        else:
            self._error(f"""
                The Python interpreter running this script does not belong to the virtualenv for
                this worktree. Activate that virtualenv and try registering the hook again.

                Interpreter: {python} Worktree: {self._root_dir}
                """)

    _hook_matcher = 'Bash'

    # The deployment selected for this worktree lives here. `_select` maintains
    # the file and `envhook.py` injects it into the Python processes PyCharm
    # starts.
    #
    _hook_env_file = _root_dir / 'environment.hook'

    _restart_warning = 'Exit every `claude` instance running in this project'

    def register(self) -> None:
        settings = self._load_settings()
        if self._find_registration(settings) is None:
            hook_command = self._hook_command()
            group = {
                'matcher': self._hook_matcher,
                'hooks': [{'type': 'command', 'command': hook_command}]
            }
            events = settings.setdefault('hooks', {})
            groups = events.setdefault(self._hook_event, [])
            groups.append(group)
            self._save_settings(settings)
            print(f'Registered {hook_command} as a {self._hook_event} hook on '
                  f'{self._hook_matcher} in {self._settings_file}')
            print(self._restart_warning)
        else:
            print(f'Already registered in {self._settings_file}')

    def unregister(self) -> None:
        settings = self._load_settings()
        registration = self._find_registration(settings)
        if registration is None:
            print(f'Not registered in {self._settings_file}')
        else:
            group, entry = registration
            # Finding a registration proves that the containers holding it exist
            hooks = settings['hooks'][self._hook_event]
            group['hooks'].remove(entry)
            # Leave no empty containers behind, so that unregistering restores
            # the settings to what they were before registering.
            if not group['hooks']:
                hooks.remove(group)
            if not hooks:
                del settings['hooks'][self._hook_event]
            if not settings['hooks']:
                del settings['hooks']
            self._save_settings(settings)
            print(f'Unregistered {entry['command']} from {self._settings_file}')
            print(self._restart_warning)

    # Claude reads local project settings from
    # <main>/.claude/settings.local.json where <main> is the main worktree of
    # the repository, the one that `git clone` creates. It also reads
    # <linked>/.claude/settings.local.json in a linked worktree but the
    # documentation sounds as if that might go away soon, so we don't want to
    # rely on it. That's why we put all hook registrations in the main
    # worktree's local settings file.
    #
    @cached_property
    def _settings_file(self) -> Path:
        return self._main_worktree() / '.claude' / 'settings.local.json'

    def _main_worktree(self) -> Path:
        # The main worktree is the first one listed
        output = subprocess.run(['git', 'worktree', 'list', '--porcelain'],
                                cwd=self._root_dir,
                                check=True,
                                capture_output=True,
                                text=True).stdout
        prefix = 'worktree '
        line = output.splitlines()[0]
        assert line.startswith(prefix), line
        return Path(line.removeprefix(prefix))

    def _selected_env(self) -> dict[str, str]:
        """
        The variables recorded in `environment.hook` for this worktree.
        """
        # We parse  the file with the same function `envhook.py` uses, so that
        # both hooks agree on its contents.
        export_environment = self._import_sibling('export_environment')
        try:
            env = export_environment.load_env_file(self._hook_env_file)
        except FileNotFoundError:
            env = {}
        if export_environment.azul_current_deployment not in env:
            self._error("""
                No deployment is selected for this worktree. Run `_select` in a another shell
                that has this worktree's environment sourced, then ask Claude Code to retry. A
                restart of `claude` is not needed.
                """)
        return env

    def _import_sibling(self, module_name: str):
        """
        A normal import wouldn't find a sibling script because Claude Code runs
        this one by absolute path, leaving the directory containing it off the
        module search path.
        """
        path = self._this_script.parent / (module_name + '.py')
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec is not None and spec.loader is not None, path
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _find_registration(self, settings: dict) -> tuple[dict, dict] | None:
        return next(
            (
                (group, entry)
                for group in settings.get('hooks', {}).get(self._hook_event, [])
                for entry in group.get('hooks', ())
                if self._hook_marker in entry.get('command', '')
            ),
            None
        )

    def _load_settings(self) -> dict:
        try:
            text = self._settings_file.read_text()
        except FileNotFoundError:
            settings = {}
        else:
            if text:
                settings = json.loads(text)
            else:
                settings = {}
        assert isinstance(settings, dict), settings
        return settings

    def _save_settings(self, settings: dict) -> None:
        self._settings_file.parent.mkdir(parents=True, exist_ok=True)
        with self._write_file_atomically(self._settings_file) as f:
            json.dump(settings, f, indent=2)
            f.write('\n')

    # A copy of azul.lib.files.write_file_atomically. We can't import that
    # module because doing so initializes the `azul` package, which needs
    # distributions that -S hides from us, and an environment that this script
    # is about to load. Claude Code watches the settings so we must not truncate
    # and rewrite them, which a read could catch halfway through.
    #
    @contextmanager
    def _write_file_atomically(self, path, mode=0o644):
        dir_path, file_name = os.path.split(path)
        fd, temp_path = tempfile.mkstemp(dir=dir_path)
        try:
            with os.fdopen(fd, 'w') as f:
                yield f
            os.chmod(temp_path, mode)
            os.rename(temp_path, path)
        except BaseException:
            os.unlink(temp_path)
            raise


Main().main(sys.argv[1:])
