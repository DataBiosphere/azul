"""
Load this project's environment before every command that Claude Code runs, by
registering this script as a `PreToolUse` hook on Claude Code's `Bash` tool.

The hook eliminates the need to run `claude` with an already populated Azul
environment, which can quickly become stale and require a restart of `claude`
followed by a resumption of the session. The hook works by prefixing every
command Claude wants to run with `source environment && _login_aws`, creating a
up-to-date environment with current AWS credentials.

An Azul worktree has to opt in to the hook by registering it in the local
project settings of that worktree (`.claude/settings.local.json`). Run this
script with the `register` argument to add that registration, or the
`unregister` argument to remove it. Alternatively, use `make claudehook` or
`make claudeunhook` to the same effect.

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
import importlib.util
import json
import os
from pathlib import (
    Path,
)
import shlex
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


def _error(message: str) -> NoReturn:
    raise Exception(_dedent(message))


# A `systemMessage` is always shown verbatim. Writing to stderr and exiting
# non-zero also works but the user only sees the first line of the message.
#
def _warn(message: str) -> NoReturn:
    json.dump({'systemMessage': _dedent(message)}, sys.stdout)
    sys.exit(0)


def _dedent(message: str) -> str:
    return dedent(message[1:]) if message.startswith('\n') else message


assert __name__ == '__main__', __name__

this_script = Path(__file__)

root_dir = this_script.parent.parent

hook_event = 'PreToolUse'


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'action',
        choices=['register', 'hook', 'unregister'],
        help=f'Register this script as a {hook_event} hook in the local project '
             f'settings of this worktree, perform the hook, or unregister '
             f'the hook.'
    )
    options = parser.parse_args(argv)
    if options.action == 'hook':
        hook()
    elif options.action == 'register':
        register()
    elif options.action == 'unregister':
        unregister()
    else:
        assert False, options.action


def hook() -> None:
    # Drain stdin and parse the JSON regardless of whether it will be used
    hook_input = json.load(sys.stdin)
    azul_env_vars = 'azul_env_vars'
    project_dir = os.environ.get('CLAUDE_PROJECT_DIR')
    if project_dir is None or Path(project_dir).resolve() != root_dir.resolve():
        _warn(f"""
            Skipping hook actions because this registration is for {root_dir}
            while this `claude` instance uses {project_dir} as its project
            directory.

            Claude Code takes the directory that `claude` was started in as
            the project directory, also when resuming a session, without
            resolving it to the root of an enclosing worktree. Start
            `claude` in the root of the worktree you mean to work in, and
            register the hook there.
            """)
    elif 'VIRTUAL_ENV' not in os.environ:
        _error("""
            This `claude` instance was started without an active virtualenv,
            which this hook needs in order to prepare the Azul environment.

            If you want to continue using the hook (recommended), exit this
            `claude` instance, run `source .venv/bin/activate` and then start
            `claude` again.

            If you want to continue using `claude` without a virtualenv (not
            recommended), exit this `claude` instance, run `source
            .venv/bin/activate`, `make claudeunhook`, `deactivate`, and start
            `claude` again.
            """)
    elif azul_env_vars in os.environ:
        _error("""
            This `claude` instance inherited a populated Azul environment and
            the hook is registered. This mode of operation is not supported.

            If you want to continue using the hook (recommended), exit this
            `claude` instance and start it again from a new shell, without first
            sourcing Azul's environment.

            If you want to continue using `claude` from a populated
            environment (not recommended), exit this `claude` instance,
            run `make claudeunhook` and start `claude` again.
            """)
    else:
        tool_input = hook_input['tool_input']
        environment = shlex.quote(str(root_dir / 'environment'))
        command = '\n'.join([
            # The deployment selected for this worktree, and any other
            # variable recorded alongside it.
            *(
                f'export {name}={shlex.quote(value)}'
                for name, value in _selected_env().items()
            ),
            # Suppress the copious diagnostic output from export_environment.py,
            # which would otherwise precede the output of every command.
            'export azul_env_quiet=1',
            # Propagate a failure to load the environment instead of running the
            # command without one.
            f'source {environment} || exit',
            # Best effort to inject the ambient AWS session credentials. If they
            # have expired, _login_aws will complain about the lack of a TTY to
            # enter the MFA token on.
            '_login_aws || true',
            # The original command
            tool_input['command']
        ])
        hook_output = {
            'hookSpecificOutput': {
                'hookEventName': hook_event,
                'updatedInput': {
                    **tool_input,
                    'command': command
                }
            }
        }
        json.dump(hook_output, sys.stdout)


# A hook registration is ours if its command carries this marker. A fuzzy match
# allows for changes to be made to the hook command over time. It has no other
# purpose and should therefore never change.
#
hook_marker = 'azul-claudehook-0605'


# The `envhook.py` site hook is mutually exclusive with this script so we pass
# -S to python to disable it.
#
# Both paths are absolute, naming the worktree this registration is for.
# Claude Code hands the registration to every instance of the repository, and
# the hook recognizes the ones it isn't for by comparing its own worktree
# against theirs.
#
# The interpreter is named explicitly because `python` may be absent outside an
# active virtualenv, and `python3` may be too old to parse this script. Either
# way the hook would die before it could report the very precondition it is
# there to enforce.
#
# Should the worktree move, the script is no longer where the registration
# says, and we skip rather than fail, leaving the command to run without an
# environment.
#
def _hook_command() -> str:
    # We should not resolve the path to executable because in a virtualenv it is
    # usually a symbolic link to the real interpreter
    python = Path(sys.executable)
    if python.is_relative_to(root_dir):
        script = shlex.quote(str(this_script.resolve()))
        python = shlex.quote(str(python))
        return '; '.join([
            f'script={script}',
            'if [ -f "$script" ]',
            f'then exec {python} -S "$script" hook',
            'fi'
        ]) + f' # {hook_marker}'
    else:
        _error(f"""
            The Python interpreter running this script does not belong to the
            virtualenv for this worktree. Activate that virtualenv and try
            registering the hook again.

            Interpreter: {python}
            Worktree: {root_dir}
            """)


hook_matcher = 'Bash'

settings_file = root_dir / '.claude' / 'settings.local.json'

# The deployment selected for this worktree lives here. `_select` maintains
# the file and `envhook.py` injects it into the Python processes PyCharm starts.
#
hook_env_file = root_dir / 'environment.hook'

restart_warning = 'Exit every `claude` instance running in this project'


def register() -> None:
    settings = _load_settings()
    if _find_registration(settings) is None:
        hook_command = _hook_command()
        group = {
            'matcher': hook_matcher,
            'hooks': [{'type': 'command', 'command': hook_command}]
        }
        events = settings.setdefault('hooks', {})
        groups = events.setdefault(hook_event, [])
        groups.append(group)
        _save_settings(settings)
        print(f'Registered {hook_command} as a {hook_event} hook on '
              f'{hook_matcher} in {settings_file}')
        print(restart_warning)
    else:
        print(f'Already registered in {settings_file}')


def unregister() -> None:
    settings = _load_settings()
    registration = _find_registration(settings)
    if registration is None:
        print(f'Not registered in {settings_file}')
    else:
        group, entry = registration
        # Finding a registration proves that the containers holding it exist
        hooks = settings['hooks'][hook_event]
        group['hooks'].remove(entry)
        # Leave no empty containers behind, so that unregistering restores the
        # settings to what they were before registering.
        if not group['hooks']:
            hooks.remove(group)
        if not hooks:
            del settings['hooks'][hook_event]
        if not settings['hooks']:
            del settings['hooks']
        _save_settings(settings)
        print(f'Unregistered {entry['command']} from {settings_file}')
        print(restart_warning)


def _selected_env() -> dict[str, str]:
    """
    The variables recorded in `environment.hook` for this worktree.
    """
    # We parse  the file with the same function `envhook.py` uses, so that both
    # hooks agree on its contents.
    export_environment = _import_sibling('export_environment')
    try:
        env = export_environment.load_env_file(hook_env_file)
    except FileNotFoundError:
        env = {}
    if export_environment.azul_current_deployment not in env:
        _error("""
            No deployment is selected for this worktree. Run `_select` in a
            another shell that has this worktree's environment sourced, then
            ask Claude Code to retry. A restart of `claude` is not needed.
            """)
    return env


def _import_sibling(module_name: str):
    """
    A normal import wouldn't find a sibling script because Claude Code runs
    this one by absolute path, leaving the directory containing it off the
    module search path.
    """
    path = this_script.parent / (module_name + '.py')
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _find_registration(settings: dict) -> tuple[dict, dict] | None:
    return next(
        (
            (group, entry)
            for group in settings.get('hooks', {}).get(hook_event, [])
            for entry in group.get('hooks', ())
            if hook_marker in entry.get('command', '')
        ),
        None
    )


def _load_settings() -> dict:
    try:
        text = settings_file.read_text()
    except FileNotFoundError:
        settings = {}
    else:
        if text:
            settings = json.loads(text)
        else:
            settings = {}
    assert isinstance(settings, dict), settings
    return settings


def _save_settings(settings: dict) -> None:
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    with _write_file_atomically(settings_file) as f:
        json.dump(settings, f, indent=2)
        f.write('\n')


# A copy of azul.lib.files.write_file_atomically. We can't import that module
# because doing so initializes the `azul` package, which needs distributions
# that -S hides from us, and an environment that this script is about to load.
# Claude Code watches the settings so we must not truncate and rewrite them,
# which a read could catch halfway through.
#
@contextmanager
def _write_file_atomically(path, mode=0o644):
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


main(sys.argv[1:])
