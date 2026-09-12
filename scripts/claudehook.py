"""
Load this project's environment before every command that Claude Code runs, by
registering this script as a `PreToolUse` hook on Claude Code's `Bash` tool.

An Azul working copy, or Claude Code *project*, has to opt in to the hook by
registering it in the settings of that project (`.claude/settings.local.json`).
Run this script with the `register` argument to add that registration, or the
`unregister` argument to remove it.

This eliminates the need to run `claude` with an already populated Azul
environment, which can quickly become stale and require a restart of `claude`
followed by a resumption of the session. To that end, `claude` must be started
in an *unpopulated* environment, with only `azul_current_deployment` set. A
convenient way to set that variable is via `env` in the project settings
mentioned above, which the `register` argument seeds from the environment it
runs in. The variable can then be changed by Claude Code mid-session, without
restarting `claude`.

Combining this hook with an already populated environment is not supported, and
neither is running `claude` without an active virtualenv. Either way, every
command fails with a message naming the two ways out: starting `claude` as
described above, or unregistering the hook. Nothing recompiles an inherited
environment, so selecting a deployment elsewhere would replace
`azul_current_deployment` in it without replacing anything derived from it.
"""
import argparse
from contextlib import (
    contextmanager,
)
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


# The only way to escalate a hook error is via exit status 2. Any other non-zero
# status merely warns, letting the command run without an environment.
#
def excepthook(*args):
    sys.__excepthook__(*args)
    sys.exit(2)


sys.excepthook = excepthook

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
        choices=['register', 'hook', 'unregister', 'select'],
        help=f'Register this script as a {hook_event} hook in the project-wide '
             f'Claude Code settings, perform the hook, unregister the hook, or '
             f'record the current deployment in those settings.'
    )
    parser.add_argument(
        'deployment',
        nargs='?',
        help='With the `select` action, the deployment to record, or the empty '
             'string to retract the one on record. Not accepted otherwise.'
    )
    options = parser.parse_args(argv)
    if options.action == 'select':
        if options.deployment is None:
            parser.error('the `select` action requires a deployment')
        select(options.deployment)
    elif options.deployment is not None:
        parser.error(f'the `{options.action}` action takes no deployment')
    elif options.action == 'hook':
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
    if 'VIRTUAL_ENV' not in os.environ:
        raise Exception(dedent("""
            This `claude` instance was started without an active virtualenv,
            which this hook needs in order to prepare the Azul environment.

            If you want to continue using the hook (recommended), exit this
            `claude` instance, run `source .venv/bin/activate` and then start
            `claude` again.

            If you want to continue using `claude` without a virtualenv (not
            recommended), exit this `claude` instance, run `source
            .venv/bin/activate`, `make claudeunhook`, `deactivate`, and start
            `claude` again.
            """[1:]))
    elif azul_env_vars in os.environ:
        raise Exception(dedent("""
            This `claude` instance inherited a populated Azul environment and
            the hook is registered. This mode of operation is not supported.

            If you want to continue using the hook (recommended), exit this
            `claude` instance and start it again from a new shell, without first
            sourcing Azul's environment.

            If you want to continue using `claude` from a populated
            environment (not recommended), exit this `claude` instance,
            run `make claudeunhook` and start `claude` again.
            """[1:]))
    else:
        tool_input = hook_input['tool_input']
        environment = shlex.quote(str(root_dir / 'environment'))
        command = '\n'.join([
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


# The `envhook.py` site hook is mutually exclusive with this script so we pass
# -S to python to disable it.
#
hook_command = 'python -S "$CLAUDE_PROJECT_DIR"/scripts/claudehook.py hook'

hook_matcher = 'Bash'

settings_file = root_dir / '.claude' / 'settings.local.json'

azul_current_deployment = 'azul_current_deployment'

restart_warning = 'Exit every `claude` instance running in this project'


def register() -> None:
    settings = _load_settings()
    if _find_registration(settings) is None:
        group = {
            'matcher': hook_matcher,
            'hooks': [{'type': 'command', 'command': hook_command}]
        }
        events = settings.setdefault('hooks', {})
        groups = events.setdefault(hook_event, [])
        groups.append(group)
        # Seed the entry from the environment this runs in, so that a session
        # started afterwards inherits the deployment selected here
        env = settings.setdefault('env', {})
        deployment = os.environ.get(azul_current_deployment, '')
        env[azul_current_deployment] = deployment
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
        # Also remove the variable specifying the current deployment. Note that
        # removing the variable does not unset it in currently running Claude
        # instances. However, our workaround of setting it to the empty string
        # would deselect any deployment in current and future instances,
        # regardless of what was selected before they were launched. This would
        # torpedo the legacy semantics that unregistering the hook is supposed
        # to restore. Therefore, removing the variable *and* stopping any
        # currently running instances is the only safe course of action.
        env = settings.get('env')
        if env is not None:
            env.pop(azul_current_deployment, None)
            if not env:
                del settings['env']
        _save_settings(settings)
        print(f'Unregistered {hook_command} from {settings_file}')
        print(restart_warning)


def select(deployment: str) -> None:
    """
    Record the given deployment in the environment that Claude Code passes
    to every command it runs. An empty argument results in an environment with
    no deployment selected.

    If there are no Claude Code settings in the working copy, or if the hook
    isn't registered, this method doesn't modify anything.
    """
    if settings_file.exists():
        settings = _load_settings()
        if _find_registration(settings) is not None:
            env = settings.setdefault('env', {})
            # Retract by emptying the entry rather than removing it, which
            # would leave Claude Code passing its last value for the rest of
            # the session
            if env.get(azul_current_deployment) != deployment:
                env[azul_current_deployment] = deployment
                _save_settings(settings)


def _find_registration(settings: dict) -> tuple[dict, dict] | None:
    return next(
        (
            (group, entry)
            for group in settings.get('hooks', {}).get(hook_event, [])
            if group.get('matcher') == hook_matcher
            for entry in group.get('hooks', ())
            if entry.get('command') == hook_command
        ),
        None
    )


def _load_settings() -> dict:
    try:
        f = open(settings_file)
    except FileNotFoundError:
        return {}
    else:
        with f:
            return json.load(f)


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
