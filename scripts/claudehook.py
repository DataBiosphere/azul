#!/usr/bin/env -S python3 -S
#
# The `envhook.py` site hook is mutually exclusive with this script so we pass
# -S to python to disable it.

"""
Load this project's environment before every command that Claude Code runs, by
registering this script as a `PreToolUse` hook on Claude Code's `Bash` tool.

An Azul working copy, or Claude Code *project*, has to opt in to the hook by
registering it in the settings of that project (`.claude/settings.local.json`).
Run this script with the `register` argument to add that registration, or the
`unregister` argument to remove it.

This eliminates the need to run `claude` with an already populated Azul
environment, which can quickly become stale and require a restart of `claude`
followed by a resumption of the session. In fact, with this hook, `claude` must
be started in an *unpopulated* environment, with only `azul_current_deployment`
set. A convenient way to set that variable is via `env` in the project settings
mentioned above. The variable can then be changed by Claude Code mid-session,
without restarting `claude`.
"""
import argparse
import json
import os
from pathlib import (
    Path,
)
import shlex
import sys


# The only way to escalate a hook error is via exit status 2. Any other non-zero
# status merely warns, letting the command run without an environment.
#
def excepthook(*args):
    sys.__excepthook__(*args)
    sys.exit(2)


sys.excepthook = excepthook

assert __name__ == '__main__', __name__

this_module = Path(__file__)

root_dir = this_module.parent.parent

settings_file = root_dir / '.claude' / 'settings.local.json'

hook_event = 'PreToolUse'

hook_matcher = 'Bash'

hook_command = '"$CLAUDE_PROJECT_DIR"/scripts/claudehook.py hook'


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('action',
                        choices=['hook', 'register', 'unregister'],
                        help='What to do. The `hook` action is the one the '
                             'registration invokes: it reads the input to the '
                             'tool from standard input and writes the modified '
                             'input to standard output. It is required, so '
                             'that running this script without arguments does '
                             'nothing but report its usage.')
    options = parser.parse_args(argv)
    if options.action == 'hook':
        prefix_command()
    elif options.action == 'register':
        register()
    elif options.action == 'unregister':
        unregister()
    else:
        assert False, options.action


def prefix_command() -> None:
    if 'VIRTUAL_ENV' not in os.environ:
        raise Exception("Run 'source .venv/bin/activate' first")
    azul_env_vars = 'azul_env_vars'
    if azul_env_vars in os.environ:
        raise Exception(f'{azul_env_vars} is set: start Claude Code from a '
                        f'shell that has not sourced `environment`')
    hook_input = json.load(sys.stdin)
    tool_input = hook_input['tool_input']
    environment = shlex.quote(str(root_dir / 'environment'))
    prefix = (
        # Suppress the copious diagnostic output from export_environment.py,
        # which would otherwise precede the output of every command.
        f'export azul_env_quiet=1\n'
        # Propagate a failure to load the environment instead of running the
        # command without one.
        f'source {environment} || exit\n'
    )
    hook_output = {
        'hookSpecificOutput': {
            'hookEventName': hook_event,
            'updatedInput': {
                **tool_input,
                'command': prefix + tool_input['command']
            }
        }
    }
    json.dump(hook_output, sys.stdout)


def register() -> None:
    settings = load_settings()
    hooks = settings.setdefault('hooks', {}).setdefault(hook_event, [])
    if find_registration(hooks) is None:
        group = {
            'matcher': hook_matcher,
            'hooks': [{'type': 'command', 'command': hook_command}]
        }
        hooks.append(group)
        save_settings(settings)
        print(f'Registered {hook_command} as a {hook_event} hook on '
              f'{hook_matcher} in {settings_file}')
    else:
        print(f'Already registered in {settings_file}')


def unregister() -> None:
    settings = load_settings()
    hooks = settings.get('hooks', {}).get(hook_event, [])
    registration = find_registration(hooks)
    if registration is None:
        print(f'Not registered in {settings_file}')
    else:
        group, entry = registration
        group['hooks'].remove(entry)
        # Leave no empty containers behind, so that unregistering restores the
        # settings to what they were before registering.
        if not group['hooks']:
            hooks.remove(group)
        if not hooks:
            del settings['hooks'][hook_event]
        if not settings['hooks']:
            del settings['hooks']
        save_settings(settings)
        print(f'Unregistered {hook_command} from {settings_file}')


def find_registration(hooks: list) -> tuple[dict, dict] | None:
    return next(
        (
            (group, entry)
            for group in hooks
            if group.get('matcher') == hook_matcher
            for entry in group.get('hooks', ())
            if entry.get('command') == hook_command
        ),
        None
    )


def load_settings() -> dict:
    try:
        f = open(settings_file)
    except FileNotFoundError:
        return {}
    else:
        with f:
            return json.load(f)


def save_settings(settings: dict) -> None:
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    with open(settings_file, 'w') as f:
        json.dump(settings, f, indent=2)
        f.write('\n')


main(sys.argv[1:])
