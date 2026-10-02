# Useful commands

- `make pep8` — lint check
- `make isort` — fix import ordering issues
- `make format` — fix other Python source code formatting issues
- `mypy` — type check (no arguments; checks only files configured in `.mypy.ini`)


# Environment

Most commands need this project's environment. There are three ways for a
command to get one, depending on whether the optional `scripts/claudehook.py`
hook is registered and on how Claude Code was started. The hook is registered
with `python -S scripts/claudehook.py register` and unregistered with `python
-S scripts/claudehook.py unregister`. Both commands edit
`.claude/settings.local.json` in the repository's *main* worktree. Claude Code
applies that file to sessions in all of the repository's worktrees, so a linked
worktree needs no copy of its own, and the absence of one there says nothing
about whether the hook is registered.

A command in which `azul_env_hash` is set has an environment, but the variable
does not tell how it got one: the hook compiles a fresh one for every command,
whereas a session started from a shell that sourced `environment` inherits
one. To tell the two apart, look at the environment of the `claude` process
itself, which `ps eww -p <pid>` prints. With the hook, `azul_env_hash` is
absent there, because `claude` must have been started without `environment`
sourced.

- *Hook registered*: nothing is needed. The hook prefixes every command,
  whether from the `Bash` tool or the `Monitor` tool, with the loading of a
  freshly compiled environment. A monitor's command is prefixed only once, when
  the monitor starts, so a long-running monitor keeps the environment,
  deployment and credentials it started with and must renew them itself if it
  outlives them. Claude Code must have been started from a shell with the
  virtualenv activated but *without* `environment` sourced. The hook enforces
  both conditions by blocking every command with a message that names the two
  ways out: restarting `claude` as described, or unregistering the hook. Relay
  that message. Neither remedy can be applied from within the session, because
  the session inherited the defect from the shell that started it.

  Because the environment is recompiled for every command, the deployment can
  be switched between commands. The hook reads `azul_current_deployment` from
  `environment.hook` in the working copy, a file that `_select` maintains, and
  a change to that file takes effect on the very next command. The selection
  lives in that file rather than in Claude Code's settings because those
  settings are shared by all worktrees of a repository, so a deployment kept
  there could not be specific to one working copy. Ask the user before
  switching the deployment yourself, and keep in mind that every `_select`
  rewrites that file, even one run as part of a larger command, and thereby
  switches the deployment for all subsequent commands.

  The hook also runs `_login_aws` before every command, so that the AWS session
  credentials are present without having been inherited. This matters to
  Terraform, whose provider configuration names no profile. Refreshing those
  credentials requires an MFA token and therefore a terminal. Once they expire,
  the hook prints `Expired AWS credentials for <deployment> (see Environment in
  CLAUDE.md).` followed by `_login_aws failed` before every command, but blocks
  none of them. This happens all the time and is only a distraction when the
  task at hand doesn't involve AWS, so don't mention it then. Only when a
  command fails for lack of credentials, or the task is about to need them,
  relay the message, naming the deployment, and ask the user to run
  `_reselect` in a terminal on this working copy, or `_login_aws` if that
  terminal already has the same deployment selected. The user's terminal may
  have a different deployment selected than the hook does, in which case
  `_login_aws` there would refresh the credentials of the wrong account. No
  restart is needed.

- *No hook, `azul_env_hash` unset*: prefix every command that needs the
  environment with `export azul_env_quiet=1` followed by `source environment ||
  exit`. Sourcing affects only the command that does it, so the prefix is
  needed every time. The `|| exit` keeps the command from running without an
  environment, and `azul_env_quiet` suppresses the 80-odd lines of diagnostics
  that would otherwise precede the command's own output.

- *No hook, `azul_env_hash` set*: the legacy arrangement. Claude Code was
  started from a shell that had sourced `environment`, so every command
  inherits a copy of that environment. Nothing is needed until the copy goes
  stale. A change to `environment.py`, or a different
  `azul_current_deployment`, makes `envhook.py` fail every Python command with
  `The environment is stale`. The AWS session credentials in the copy also
  expire on their own after a few hours. Since they are not derived from any
  `environment.py`, their expiry leaves `azul_env_hash` unchanged and shows up
  not as a complaint from `envhook.py` but as authentication failures from
  anything that calls AWS. Either way, recovery requires the user to re-source
  `environment`, restart Claude Code and resume the session.


# Guidelines

- Follow the directives in `CONTRIBUTING.rst` in addition to those in this
  document

- After making any code changes, always verify them with `make pep8` and `mypy`

- Don't order imports by hand; run `make isort` instead

- When extracting code from a module covered by `mypy` into a new module, add
  the new module to `.mypy.ini`. Ask for confirmation before making any change
  that would reduce `mypy` coverage

- `.mypy.ini` covers a module in one of two ways: *explicitly*, by listing its
  fully qualified name in the `modules` section, or *implicitly*, by listing
  one of its ancestor packages in the `packages` section

- Append new entries at the end of the `modules` list in `.mypy.ini`

- Prefer `git mv` for renaming or moving files

- Do not commit or amend unless explicitly asked to. A prior request to commit
  does not authorize later commits or amends, though proposing a commit is
  always fine. When you do commit, add a trailer to the commit message that
  attributes the change to you

- Files under `attic/` can usually be disregarded, except as reference. Never
  modify the attic, except when instructed to move files there

- `.mypy.ini` is the default config for `mypy`, so passing `--config-file
  .mypy.ini` is unnecessary

- Do not quote annotations. The project uses Python 3.14, which defers the
  evaluation of annotations by default (PEP 649), so forward references and
  `TYPE_CHECKING`-guarded imports need no quotes

- When any `assert` with `R()` in a function needs line wrapping, wrap all of
  them in that function the same way:
  ```python
  assert condition, R(
      'message', value)
  ```
  `R(` ends the `assert` line and the closing `)` ends the next one

- Combine pairs of symmetric assignments like `a = foo(x)` and `b = foo(y)`
  into one tuple assignment, `a, b = foo(x), foo(y)`, unless the result would
  need to be wrapped
