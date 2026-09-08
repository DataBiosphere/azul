# Useful commands

- `make pep8` — lint check
- `make isort` — fix import ordering issues
- `make format` — fix other Python source code formatting issues  
- `mypy` — type check (no arguments; checks only files configured in `.mypy.ini`)


# Environment

Most commands need this project's environment, which reaches them in one of
three ways. The optional `scripts/claudehook.py` hook is registered in
`.claude/settings.local.json` by `scripts/claudehook.py register`, and removed
from it by `scripts/claudehook.py unregister`. Whether a command has an
environment at all is evident from `azul_env_hash` being set in it.

- *Hook registered*: nothing is needed, because the hook prefixes every command
  with the loading of a freshly compiled environment. Claude Code must have been
  started from a shell with the virtualenv activated but *without* `environment`
  sourced. The hook enforces that, blocking every command with `Run 'source
  .venv/bin/activate' first` or `azul_env_vars is set: ...` respectively; relay
  whichever applies and ask the user to relaunch from a shell in that state.

  Because the environment is recompiled per command, the deployment can be
  switched between commands: it comes from `azul_current_deployment`, which is
  most conveniently set via `env` in the same settings file, and a change to
  that value takes effect on the very next command. Ask the user before
  switching it yourself. The AWS session credentials are neither part of the
  environment nor inherited in this case, so anything calling AWS resolves them
  from the shared CLI cache; when they lapse, the user refreshes them with
  `_login_aws` in a terminal, again without a restart.

- *No hook, and `azul_env_hash` unset*: prefix every command that needs the
  environment with `export azul_env_quiet=1` followed by `source environment ||
  exit`. Sourcing affects only the command that does it, so the prefix is needed
  every time. The `|| exit` prevents the command from running without an
  environment, and `azul_env_quiet` suppresses the 80-odd lines of diagnostics
  that would otherwise precede the command's own output.

- *No hook, but `azul_env_hash` set*: the legacy arrangement, in which Claude
  Code was started from a shell that had sourced `environment`, so that every
  command inherits a copy of it. Nothing is needed until that copy goes stale.
  An `environment.py` change, or a different `azul_current_deployment`, makes
  `envhook.py` fail every Python command with `The environment is stale`. The
  AWS session credentials in the copy also expire on their own after a few
  hours, which cannot change `azul_env_hash` because they are not derived from
  any `environment.py`, and so surfaces as authentication failures from
  anything calling AWS rather than as a complaint from `envhook.py`. Either way,
  recovering requires the user to re-source `environment`, restart Claude Code
  and resume the session.


# Guidelines

- In addition to the directives in this document, also respect those contained
  in `CONTRIBUTING.rst`

- After making any code changes, always verify them with `make pep8` and `mypy` 

- Don't worry about manually ordering imports. Instead, just run `make isort`

- When extracting code out of a module covered by `mypy` into a new module,
  remember to add the new module to `.mypy.ini`. Ask for confirmation before
  making changes that would reduce `mypy` coverage

- Remember that in `.mypy.ini`, there are two ways to configure a Python module
  for coverage by `mypy`: *explicitly*, by listing its fully qualified module
  path in the `modules` section of that file, or *implicitly*, by listing its
  parent or ancestor package in the `packages` section

- When adding a module to the `modules` list in `.mypy.ini`, always append it
  at the end of the list

- Prefer to use `git mv` when renaming or moving files

- Do not commit or amend any changes unless explicitly asked to do so. A prior
  request to commit does not authorize subsequent commits or amends. However,
  it's OK to propose committing changes. When committing changes, include a
  trailer in the commit message that attributes the change to you

- You can usually disregard any files under `attic/`, except for reference.
  Never modify the attic, except when instructed to move files there.

- Passing `--config-file .mypy.ini` to `mypy` is unnecessary; since `.mypy.ini` 
  is the default config

- Do not quote type hints in annotations. The project uses Python 3.14, which
  defers evaluation of annotations by default (PEP 649), so forward references
  and `TYPE_CHECKING`-guarded imports work without quotes

- When using `assert` with `R()` and at least one assertion in the function
  needs line wrapping, wrap all of them consistently. The convention is:
  ```python
  assert condition, R(
      'message', value)
  ```
  `R(` goes at the end of the `assert` line, the closing `)` at the end of
  the following line

- For pairs of symmetric assignments like `a = foo(x)` and `b = foo(y)`, use
  tuple assignment: `a, b = foo(x), foo(y)`. Do not apply this when it would
  require wrapping the line
