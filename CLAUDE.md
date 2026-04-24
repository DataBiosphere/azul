# Useful commands

- `make pep8` — lint check
- `make isort` — fix import ordering issues
- `make format` — fix other Python source code formatting issues  
- `mypy` — type check (no arguments; checks only files configured in `.mypy.ini`)


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

- Prefer to use `git mv` when renaming or moving files

- Do not commit any changes unless explicitly asked to do so. However, it's OK
  to propose committing changes. When committing changes, include a trailer in 
  the commit message that attributes the change to you

- You can usually disregard any files under `attic/`, except for reference.
  Never modify the attic, except when instructed to move files there.

- Passing `--config-file .mypy.ini` to `mypy` is unnecessary; since `.mypy.ini` 
  is the default config
