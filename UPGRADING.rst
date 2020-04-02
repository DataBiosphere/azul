Upgrading
---------

This file documents any upgrade procedure that must be performed. Because we
don't use a semantic version, a change that requires explicit steps to upgrade
a is referenced by its Github issue number. After checking out a branch that
contains a commit resolving an issue listed here, the steps listed underneath
the issue need to be performed. When switching away from that branch, to a
branch that does not have the listed changes, the steps would need to be
reverted. This is all fairly informal and loosely defined. Hopefully we won't
have too many entries in this file.


#1637 Refactor handling of environment for easier reuse
=======================================================

1) Run ::

      scripts/convert_environment.py deployments/foo.local/environment{,.local}

   where ``foo.local`` is the name of your personal deployment. This should
   create ``environment.py`` and possibly ``environment.local.py`` with
   essentially the same settings, but in Python syntax.

2) Close the shell, start a new one and activate your venv

3) Run ``source environment``

4) Run ``_select foo.local``

5) If you use ``envhook.py``

   * Confirm that PyCharm picks up the new files via ``envhook.py`` by starting a
     Python console inside PyCharm or running a unit test

   * Confirm that running ``python`` from a shell picks up the new files via
     ``envhook.py``

7) Confirm that ``make deploy`` and ``make terraform`` still work
