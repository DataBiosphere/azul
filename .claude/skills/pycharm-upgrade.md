---
name: pycharm-upgrade
description: "Determine anew which files to extract from the PyCharm distribution, after `make format` broke following a bump of `azul_pycharm_version`."
user_invocable: true
---

# PyCharm upgrade

Apply this skill only when `make format` fails after `azul_pycharm_version` was
bumped, as instructed by the comment preceding that variable in
`environment.py`. If `make format` passes, the upgrade is done and none of what
follows is needed.

The image built from `Dockerfile` contains the PyCharm formatter that `make
format` and `make _format` invoke. Only part of the distribution is extracted
from the archive: the platform, the launchers and the plugins PyCharm considers
essential. The bundled JetBrains Runtime, four dozen other plugins and the
helper scripts of the Python plugin are left behind, and the distribution's JRE
is installed instead of the bundled one.

That set is coarse on purpose, in whole directories, so it usually survives an
upgrade untouched. When it doesn't, this is how to establish the new one. Budget
half an hour, most of it waiting for builds.

Every step compares formatted output against a reference produced by the image
that is being replaced. Nothing is accepted because it looks right.

## Step 0: Triage the failure

Two kinds of failure lead here, and they need different fixes.

- The image doesn't build, because the download 404s. JetBrains renamed the
  archive. Fix the `tarball=` assignment in `Dockerfile` and the URL in the
  `pycharm_checksums` target, rerun `make pycharm_checksums`, and try again;
  the rest of this skill does not apply.

- The image builds but the formatter fails to start, or formats differently.
  The extracted set is no longer sufficient. Continue below.

## Step 1: Note what changed

The bump has already happened. Record which version the branch came from, since
Step 2 needs an image built from it, and read the release notes for anything
about bundled plugins or the layout of the distribution.

## Step 2: Establish the reference

Build an image from the version the branch came from and capture what it
produces. The formatter rewrites files in place, so work on throwaway copies of
the sources, never on the working tree. Make two, e.g. with `git archive HEAD |
tar -x -C <dir>`:

- one left as checked out, which proves the formatter changes nothing, and

- one whose formatting has been mangled beforehand, which proves it changes the
  right things. Mangle with something crude, e.g. `perl -0pi -e 's/, /,/g;
  s/ = /=/g'` over every source in `relative_sources`.

Format both with that image and keep the results. They are the reference that
every later step is compared against.

The unmangled copy alone is a weak reference: almost any broken configuration
also leaves an already formatted file untouched.

## Step 3: Confirm the new version formats identically

Build an image with the new version, extracting the *whole* archive for now, and
format both copies. If the output differs from the reference, the upgrade
changes our formatting. Stop and report that to the user; whether to accept a
reformat of the code base is their decision, not something to absorb into this
task.

## Step 4: Find the plugins the platform now needs

Extract nothing under `plugins/` and run the formatter. The platform refuses to
start and names what it needs:

    EssentialPluginMissingException: Missing essential plugins: PythonCore, …

Map those IDs to directory names and compare against the patterns in
`Dockerfile`. Adjust the patterns to match. This is the most likely reason for
the failure that brought you here.

## Step 5: Restore the filtered extraction and validate

Put the `tar` patterns back and build again. Then, each against the reference
from Step 2:

1. both copies, all sources, byte identical

2. the other architecture, via `docker build --platform` and `docker run
   --platform`

3. `make _format` inside an image built by `make docker_image`

4. the GitHub build, which runs `make format` followed by `make check_clean`

Report the size of `/opt/pycharm` and the line count of one run's output. Both
are useful regression signals: a sudden jump in size means a pattern is matching
more than intended, and a jump in noise means something the platform wants is no
longer extracted.

## Gotchas

`--no-wildcards-match-slash` in the `tar` invocation is load-bearing. Without
it a `*` matches `/` too, so `*/lib` also matches everything below `lib` and
`--exclude` patterns are defeated by the include patterns that follow. An
earlier attempt without it extracted 1.6 GB where the correct patterns extract
915 MB, and reported no error while doing so.

The plugins are named individually rather than as a directory because
`plugins/plugin-classpath.txt` must not be extracted. It indexes the JARs of all
bundled plugins, and with it in place the platform fails to start unless every
one of them is present.

Extraction is an include list, so anything new in a future release is *absent*
by default rather than merely unwanted. A newly required file therefore fails
loudly at startup instead of quietly bloating the image, which is the failure
mode to expect when the platform's layout changes.

The JRE installed from the distribution must match the major version of the
bundled runtime. Extract `jbr/release` from the archive and read `JAVA_VERSION`
before assuming the pinned `openjdk-*-jre-headless` is still right.

Expect on the order of a hundred lines of stack traces per run even when
everything is correct; `make _format` discards stderr for that reason. Judge
success by the `N file(s) formatted` line, the exit status and the diff, never
by the absence of exceptions.
