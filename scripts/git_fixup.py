"""
EXPERIMENTAL — written by Claude Code (claude-opus-4-6).

Stage the first hunk from the current unstaged diff and create a fixup
commit targeting the commit that last touched the first changed line of
that hunk. With ``-n``/``--dry-run``, print the command instead of
running it, and don't stage anything.
"""

import argparse
import os
import re
import shlex
import subprocess
import sys
import tempfile


def run(*args):
    result = subprocess.run(args, capture_output=True, text=True, check=True)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-n', '--dry-run', action='store_true',
                        help='Print the commit command without staging or committing')
    args = parser.parse_args()

    diff = run('git', 'diff')
    if not diff:
        print('No unstaged changes.', file=sys.stderr)
        sys.exit(1)

    # Split the diff into per-file sections. Each starts with "diff --git …"
    file_starts = [m.start() for m in re.finditer(r'^diff --git ', diff, re.MULTILINE)]
    if not file_starts:
        print('No hunks found.', file=sys.stderr)
        sys.exit(1)
    first_file_diff = diff[file_starts[0]:file_starts[1]] if len(file_starts) > 1 else diff[file_starts[0]:]

    # Within that file section, isolate the file header and the first hunk.
    hunk_starts = [m.start() for m in re.finditer(r'^@@', first_file_diff, re.MULTILINE)]
    if not hunk_starts:
        print('No hunks found.', file=sys.stderr)
        sys.exit(1)
    file_header = first_file_diff[:hunk_starts[0]]
    if len(hunk_starts) > 1:
        first_hunk = first_file_diff[hunk_starts[0]:hunk_starts[1]]
    else:
        first_hunk = first_file_diff[hunk_starts[0]:]

    # Build a minimal patch containing only the first hunk and stage it.
    if not args.dry_run:
        patch = file_header + first_hunk
        if not patch.endswith('\n'):
            patch += '\n'
        fd, patch_path = tempfile.mkstemp(suffix='.patch')
        try:
            os.write(fd, patch.encode())
            os.close(fd)
            run('git', 'apply', '--cached', patch_path)
        finally:
            os.unlink(patch_path)

    # Determine the old-file path from the diff header.
    m = re.match(r'diff --git a/(.*?) b/', first_file_diff)
    file_path = m.group(1)

    # Find the line number of the first removed line in the hunk.
    m = re.match(r'@@ -(\d+)', first_hunk)
    old_start = int(m.group(1))
    hunk_lines = first_hunk.split('\n')[1:]  # skip the @@ header
    offset = 0
    for line in hunk_lines:
        if line.startswith('-'):
            break
        if not line.startswith('+'):
            offset += 1
    first_line = old_start + offset

    # Blame the first changed line (against HEAD, before our uncommitted changes).
    blame = run('git', 'blame', '-l', f'-L{first_line},{first_line}', 'HEAD', '--', file_path)
    commit_hash = blame.split()[0]

    title = run('git', 'log', '--format=%s', '-1', commit_hash).strip()
    commit_args = ['git', 'commit', '-m', f'fixup! {title}']
    if args.dry_run:
        print(shlex.join(commit_args))
    else:
        run(*commit_args)


if __name__ == '__main__':
    main()
