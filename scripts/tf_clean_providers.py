"""
Remove Terraform provider versions not pinned in this working copy's lock files.
Optionally clean the shared plugin cache of versions no longer used by any
working copy.

Usage with two working copies, `azul1` and `azul2`:

```
azul1$ python scripts/tf_clean_providers.py --dry-run
```

Inspect the output and if everything looks good, invoke the script again without
--dry-run:

```
azul1$ python scripts/tf_clean_providers.py
```

The above removes unpinned provider versions from all deployment data
directories in the current working copy. Repeat this for each remaining working
copy:

```
azul2$ python scripts/tf_clean_providers.py --dry-run
azul2$ python scripts/tf_clean_providers.py
```

If you use Terraform's plugin cache (as recommended by the Azul README): In the
last remaining working copy, invoke the script again with `--clean-cache`:

```
azul2$ python scripts/tf_clean_providers.py --clean-cache --dry-run
azul2$ python scripts/tf_clean_providers.py --clean-cache
```

The script invocation in `azul1` will have created a marker file in the cache
directory and also touched each cached provider still in use. The next
invocation in `azul2` will leave the marker file untouched but touch all cached
providers in use by azul2. The `--clean-cache` mode then removes cached
versions not touched since the marker was created, and removes the marker.
"""
import logging
import os
from pathlib import (
    Path,
)
import re
import shutil

from azul.lib import (
    R,
)
from azul.lib.strings import (
    format_and_dedent,
)

log = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent

marker_name = '.azul_tf_clean_providers'


def pinned_versions() -> set[tuple[str, str, str]]:
    result = set()
    terraform_dir = project_root / 'terraform'
    for lock_file in terraform_dir.rglob('.terraform.lock.hcl'):
        namespace, provider = None, None
        for line in lock_file.read_text().splitlines():
            m = re.match(r'\s*provider\s+"registry\.terraform\.io/([^/]+)/([^"]+)"', line)
            if m:
                namespace, provider = m.group(1), m.group(2)
                continue
            if namespace is not None:
                assert provider is not None
                m = re.match(r'\s*version\s*=\s*"([^"]+)"', line)
                if m:
                    result.add((namespace, provider, m.group(1)))
                    namespace, provider = None, None
    return result


def plugin_cache_dir() -> Path | None:
    env_dir = os.environ.get('TF_PLUGIN_CACHE_DIR')
    if env_dir:
        return Path(env_dir)
    rc = Path(os.environ.get('TF_CLI_CONFIG_FILE', Path.home() / '.terraformrc'))
    if not rc.exists():
        return None
    for line in rc.read_text().splitlines():
        m = re.match(r'\s*plugin_cache_dir\s*=\s*"([^"]+)"', line)
        if m:
            path = m.group(1).replace('$HOME', str(Path.home()))
            return Path(path)
    return None


def find_installed(base: Path) -> dict[tuple[str, str, str], Path]:
    result: dict[tuple[str, str, str], Path] = {}
    registry = base / 'registry.terraform.io'
    if not registry.is_dir():
        return result
    for namespace_dir in sorted(registry.iterdir()):
        if not namespace_dir.is_dir():
            continue
        for provider_dir in sorted(namespace_dir.iterdir()):
            if not provider_dir.is_dir():
                continue
            for version_dir in sorted(provider_dir.iterdir()):
                if not version_dir.is_dir():
                    continue
                key = (namespace_dir.name, provider_dir.name, version_dir.name)
                result[key] = version_dir
    return result


def remove_version(version_dir: Path, *, dry_run: bool) -> int:
    size = sum(f.stat().st_size for f in version_dir.rglob('*') if f.is_file())
    if dry_run:
        log.info('Would remove %s (%.1f MB)', version_dir, size / 1e6)
    else:
        shutil.rmtree(version_dir)
        log.info('Removed %s (%.1f MB)', version_dir, size / 1e6)
        for parent in (version_dir.parent, version_dir.parent.parent):
            try:
                parent.rmdir()
            except OSError:
                break
    return size


def clean_deployments(pinned: set, *, dry_run: bool) -> int:
    deployments_dir = project_root / 'deployments'
    total = 0
    for tf_data_dir in sorted(deployments_dir.rglob('.terraform.*')):
        if not tf_data_dir.is_dir():
            continue
        providers_dir = tf_data_dir / 'providers'
        if providers_dir.is_dir() and not providers_dir.is_symlink():
            installed = find_installed(providers_dir)
            for key, path in sorted(installed.items()):
                if key not in pinned:
                    total += remove_version(path, dry_run=dry_run)
    return total


def touch_cached_targets(cache_dir: Path, *, dry_run: bool) -> set[Path]:
    deployments_dir = project_root / 'deployments'
    touched = set()
    cache_registry = cache_dir / 'registry.terraform.io'
    for tf_data_dir in sorted(deployments_dir.rglob('.terraform.*')):
        if not tf_data_dir.is_dir():
            continue
        providers_dir = tf_data_dir / 'providers'
        if not providers_dir.is_dir() or providers_dir.is_symlink():
            continue
        for entry in providers_dir.rglob('*'):
            if entry.is_symlink():
                target = entry.resolve()
                try:
                    rel = target.relative_to(cache_registry)
                except ValueError:
                    continue
                parts = rel.parts
                if len(parts) >= 3:
                    version_dir = cache_registry / parts[0] / parts[1] / parts[2]
                    if version_dir not in touched and version_dir.is_dir():
                        if not dry_run:
                            os.utime(version_dir)
                        touched.add(version_dir)
    return touched


def clean_cache(cache_dir: Path, marker: Path, *, dry_run: bool) -> int:
    marker_mtime = marker.stat().st_mtime
    installed = find_installed(cache_dir)
    total = 0
    for key, version_dir in sorted(installed.items()):
        if version_dir.stat().st_mtime < marker_mtime:
            total += remove_version(version_dir, dry_run=dry_run)
    if dry_run:
        log.info('Would remove marker %s', marker)
    else:
        marker.unlink()
        log.info('Removed marker %s', marker)
    return total


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Remove Terraform provider versions not pinned in lock files.'
    )
    parser.add_argument(
        '-n', '--dry-run',
        action='store_true',
        help='Show what would be removed without actually deleting anything.'
    )
    parser.add_argument(
        '--clean-cache',
        action='store_true',
        help='Remove cached provider versions not touched since the marker was'
             ' created. Run without this flag in every working copy first.'
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    cache_dir = plugin_cache_dir()
    total = 0

    if args.clean_cache:
        assert cache_dir is not None, R(
            'No Terraform plugin cache is configured')
        assert cache_dir.is_dir(), R(
            'Plugin cache does not exist or is not a directory', cache_dir)
        marker = cache_dir / marker_name
        assert marker.exists(), R(
            'No marker file found, run without --clean-cache in every working copy first', marker)
        log.info('Cleaning plugin cache: %s', cache_dir)
        total += clean_cache(cache_dir, marker, dry_run=args.dry_run)
    else:
        pinned = pinned_versions()
        assert pinned, R(
            'No pinned provider versions found in lock files')

        log.info('Pinned provider versions:')
        for namespace, provider, version in sorted(pinned):
            log.info('  %s/%s %s', namespace, provider, version)

        log.info('Deployment data directories:')
        total += clean_deployments(pinned, dry_run=args.dry_run)

        if cache_dir is None:
            log.info(format_and_dedent('''
                Note: No Terraform plugin cache is configured. Without a
                cache, each deployment stores its own copy of every provider
                binary. See the Terraform section in README.md for setup
                instructions.
            '''))
        else:
            assert cache_dir.is_dir(), R(
                'Plugin cache does not exist or is not a directory', cache_dir)
            marker = cache_dir / marker_name
            if not marker.exists():
                if not args.dry_run:
                    marker.touch()
                verb = 'Would create' if args.dry_run else 'Created'
                log.info('%s marker %s', verb, marker)
            touched = touch_cached_targets(cache_dir, dry_run=args.dry_run)
            if touched:
                verb = 'Would touch' if args.dry_run else 'Touched'
                log.info('%s %d cached provider version(s):', verb, len(touched))
                for path in sorted(touched):
                    log.info('  %s', path)

    if total > 0:
        verb = 'Would free' if args.dry_run else 'Freed'
        log.info('%s %.1f MB', verb, total / 1e6)
    else:
        log.info('Nothing to clean up.')


if __name__ == '__main__':
    main()
