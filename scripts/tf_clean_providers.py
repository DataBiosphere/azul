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
from contextlib import (
    suppress,
)
import logging
import os
from pathlib import (
    Path,
)
import re
import shutil

import attrs
from more_itertools import (
    partition,
)

from azul.lib import (
    R,
)
from azul.lib.strings import (
    format_and_dedent,
)

log = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent

marker_name = '.azul_tf_clean_providers'


@attrs.frozen(kw_only=True, order=True)
class Provider:
    """
    The identity of one version of one Terraform provider, as pinned in a lock
    file and as laid out on disk underneath a `registry.terraform.io` directory.
    The same provider version is typically installed in more than one place: in
    each deployment's data directory that uses it, and in the plugin cache.
    """
    namespace: str
    name: str
    version: str

    def __str__(self) -> str:
        return f'{self.namespace}/{self.name} {self.version}'

    def path_in(self, base: Path) -> Path:
        """
        Where this version resides underneath the given providers directory or
        plugin cache, whether or not it is actually installed there.
        """
        return base / 'registry.terraform.io' / self.namespace / self.name / self.version

    def installed_in(self, base: Path) -> ProviderInstallation | None:
        path = self.path_in(base)
        if path.is_dir():
            return ProviderInstallation(provider=self, path=path)
        else:
            return None


@attrs.frozen(kw_only=True)
class ProviderInstallation:
    """
    One provider version as installed in a particular deployment's providers
    directory, or in the plugin cache.
    """
    provider: Provider
    path: Path

    @property
    def linked(self) -> bool:
        """
        True if this installation merely references binaries in the plugin
        cache, so that removing it doesn't free the space they occupy.
        """
        return any(entry.is_symlink() for entry in self.path.iterdir())

    @property
    def size(self) -> int:
        """
        The space occupied by this installation. Zero for a linked one, since
        the binaries it references are in the plugin cache, not here.
        """
        return sum(f.stat().st_size for f in self.path.rglob('*') if f.is_file())

    def remove(self, cache_dir: Path | None, *, dry_run: bool) -> Removal:
        """
        Remove this installation. For a linked one, the binaries it references
        stay behind in the given plugin cache, and freeing their space is
        deferred to a later invocation with --clean-cache.
        """
        linked = self.linked
        if linked:
            cached = None if cache_dir is None else self.provider.installed_in(cache_dir)
            size = 0 if cached is None else cached.size
        else:
            size = self.size
        if not dry_run:
            shutil.rmtree(self.path)
            for parent in (self.path.parent, self.path.parent.parent):
                with suppress(OSError):
                    parent.rmdir()
        if linked:
            verb = 'Would unlink' if dry_run else 'Unlinked'
            log.info('%s %s', verb, self.path)
        else:
            verb = 'Would remove' if dry_run else 'Removed'
            log.info('%s %s (%.1f MB)', verb, self.path, size / 1e6)
        return Removal(provider=self.provider, size=size, deferred=linked)


@attrs.frozen(kw_only=True)
class Removal:
    """
    The removal of one provider installation, and the space it accounts for.
    That space is not freed by the removal itself if it is deferred: the
    installation merely referenced the plugin cache, where the binaries remain
    until a later invocation with --clean-cache removes them.
    """
    provider: Provider
    size: int
    deferred: bool


def pinned_versions() -> set[Provider]:
    result = set()
    terraform_dir = project_root / 'terraform'
    for lock_file in terraform_dir.rglob('.terraform.lock.hcl'):
        namespace, name = None, None
        for line in lock_file.read_text().splitlines():
            m = re.match(r'\s*provider\s+"registry\.terraform\.io/([^/]+)/([^"]+)"', line)
            if m:
                namespace, name = m.group(1), m.group(2)
            elif namespace is not None:
                assert name is not None
                m = re.match(r'\s*version\s*=\s*"([^"]+)"', line)
                if m:
                    result.add(Provider(namespace=namespace,
                                        name=name,
                                        version=m.group(1)))
                    namespace, name = None, None
    return result


def plugin_cache_dir() -> Path | None:
    result = None
    env_dir = os.environ.get('TF_PLUGIN_CACHE_DIR')
    if env_dir:
        result = Path(env_dir)
    else:
        rc = Path(os.environ.get('TF_CLI_CONFIG_FILE', Path.home() / '.terraformrc'))
        if rc.exists():
            pattern = re.compile(r'\s*plugin_cache_dir\s*=\s*"([^"]+)"')
            matches = filter(None, map(pattern.match, rc.read_text().splitlines()))
            m = next(matches, None)
            if m is not None:
                result = Path(m.group(1).replace('$HOME', str(Path.home())))
    return result


def find_installations(base: Path) -> list[ProviderInstallation]:
    registry = base / 'registry.terraform.io'
    if registry.is_dir():
        result = [
            ProviderInstallation(provider=Provider(namespace=namespace_dir.name,
                                                   name=provider_dir.name,
                                                   version=version_dir.name),
                                 path=version_dir)
            for namespace_dir in sorted(registry.iterdir()) if namespace_dir.is_dir()
            for provider_dir in sorted(namespace_dir.iterdir()) if provider_dir.is_dir()
            for version_dir in sorted(provider_dir.iterdir()) if version_dir.is_dir()
        ]
    else:
        result = []
    return result


def clean_deployments(pinned: set[Provider],
                      cache_dir: Path | None,
                      *,
                      dry_run: bool
                      ) -> list[Removal]:
    deployments_dir = project_root / 'deployments'
    removals = []
    for tf_data_dir in sorted(deployments_dir.rglob('.terraform.*')):
        providers_dir = tf_data_dir / 'providers'
        if providers_dir.is_dir() and not providers_dir.is_symlink():
            for installation in find_installations(providers_dir):
                if installation.provider not in pinned:
                    removals.append(installation.remove(cache_dir,
                                                        dry_run=dry_run))
    return removals


def touch_cached_targets(cache_dir: Path, *, dry_run: bool) -> set[Path]:
    deployments_dir = project_root / 'deployments'
    touched = set()
    cache_registry = cache_dir / 'registry.terraform.io'
    for tf_data_dir in sorted(deployments_dir.rglob('.terraform.*')):
        providers_dir = tf_data_dir / 'providers'
        if providers_dir.is_dir() and not providers_dir.is_symlink():
            for entry in providers_dir.rglob('*'):
                if entry.is_symlink():
                    target = entry.resolve()
                    if target.is_relative_to(cache_registry):
                        parts = target.relative_to(cache_registry).parts
                        if len(parts) >= 3:
                            namespace, name, version = parts[:3]
                            provider = Provider(namespace=namespace,
                                                name=name,
                                                version=version)
                            version_dir = provider.path_in(cache_dir)
                            if version_dir not in touched and version_dir.is_dir():
                                if not dry_run:
                                    os.utime(version_dir)
                                touched.add(version_dir)
    return touched


def clean_cache(cache_dir: Path,
                marker: Path,
                *,
                dry_run: bool
                ) -> list[Removal]:
    marker_mtime = marker.stat().st_mtime
    removals = []
    for installation in find_installations(cache_dir):
        if installation.path.stat().st_mtime < marker_mtime:
            removals.append(installation.remove(None, dry_run=dry_run))
    if dry_run:
        log.info('Would remove marker %s', marker)
    else:
        marker.unlink()
        log.info('Removed marker %s', marker)
    return removals


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

    if args.clean_cache:
        assert cache_dir is not None, R(
            'No Terraform plugin cache is configured')
        assert cache_dir.is_dir(), R(
            'Plugin cache does not exist or is not a directory', cache_dir)
        marker = cache_dir / marker_name
        assert marker.exists(), R(
            'No marker file found, run without --clean-cache in every working copy first', marker)
        log.info('Cleaning plugin cache: %s', cache_dir)
        removals = clean_cache(cache_dir, marker, dry_run=args.dry_run)
    else:
        pinned = pinned_versions()
        assert pinned, R(
            'No pinned provider versions found in lock files')

        log.info('Pinned provider versions:')
        for provider in sorted(pinned):
            log.info('  %s', provider)

        log.info('Deployment data directories:')
        removals = clean_deployments(pinned, cache_dir, dry_run=args.dry_run)

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

    by_deferred = partition(lambda removal: removal.deferred, removals)
    removed, unlinked = map(list, by_deferred)
    if removed:
        verb = 'Would remove' if args.dry_run else 'Removed'
        size = sum(removal.size for removal in removed)
        log.info('%s %d provider version(s), freeing %.1f MB',
                 verb, len(removed), size / 1e6)
    if unlinked:
        verb = 'Would unlink' if args.dry_run else 'Unlinked'
        by_size = partition(lambda removal: removal.size > 0, unlinked)
        unaccounted, accounted = map(list, by_size)
        if accounted:
            # However many installations referenced it, the cache holds one copy
            sizes = {removal.provider: removal.size for removal in accounted}
            log.info('%s %d provider version(s), taking up %.1f MB of storage. '
                     'Reclaiming that storage requires running this script in '
                     'every working copy, and then once with --clean-cache.',
                     verb, len(accounted), sum(sizes.values()) / 1e6)
        if unaccounted:
            log.info('%s %d provider version(s) whose binaries are absent from '
                     'the plugin cache', verb, len(unaccounted))
    if not removals:
        log.info('Nothing to clean up.')


if __name__ == '__main__':
    main()
