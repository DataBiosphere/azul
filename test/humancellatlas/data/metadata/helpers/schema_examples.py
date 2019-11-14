from io import BytesIO
import tarfile
import urllib.request
import uuid
import json

from dcplib.checksumming_io import ChecksummingSink


def download_example_bundle(repo, branch, path='/'):  # pragma: no cover (because of canning)
    manifest = []
    metadata_files = {}
    if path.startswith('/') or not path.endswith('/'):
        raise ValueError(path)
    with urllib.request.urlopen(f"https://github.com/{repo}/tarball/{branch}") as f:
        # We need to buffer the response because `tarfile` requires a seekable file object when the tarball is
        # compressed whereas the file object returned by `urlopen` is not seekable.
        buf = BytesIO(f.read())
        with tarfile.open(fileobj=buf) as tf:
            for member in tf.getmembers():
                # Member names always start with a synthetic root dir containing the repo name and commit hash
                member_path = member.name.partition('/')[2]
                if member_path.startswith(path):
                    member_path = member_path[len(path):]
                    if member_path.endswith('.json'):
                        with tf.extractfile(member) as tfm:
                            contents = tfm.read()
                        json_contents = json.loads(contents)
                        metadata_files[member_path] = json_contents
                        sink = ChecksummingSink(64 * 2 ** 20)
                        sink.write(contents)
                        md_file_type = member_path.partition('.')[2]
                        checksums = sink.get_checksums()
                        manifest.append({**checksums,
                                         'content-type': f'application/json; dcp-type="metadata/{md_file_type}"',
                                         'indexed': True,
                                         'name': member_path,
                                         'size': member.size,
                                         'uuid': str(uuid.uuid4()),
                                         'version': '1'})
                        schema_name, _, suffix = member_path[:-5].rpartition('_')
                        if schema_name.endswith('_file') or schema_name == 'file':
                            # Fake the manifest entry for the data file as best as we can. Reuse the metadata file's
                            # checksums for the data file.
                            manifest.append({**checksums,
                                             'content-type': 'application/octet-stream',
                                             'indexed': False,
                                             'name': json_contents['file_core']['file_name'],
                                             'size': member.size,
                                             'uuid': str(uuid.uuid4()),
                                             'version': '1'})
    return manifest, metadata_files
