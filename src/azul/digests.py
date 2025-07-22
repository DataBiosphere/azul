import base64
import pickle
from typing import (
    Literal,
)

import attrs
import resumablehash

from azul import (
    R,
)

type Hasher = resumablehash.md5 | resumablehash.sha256


def get_resumable_hasher(digest_type: str) -> Hasher:
    supported_types = ('sha256', 'md5')
    assert digest_type in supported_types, R(
        'Unsupported digest type', digest_type, supported_types)
    return getattr(resumablehash, digest_type)()


def hasher_to_str(hasher: Hasher) -> str:
    return base64.b64encode(pickle.dumps(hasher)).decode('ascii')


def hasher_from_str(s: str) -> Hasher:
    return pickle.loads(base64.b64decode(s))


@attrs.frozen(kw_only=True)
class Digest:
    """
    A hexadecimal digest of a sequence of bytes, and the type of algorithm used
    to produce said digest. The set of supported algorithms is limited to those
    we believe to present an acceptable risk of hash collisions.
    """
    type: Literal['sha256', 'sha1', 'md5']
    value: str
