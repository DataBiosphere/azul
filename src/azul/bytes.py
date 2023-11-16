import base64

_urlsafe_encode_translation = bytes.maketrans(b'+/', b'-_')
_urlsafe_decode_translation = bytes.maketrans(b'-_+/', b'+/\0\0')


def azul_urlsafe_b64encode(x: bytes) -> str:
    r"""
    Same as base64.urlsafe_b64encode but removes padding and returns a string.

    >>> azul_urlsafe_b64encode(b'')
    ''

    >>> azul_urlsafe_b64encode(b'\x00')
    'AA'
    >>> base64.urlsafe_b64encode(b'\x00')
    b'AA=='

    >>> azul_urlsafe_b64encode(b'\x00\x01')
    'AAE'
    >>> base64.urlsafe_b64encode(b'\x00\x01')
    b'AAE='

    """
    x = base64.b64encode(x).rstrip(b'=').translate(_urlsafe_encode_translation)
    return x.decode()


def azul_urlsafe_b64decode(s: str) -> bytes:
    r"""
    Same as base64.urlsafe_b64decode but also works with inputs from which
    padding was removed *and* rejects inputs with characters not part of the
    base64 alphabet. It's also stricter in rejecting the URL-unsafe alt
    characters plus and slash.

    >>> azul_urlsafe_b64decode('')
    b''

    >>> azul_urlsafe_b64decode('AQ')
    b'\x01'
    >>> azul_urlsafe_b64decode('AQ==')
    b'\x01'

    >>> azul_urlsafe_b64decode('AQI')
    b'\x01\x02'
    >>> azul_urlsafe_b64decode('AQI=')
    b'\x01\x02'

    >>> azul_urlsafe_b64decode('AQI==')
    Traceback (most recent call last):
    ...
    binascii.Error: Excess data after padding

    >>> azul_urlsafe_b64decode('-_')
    b'\xfb'

    An invalid characters is rejected.

    >>> azul_urlsafe_b64decode('AQ$')
    Traceback (most recent call last):
    ...
    binascii.Error: Only base64 data is allowed

    Same for the builtin, but for unintuitive reason.

    >>> base64.urlsafe_b64decode('AQ$')
    Traceback (most recent call last):
    ...
    binascii.Error: Incorrect padding

    The same happens with padding.

    >>> azul_urlsafe_b64decode('AQ$=')
    Traceback (most recent call last):
    ...
    binascii.Error: Only base64 data is allowed

    >>> base64.urlsafe_b64decode('AQ$=')
    Traceback (most recent call last):
    ...
    binascii.Error: Incorrect padding

    With just the right amount of padding, however, the builtin can be coaxed
    into ignoring the invalid character …

    >>> base64.urlsafe_b64decode('AQ$==')
    b'\x01'

    … whereas this function cannot.

    >>> azul_urlsafe_b64decode('AQ$==')
    Traceback (most recent call last):
    ...
    binascii.Error: Only base64 data is allowed

    Also, somewhat surprisingly, base64.urlsafe_b64decode allows plus and slash
    in addition to dash and underscore.

    >>> base64.urlsafe_b64decode('+/==')
    b'\xfb'

    This function doesn't.

    >>> azul_urlsafe_b64decode('+/')
    Traceback (most recent call last):
    ...
    binascii.Error: Only base64 data is allowed
    """
    # We could pass `altchars=` to b64decode() but that would invoke
    # bytes.maketrans() on every invocation. Using a static translation table is
    # slightly faster. That's the same approach base64.urlsafe_b64decode uses.
    s = s.translate(_urlsafe_decode_translation)
    s += '=='[:3 - ((len(s) + 3) % 4)]
    return base64.b64decode(s, validate=True)
