import base64
from collections import (
    defaultdict,
)
import logging
import re
import secrets
from typing import (
    Self,
)

import attrs
from more_itertools import (
    only,
    prepend,
)

from azul import (
    R,
)
from azul.strings import (
    single_quote as sq,
)

log = logging.getLogger(__name__)


@attrs.frozen
class CSP:
    directives: dict[str, list[str]]

    @classmethod
    def for_azul(cls, nonce: str | None = None) -> Self:
        self, none, data = sq('self'), sq('none'), 'data:'
        nonce = [] if nonce is None else [sq('nonce-' + nonce)]
        return cls({
            'default-src': [self],
            'img-src': [self, data],
            'script-src': [self, *nonce],
            'style-src': [self, *nonce],
            'frame-ancestors': [none]
        })

    @classmethod
    def new_nonce(cls) -> str:
        """
        A random nonce for use in a CSP.
        """
        return base64.b64encode(secrets.token_bytes(32)).decode('ascii').rstrip('=')

    @classmethod
    def parse(cls, csp: str) -> Self:
        """(

        Parse the given CSP or raise RequirementError if it is not syntactically
        valid against the specification at https://www.w3.org/TR/CSP2.

        >>> from azul.doctests import (
        ...     assert_json,
        ... )

        >>> def parse(s): return CSP.parse(s).directives

        A valid CSP:

        >>> valid_csp = "img-src 'self' data:;frame-ancestors 'none'"
        >>> assert_json(parse(valid_csp))
        {
            "img-src": [
                "'self'",
                "data:"
            ],
            "frame-ancestors": [
                "'none'"
            ]
        }

        Insignificant whitespace is removed:

        >>> fluffy_csp = " \timg-src\t'self'  data:\t;\tframe-ancestors\t 'none' \t"
        >>> parse(valid_csp) == parse(fluffy_csp)
        True

        Multiple multiple directives of the same name are consolidated:

        >>> assert_json(parse("img-src data:;img-src 'self':"))
        {
            "img-src": [
                "data:",
                "'self':"
            ]
        }

        Invalid CSPs:

        >>> parse(";")
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid directive', '')

        >>> parse('img_src;')
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid directive', 'img_src')

        >>> parse('img-src a,b')
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid directive', 'img-src a,b')
        """
        # https://www.w3.org/TR/CSP2/#policy-syntax
        directive_re = re.compile(r'[ \t]*([a-zA-Z0-9-]+)'
                                  # Space, tab and any visible character
                                  # (0x21-0xFE) except for comma (0x2C) or
                                  # semicolon (0x3B).
                                  r'(?:[ \t]([ \t\x21-\x2B\x2D-\x3A\x3C-\xFE]*))?')
        wsp_re = re.compile(r'[ \t]+')
        directives: dict[str, list[str]] = defaultdict(list)
        for directive in csp.split(';'):
            match = directive_re.fullmatch(directive)
            assert match is not None, R('Invalid directive', directive)
            name, values = match.groups()
            values = [] if values is None else filter(None, wsp_re.split(values))
            directives[name].extend(values)
        return cls(directives)

    # Matches only Azul nonces, specifically
    nonce_re = re.compile(sq(r'nonce-([a-zA-Z0-9+/]{43})'))

    def validate(self):
        """
        Validate the directive values against a subset of the Source List
        grammar from the specification. Of that grammar, only the productions
        used in CSPs for Azul are supported.

        >>> def validate(s): return CSP.parse(s).validate()

        >>> valid = ('0a+/' * 11)[:43]
        >>> validate(f"script-src 'self' 'nonce-{valid}'")

        Disallowed characters in nonce:

        >>> invalid = valid.replace('+','*')
        >>> validate(f"script-src 'self' 'nonce-{invalid}'")
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid value', "'nonce-0a*/0a*/0a*/0a*/0a*/0a*/0a*/0a*/0a*/0a*/0a*'")

        Nonce is too short:

        >>> invalid = valid[:-1]
        >>> validate(f"script-src 'self' 'nonce-{invalid}'")
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid value', "'nonce-0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a'")

        Nonce is too long:

        >>> invalid = valid + '/'
        >>> validate(f"script-src 'self' 'nonce-{invalid}'")
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid value', "'nonce-0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/0a+/'")

        Other invalid combinations:

        >>> validate("frame-ancestors 'none' 'none'")
        Traceback (most recent call last):
        ...
        AssertionError: R("'none' must appear alone", ["'none'", "'none'"])

        >>> validate("frame-ancestors 'self' 'none'")
        Traceback (most recent call last):
        ...
        AssertionError: R("'none' must appear alone", ["'self'", "'none'"])

        >>> validate("img-src 'self' data: 'self'")
        Traceback (most recent call last):
        ...
        AssertionError: R('Duplicated value', ["'self'", 'data:', "'self'"])
        """
        self_, none, data = sq('self'), sq('none'), 'data:'
        value_res = prepend(self.nonce_re.pattern, map(re.escape, [self_, none, data]))
        value_re = re.compile('|'.join(value_res))
        for name, values in self.directives.items():
            for value in values:
                match = value_re.fullmatch(value)
                assert match is not None, R('Invalid value', value)
            assert values == [none] or none not in values, R(
                f'{none} must appear alone', values)
            assert len(values) == len(set(values)), R('Duplicated value', values)

    def nonce(self) -> str | None:
        """
        Extract the Azul nonce from this CSP, if present. If there are multiple
        occurrances of a nonce, they must all be equal.
        """
        return only(set(
            value
            for name, values in self.directives.items()
            for value in values
            if self.nonce_re.fullmatch(value) is not None
        ))

    def __str__(self) -> str:
        """
        >>> s = "img-src 'self' data:;frame-ancestors 'none'"
        >>> s == str(CSP.parse(s))
        True
        """
        return ';'.join(
            ' '.join(value for value in prepend(name, values))
            for name, values in self.directives.items()
        )
