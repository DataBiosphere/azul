from base64 import b64encode
from typing import Dict, Any
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt import encode


def generate_no_signature_token(claim:Dict[str, Any], headers:Dict[str, Any] = None):
    return encode(claim,
                  None,
                  None,
                  headers=headers).decode()


def get_test_claims(issuer:str=None):
    # As it is difficult to generate a proper JWT to pass the verification, the
    # jwt.decode method must be patched with a Mock object. Therefore, the
    # iat and exp claims are to be constant.

    return {
        "azp": "fake-authorizer",
        "exp": 1234567900,
        "https://auth.data.humancellatlas.org/email": "robot@hca.org",
        "https://auth.data.humancellatlas.org/group": "foo",
        "iat": 1234567890,
        "iss": issuer or "https://humancellatlas.auth0.com/",
        "scope": "openid email",
        "sub": "foo|1234567890"
    }


def generate_fake_key_response(kid, e, n):
    return {
        'kid': kid,
        'e': b64encode(int(e).to_bytes(3, 'little')).decode('utf-8'),
        'n': b64encode(int(n).to_bytes(3, 'little')).decode('utf-8')
    }
