from unittest import TestCase
from unittest.mock import Mock, patch
from cryptography.hazmat.backends import default_backend
from jwt.exceptions import DecodeError, PyJWTError
from azul.security.verifier import (get_jwks_uri,
                                    gserviceaccount_domain,
                                    get_public_keys,
                                    verify,
                                    NonDecodableTokenError,
                                    InvalidTokenError)
from .helper import generate_fake_key_response, get_test_claims, generate_no_signature_token


class VerifierTest(TestCase):
    def test_get_jwks_uri_with_google_as_provider(self):
        openid_provider = f'https://hca.{gserviceaccount_domain}'
        result = get_jwks_uri(openid_provider)
        self.assertEqual(f'https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}', result)

    def test_get_jwks_uri_with_auth0_as_provider(self):
        openid_provider = 'https://humancellatlas.auth0.com/'
        result = get_jwks_uri(openid_provider)
        self.assertEqual(f'{openid_provider}.well-known/jwks.json', result)

    @patch('azul.security.verifier.session')
    def test_get_public_keys(self, session):
        openid_provider = 'https://humancellatlas.auth0.com/'

        test_public_exponent = 65537
        test_public_modulus = 31

        response_mock = Mock()
        response_mock.json.side_effect = [
            # This is for get_openid_config.
            # NOTE: This represents a subset of the actual response.
            {
                'jwks_uri': 'https://fake_jwks_uri/'
            },
            # This is for session.get.
            # NOTE: This represents a subset of the actual response.
            {
                'keys': [
                    generate_fake_key_response('test', e=test_public_exponent, n=test_public_modulus)
                ]
            }
        ]
        session.get.return_value = response_mock

        key_map = get_public_keys(openid_provider)

        self.assertIn('test', key_map)

        test_key = key_map['test']
        self.assertEqual(test_public_exponent, test_key.public_numbers().e)
        self.assertEqual(test_public_modulus * (test_public_exponent - 1), test_key.public_numbers().n)
        self.assertEqual(21, test_key.key_size)

    @patch('azul.security.verifier.session')
    def test_verify_ok(self, session):
        openid_provider = 'https://humancellatlas.auth0.com/'

        test_public_exponent = 65537
        test_public_modulus = 31

        response_mock = Mock()
        response_mock.json.side_effect = [
            # This is for get_openid_config.
            # NOTE: This represents a subset of the actual response.
            {
                'jwks_uri': 'https://fake_jwks_uri/'
            },
            # This is for session.get.
            # NOTE: This represents a subset of the actual response.
            {
                'keys': [
                    generate_fake_key_response('test', e=test_public_exponent, n=test_public_modulus)
                ]
            }
        ]

        session.get.return_value = response_mock

        test_claims = get_test_claims()
        test_jwt = generate_no_signature_token(test_claims, {'kid': 'test'})

        with patch('jwt.decode') as decode_mock:
            decode_mock.return_value = test_claims  # Assume that the signature is valid.
            result = verify(test_jwt)

        self.assertEqual(test_claims, result)

    @patch('azul.security.verifier.session')
    def test_verify_raises_error_while_decoding_with_verification(self, session):
        openid_provider = 'https://humancellatlas.auth0.com/'

        test_public_exponent = 65537
        test_public_modulus = 31

        response_mock = Mock()
        response_mock.json.side_effect = [
            # This is for get_openid_config.
            # NOTE: This represents a subset of the actual response.
            {
                'jwks_uri': 'https://fake_jwks_uri/'
            },
            # This is for session.get.
            # NOTE: This represents a subset of the actual response.
            {
                'keys': [
                    generate_fake_key_response('test', e=test_public_exponent, n=test_public_modulus)
                ]
            }
        ]

        session.get.return_value = response_mock

        test_claims = get_test_claims()
        test_jwt = generate_no_signature_token(test_claims, {'kid': 'test'})

        with self.assertRaises(InvalidTokenError):
            with patch('jwt.decode') as decode_mock:
                decode_mock.side_effect = [test_claims, PyJWTError('Test')]
                result = verify(test_jwt)

    def test_verify_raises_non_decodable_token(self):
        with self.assertRaises(NonDecodableTokenError):
            with patch('jwt.decode') as decode_mock:
                decode_mock.side_effect = DecodeError('Test')
                verify('something')

    def test_verify_with_invalid_issuer_raises_error(self):
        test_claims = get_test_claims()
        test_claims['iss'] = 'rogue_issuer'
        test_jwt = generate_no_signature_token(test_claims)

        with self.assertRaises(InvalidTokenError):
            verify(test_jwt)

    def test_verify_without_public_key_id_raises_error(self):
        test_claims = get_test_claims()
        test_jwt = generate_no_signature_token(test_claims)

        with self.assertRaises(InvalidTokenError):
            verify(test_jwt)
