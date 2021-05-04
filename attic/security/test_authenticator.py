from time import (
    time,
)
from typing import (
    Any,
    Dict,
)
from unittest import (
    TestCase,
)
from unittest.mock import (
    patch,
)
from urllib.parse import (
    parse_qs,
    urlparse,
)

from jwt import (
    encode,
)
import responses

from azul import (
    config,
)
from azul.security.authenticator import (
    AuthenticationError,
    Authenticator,
    InvalidRedirectUriError,
    InvalidTokenError,
    NonDecodableTokenError,
)
from retorts import (
    AuthResponseHelper,
    TestKeyManager,
)


class AuthenticatorTestCase(TestCase):

    @classmethod
    def tearDownClass(cls):
        super().setUpClass()
        TestKeyManager.remove_test_keys()

    def test_get_fusillade_login_url_with_no_redirect_uri(self):
        url = Authenticator.get_fusillade_login_url()
        parsed_url = urlparse(url)
        self.assertEqual('/authorize', parsed_url.path)
        parsed_query = parse_qs(parsed_url.query)
        self.assertEqual('code', parsed_query['response_type'][0])
        scopes = str(parsed_query['scope'][0]).split(' ')
        self.assertIn('openid', scopes)
        self.assertIn('email', scopes)

    def test_get_fusillade_login_url_with_localhost_as_redirect_uri(self):
        expected_redirect_uri = 'http://localhost:12345/abc6789'
        url = Authenticator.get_fusillade_login_url(expected_redirect_uri)
        parsed_url = urlparse(url)
        self.assertEqual('/authorize', parsed_url.path)
        parsed_query = parse_qs(parsed_url.query)
        self.assertEqual('code', parsed_query['response_type'][0])
        scopes = str(parsed_query['scope'][0]).split(' ')
        self.assertIn('openid', scopes)
        self.assertIn('email', scopes)
        self.assertEqual(expected_redirect_uri, parsed_query['redirect_uri'][0])

    def test_get_fusillade_login_url_with_hca_as_redirect_uri(self):
        expected_redirect_uri = 'https://data.humancellatlas.org/def123'
        url = Authenticator.get_fusillade_login_url(expected_redirect_uri)
        parsed_url = urlparse(url)
        self.assertEqual('/authorize', parsed_url.path)
        parsed_query = parse_qs(parsed_url.query)
        self.assertEqual('code', parsed_query['response_type'][0])
        scopes = str(parsed_query['scope'][0]).split(' ')
        self.assertIn('openid', scopes)
        self.assertIn('email', scopes)
        self.assertEqual(expected_redirect_uri, parsed_query['redirect_uri'][0])

    def test_get_fusillade_login_url_with_invalid_redirect_uri(self):
        expected_redirect_uri = 'https://example/def123'
        with self.assertRaises(InvalidRedirectUriError):
            Authenticator.get_fusillade_login_url(expected_redirect_uri)

    def test_authorize_ok(self):
        fake_claim = {'abc': 'def'}
        fake_jwt = self.generate_no_signature_token(fake_claim)
        fake_bearer_token = f'Bearer {fake_jwt}'
        with patch.object(Authenticator, 'verify_jwt', return_value=fake_claim):
            authenticator = Authenticator()
            decoded_info = authenticator.authenticate_bearer_token(fake_bearer_token)
        self.assertEqual(fake_claim, decoded_info)

    def test_authorize_failed_due_to_token_without_bearer_prefix(self):
        authenticator = Authenticator()
        with self.assertRaisesRegex(AuthenticationError, 'not_bearer_token'):
            authenticator.authenticate_bearer_token('asdf')

    def test_authorize_failed_due_to_token_with_unknown_bearer_prefix(self):
        fake_bearer_token = 'x asfasdfasdf'
        authenticator = Authenticator()
        with self.assertRaisesRegex(AuthenticationError, 'not_bearer_token'):
            authenticator.authenticate_bearer_token(fake_bearer_token)

    def test_authorize_failed_due_to_empty_token(self):
        fake_bearer_token = 'Bearer '
        authenticator = Authenticator()
        with self.assertRaisesRegex(AuthenticationError, 'missing_bearer_token'):
            authenticator.authenticate_bearer_token(fake_bearer_token)

    def test_authorize_failed_due_to_invalid_token(self):
        fake_bearer_token = 'Bearer asdfasdfasdf'
        authenticator = Authenticator()
        with self.assertRaisesRegex(AuthenticationError, 'non_decodable_token'):
            authenticator.authenticate_bearer_token(fake_bearer_token)

    def test_get_access_token_ok(self):
        fake_claim = {'abc': 'def'}
        fake_jwt = self.generate_no_signature_token(fake_claim)
        fake_bearer_token = f'Bearer {fake_jwt}'
        with patch.object(Authenticator, 'verify_jwt', return_value=fake_claim):
            authenticator = Authenticator()
            decoded_info = authenticator.get_access_token({'authorization': fake_bearer_token})
        self.assertEqual(fake_claim, decoded_info)

    def test_get_access_token_failed_due_to_missing_authorization_header(self):
        authenticator = Authenticator()

        with self.assertRaisesRegex(AuthenticationError, 'missing_authorization_header'):
            authenticator.get_access_token({})

    def test_get_jwks_uri_with_google_as_provider(self):
        openid_provider = 'https://hca.iam.gserviceaccount.com'
        authenticator = Authenticator()
        result = authenticator.get_jwks_uri(openid_provider)
        self.assertEqual(f'https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}', result)

    def test_get_jwks_uri_with_auth0_as_provider(self):
        openid_provider = 'https://humancellatlas.auth0.com/'
        authenticator = Authenticator()
        result = authenticator.get_jwks_uri(openid_provider)
        self.assertEqual(f'{openid_provider}.well-known/jwks.json', result)

    @responses.activate
    def test_get_public_keys(self):
        public_key = TestKeyManager.get_public_key()
        exponent = public_key.public_numbers().e
        modulus = public_key.public_numbers().n
        with AuthResponseHelper():
            authenticator = Authenticator()
            key_map = authenticator.get_public_keys(config.access_token_issuer)
        self.assertIn('local_test', key_map)
        test_key = key_map['local_test']
        self.assertEqual(exponent, test_key.public_numbers().e)
        self.assertEqual(modulus, test_key.public_numbers().n)

    @responses.activate
    def test_verify_ok(self):
        partial_test_claims = dict(email='foo@bar.org',
                                   issued_at=time())
        test_jwt = AuthResponseHelper.generate_test_jwt(**partial_test_claims)
        test_claims = AuthResponseHelper.generate_test_claims(**partial_test_claims)
        with AuthResponseHelper():
            authenticator = Authenticator()
            result = authenticator.verify_jwt(test_jwt)
        self.assertEqual(test_claims, result)

    @responses.activate
    def test_verify_raises_error_while_decoding_with_verification(self):
        partial_test_claims = dict(email='foo@bar.org',
                                   issued_at=time() - 120)  # this is to force the token to expire.
        test_jwt = AuthResponseHelper.generate_test_jwt(**partial_test_claims)
        with self.assertRaises(InvalidTokenError):
            with AuthResponseHelper():
                authenticator = Authenticator()
                authenticator.verify_jwt(test_jwt)

    def test_verify_raises_non_decodable_token(self):
        with self.assertRaises(NonDecodableTokenError):
            authenticator = Authenticator()
            authenticator.verify_jwt('something')

    def test_verify_with_invalid_issuer_raises_error(self):
        test_claims = self.get_test_claims()
        test_claims['iss'] = 'rogue_issuer'
        test_jwt = self.generate_no_signature_token(test_claims)

        with self.assertRaises(InvalidTokenError):
            authenticator = Authenticator()
            authenticator.verify_jwt(test_jwt)

    def test_verify_without_public_key_id_raises_error(self):
        test_claims = self.get_test_claims()
        test_jwt = self.generate_no_signature_token(test_claims)

        with self.assertRaises(InvalidTokenError):
            authenticator = Authenticator()
            authenticator.verify_jwt(test_jwt)

    @staticmethod
    def generate_no_signature_token(claim: Dict[str, Any], headers: Dict[str, Any] = None):
        return encode(claim, None, None, headers=headers).decode('utf-8')

    @staticmethod
    def get_test_claims():
        return {
            "azp": "fake-authorizer",
            "exp": 1234567900,
            "https://auth.data.humancellatlas.org/email": "robot@hca.org",
            "https://auth.data.humancellatlas.org/group": "foo",
            "iat": 1234567890,
            "iss": config.access_token_issuer,
            "scope": "openid email",
            "sub": "foo|1234567890"
        }
