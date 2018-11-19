from unittest import TestCase
from unittest.mock import patch
from urllib.parse import urlparse, parse_qs

from azul.security.auth import (get_fusillade_login_url,
                                authenticate,
                                AuthenticationError,
                                get_access_token)
from azul.security.verifier import NonDecodableTokenError, InvalidTokenError

from .helper import generate_no_signature_token


class AuthTestCase(TestCase):
    def test_get_fusillade_login_url(self):
        url = get_fusillade_login_url()
        parsed_url = urlparse(url)
        self.assertEqual('/authorize', parsed_url.path)
        parsed_query = parse_qs(parsed_url.query)
        self.assertEqual('code', parsed_query['response_type'][0])
        scopes = str(parsed_query['scope'][0]).split(' ')
        self.assertIn('openid', scopes)
        self.assertIn('email', scopes)

    @patch('azul.security.auth.verify')
    def test_authorize_ok(self, verify_mock):
        fake_claim = {'abc': 'def'}
        fake_jwt = generate_no_signature_token(fake_claim)
        fake_bearer_token = f'Bearer {fake_jwt}'

        verify_mock.return_value = fake_claim

        decoded_info = authenticate(fake_bearer_token)

        self.assertEqual(fake_claim, decoded_info)

    def test_authorize_failed_due_to_token_without_bearer_prefix(self):
        with self.assertRaisesRegex(AuthenticationError, 'not_bearer_token'):
            authenticate('asdf')

    def test_authorize_failed_due_to_token_with_unknown_bearer_prefix(self):
        fake_bearer_token = f'x asfasdfasdf'

        with self.assertRaisesRegex(AuthenticationError, 'not_bearer_token'):
            authenticate(fake_bearer_token)

    def test_authorize_failed_due_to_empty_token(self):
        fake_bearer_token = f'Bearer '

        with self.assertRaisesRegex(AuthenticationError, 'missing_bearer_token'):
            authenticate(fake_bearer_token)

    @patch('azul.security.auth.verify')
    def test_authorize_failed_due_to_non_decodable_token(self, verify_mock):
        fake_bearer_token = f'Bearer asdfasdfasdf'

        verify_mock.side_effect = NonDecodableTokenError()

        with self.assertRaisesRegex(AuthenticationError, 'non_decodable_token'):
            authenticate(fake_bearer_token)

    @patch('azul.security.auth.verify')
    def test_authorize_failed_due_to_invalid_token(self, verify_mock):
        fake_bearer_token = f'Bearer asdfasdfasdf'

        verify_mock.side_effect = InvalidTokenError()

        with self.assertRaisesRegex(AuthenticationError, 'invalid_token'):
            authenticate(fake_bearer_token)

    @patch('azul.security.auth.verify')
    def test_get_access_token_ok(self, verify_mock):
        fake_claim = {'abc': 'def'}
        fake_jwt = generate_no_signature_token(fake_claim)
        fake_bearer_token = f'Bearer {fake_jwt}'

        verify_mock.return_value = fake_claim

        decoded_info = get_access_token({'authorization': fake_bearer_token})

        self.assertEqual(fake_claim, decoded_info)

    def test_get_access_token_failed_due_to_missing_authorization_header(self):
        with self.assertRaisesRegex(AuthenticationError, 'missing_authorization_header'):
            get_access_token({})
