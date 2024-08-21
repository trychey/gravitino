"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import base64
import os
import unittest

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.auth.token_auth_provider import TokenAuthProvider
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException


class TestSimpleAuthProvider(unittest.TestCase):

    def test_auth_provider(self):
        mock_token = "this is test"
        token_provider: TokenAuthProvider = TokenAuthProvider(mock_token)
        self.assertTrue(token_provider.has_token_data())
        decode_token = token_provider.get_token_data().decode("utf-8")[
            len(f"{AuthConstants.AUTHORIZATION_TOKEN_HEADER}") :
        ]
        self.assertEqual(mock_token, base64.b64decode(decode_token).decode("utf-8"))

    def test_null(self):
        with self.assertRaises(GravitinoRuntimeException):
            token = ""
            token_provider: TokenAuthProvider = TokenAuthProvider(token)

        with self.assertRaises(TypeError):
            token_provider: TokenAuthProvider = TokenAuthProvider()
