"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import base64

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException


class TokenAuthProvider(AuthDataProvider):
    """TokenAuthProvider will use the token for every request."""

    _token: bytes

    def __init__(self, token: str):
        if token is None or len(token) == 0:
            raise GravitinoRuntimeException("token is null")
        self._token = (
            AuthConstants.AUTHORIZATION_TOKEN_HEADER
            + base64.b64encode(token.encode("utf-8")).decode("utf-8")
        ).encode("utf-8")

    def has_token_data(self) -> bool:
        return True

    def get_token_data(self) -> bytes:
        return self._token

    def close(self):
        pass
