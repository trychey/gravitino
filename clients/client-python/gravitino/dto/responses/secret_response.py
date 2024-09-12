"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.secret_dto import SecretDTO


@dataclass
class SecretResponse(BaseResponse):
    """Represents a response for a secret."""

    _secret: SecretDTO = field(metadata=config(field_name="secret"))

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if the secret name, type or audit is not set.
        """
        super().validate()

        assert self._secret is not None, "secret must not be null"
        assert (
            self._secret.name() is not None
        ), "secret 'name' must not be null and empty"
        assert self._secret.value() is not None, "secret 'value' must not be null"
        assert self._secret.type() is not None, "secret 'type' must not be null"
        assert self._secret.audit_info() is not None, "secret 'audit' must not be null"

    def secret(self) -> SecretDTO:
        return self._secret
