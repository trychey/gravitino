"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from typing import Dict, Optional

from gravitino.api.auditable import Auditable


class Secret(Auditable):
    """The interface of a secret. The secret is the entity which contains any credentials."""

    @abstractmethod
    def name(self) -> str:
        """
        Returns:
             The name of the secret.
        """
        pass

    @abstractmethod
    def value(self) -> str:
        """
        Returns:
             The secret value. The value is an encrypted string, which can be used to store any kind of secret
        """
        pass

    @abstractmethod
    def type(self) -> str:
        """
        Returns:
             The type of the secret.
        """
        pass

    @abstractmethod
    def properties(self) -> Optional[Dict[str, str]]:
        """
        Returns:
             The properties of the secret.
        """
        pass
