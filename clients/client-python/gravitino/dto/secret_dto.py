"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field
from typing import Dict

from dataclasses_json import config

from gravitino.api.secret import Secret
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class SecretDTO(Secret):
    """Represents a Secret Data Transfer Object (DTO)."""

    _name: str = field(metadata=config(field_name="name"))
    _value: str = field(metadata=config(field_name="value"))
    _type: str = field(metadata=config(field_name="type"))
    _properties: Dict[str, str] = field(metadata=config(field_name="properties"))
    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))

    def builder(
        self,
        name: str,
        value: str,
        secret_type: str,
        properties: Dict[str, str] = None,
        audit: AuditDTO = None,
    ):
        self._name = name
        self._value = value
        self._type = secret_type
        self._properties = properties
        self._audit = audit

    def name(self) -> str:
        return self._name

    def value(self) -> str:
        return self._value

    def type(self) -> str:
        return self._type

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit
