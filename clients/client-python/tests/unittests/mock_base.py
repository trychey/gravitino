"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import base64
from typing import List

from gravitino import GravitinoMetalake, Catalog, Fileset
from gravitino.catalog.fileset_catalog import FilesetCatalog
from gravitino.dto.fileset_context_dto import FilesetContextDTO
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO

from unittest.mock import patch, Mock

from gravitino.dto.responses.fileset_context_response import FilesetContextResponse
from gravitino.dto.secret_dto import SecretDTO
from gravitino.utils import HTTPClient


def mock_load_metalake():
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    metalake_dto = MetalakeDTO(
        _name="metalake_demo",
        _comment="this is test",
        _properties={"k": "v"},
        _audit=audit_dto,
    )
    return GravitinoMetalake(metalake_dto)


def mock_load_fileset_catalog():
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    catalog = FilesetCatalog(
        name="fileset_catalog",
        catalog_type=Catalog.Type.FILESET,
        provider="hadoop",
        comment="this is test",
        properties={"k": "v"},
        audit=audit_dto,
        rest_client=HTTPClient("http://localhost:8090"),
    )
    return catalog


def mock_load_fileset(name: str, location: str):
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    fileset = FilesetDTO(
        _name=name,
        _type=Fileset.Type.MANAGED,
        _comment="this is test",
        _properties={"k": "v"},
        _storage_location=location,
        _audit=audit_dto,
    )
    return fileset


def mock_get_secret(expire_time: int = None):
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    if expire_time is None:
        secret = SecretDTO(
            _name="test",
            _value=base64.b64encode(b"test value").decode("utf-8"),
            _type="kerberos",
            _properties={"expireTime": "-1"},
            _audit=audit_dto,
        )
    else:
        secret = SecretDTO(
            _name="test",
            _value=base64.b64encode(b"test value").decode("utf-8"),
            _type="kerberos",
            _properties={"expireTime": str(expire_time)},
            _audit=audit_dto,
        )
    return secret


def mock_get_fileset_context(name: str, location: str, actual_paths: List[str]):
    fileset = mock_load_fileset(name, location)
    context = FilesetContextDTO(fileset, actual_paths)
    return context


def mock_get_fileset_context_response(
    name: str, location: str, actual_paths: List[str]
):
    audit_dto = AuditDTO(
        _creator="test",
        _create_time="2022-01-01T00:00:00Z",
        _last_modifier="test",
        _last_modified_time="2024-04-05T10:10:35.218Z",
    )
    fileset = FilesetDTO(
        _name=name,
        _type=Fileset.Type.MANAGED,
        _comment="this is test",
        _properties={"k": "v"},
        _storage_location=location,
        _audit=audit_dto,
    )
    context = FilesetContextDTO(_fileset=fileset, _actual_paths=actual_paths)
    context_resp = FilesetContextResponse(_code=200, _context=context)
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.body = context_resp.to_json().encode("utf-8")
    return mock_response


def mock_data(cls):
    @patch(
        "gravitino.client.gravitino_client_base.GravitinoClientBase.load_metalake",
        return_value=mock_load_metalake(),
    )
    @patch(
        "gravitino.client.gravitino_metalake.GravitinoMetalake.load_catalog",
        return_value=mock_load_fileset_catalog(),
    )
    @patch(
        "gravitino.client.gravitino_client_base.GravitinoClientBase.check_version",
        return_value=True,
    )
    @patch(
        "gravitino.client.gravitino_metalake.GravitinoMetalake.get_secret",
        return_value=mock_get_secret(),
    )
    class Wrapper(cls):
        pass

    return Wrapper
