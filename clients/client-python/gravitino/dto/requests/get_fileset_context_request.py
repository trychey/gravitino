"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Dict
from dataclasses import dataclass, field
from dataclasses_json import config

from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.api.source_engine_type import SourceEngineType
from gravitino.rest.rest_message import RESTRequest


@dataclass
class GetFilesetContextRequest(RESTRequest):
    """Request to represent to get a fileset context."""

    _sub_path: str = field(metadata=config(field_name="subPath"))
    _operation: FilesetDataOperation = field(metadata=config(field_name="operation"))
    _client_type: ClientType = field(metadata=config(field_name="clientType"))
    _ip: str = field(metadata=config(field_name="ip"))
    _source_engine_type: SourceEngineType = field(
        metadata=config(field_name="sourceEngineType")
    )
    _app_id: str = field(metadata=config(field_name="appId"))
    _extra_info: Dict[str, str] = field(metadata=config(field_name="extraInfo"))

    def __init__(
        self,
        sub_path: str,
        operation: FilesetDataOperation,
        client_type: ClientType,
        source_engine_type: SourceEngineType,
        ip: str = None,
        app_id: str = None,
        extra_info: Dict[str, str] = None,
    ):
        self._sub_path = sub_path
        self._operation = operation
        self._client_type = client_type
        self._ip = ip
        self._source_engine_type = source_engine_type
        self._app_id = app_id
        self._extra_info = extra_info
        self.validate()

    def validate(self):
        assert self._sub_path is not None, "subPath is required"
        assert self._operation is not None, "operation is required"
        assert self._client_type is not None, "clientType is required"
        assert self._source_engine_type is not None, "sourceEngineType is required"
