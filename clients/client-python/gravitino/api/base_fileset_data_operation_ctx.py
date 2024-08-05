"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Dict

from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.api.fileset_data_operation_ctx import FilesetDataOperationCtx
from gravitino.api.source_engine_type import SourceEngineType


class BaseFilesetDataOperationCtx(FilesetDataOperationCtx):
    """Base implementation of FilesetDataOperationCtx."""

    _sub_path: str
    _operation: FilesetDataOperation
    _client_type: ClientType
    _ip: str
    _source_engine_type: SourceEngineType
    _app_id: str
    _extra_info: Dict[str, str]

    def __init__(
        self,
        sub_path: str,
        operation: FilesetDataOperation,
        client_type: ClientType,
        ip: str,
        source_engine_type: SourceEngineType,
        app_id: str,
        extra_info: Dict[str, str] = None,
    ):
        assert sub_path is not None
        assert operation is not None
        assert client_type is not None
        assert source_engine_type is not None
        self._sub_path = sub_path
        self._operation = operation
        self._client_type = client_type
        self._ip = ip
        self._source_engine_type = source_engine_type
        self._app_id = app_id
        self._extra_info = extra_info

    def sub_path(self) -> str:
        return self._sub_path

    def operation(self) -> FilesetDataOperation:
        return self._operation

    def client_type(self) -> ClientType:
        return self._client_type

    def ip(self) -> str:
        return self._ip

    def source_engine_type(self) -> SourceEngineType:
        return self._source_engine_type

    def app_id(self) -> str:
        return self._app_id

    def extra_info(self) -> Dict[str, str]:
        return self._extra_info
