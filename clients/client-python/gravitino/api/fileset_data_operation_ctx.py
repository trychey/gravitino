"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod

from typing import Dict

from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.api.source_engine_type import SourceEngineType


class FilesetDataOperationCtx(ABC):
    """An interface representing a fileset data operation context. This interface defines some
    information need to report to the server.
    """

    @abstractmethod
    def sub_path(self) -> str:
        """The sub path which is operated by the data operation.

        Returns:
            the sub path which is operated by the data operation.
        """
        pass

    @abstractmethod
    def operation(self) -> FilesetDataOperation:
        """The data operation type.

        Returns:
            the data operation type.
        """
        pass

    @abstractmethod
    def client_type(self) -> ClientType:
        """The client type of the data operation.

        Returns:
            the client type of the data operation.
        """
        pass

    @abstractmethod
    def ip(self) -> str:
        """The client ip of the data operation.

        Returns:
            the client ip of the data operation.
        """
        pass

    @abstractmethod
    def source_engine_type(self) -> SourceEngineType:
        """The source type of the data operation.

        Returns:
            the source type of the data operation.
        """
        pass

    @abstractmethod
    def app_id(self) -> str:
        """The application id of the data operation.

        Returns:
            the application id of the data operation.
        """
        pass

    @abstractmethod
    def extra_info(self) -> Dict[str, str]:
        """The extra info of the data operation.

        Returns:
            the extra info of the data operation.
        """
        pass
