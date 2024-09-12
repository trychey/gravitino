"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod

from fsspec import AbstractFileSystem


class FileSystemContext(ABC):
    """Context for FileSystem."""

    @abstractmethod
    def get_filesystem(self) -> AbstractFileSystem:
        pass

    @abstractmethod
    def get_secret(self) -> str:
        pass
