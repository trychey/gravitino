"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC
from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.fileset_context import FilesetContext
from gravitino.dto.fileset_dto import FilesetDTO


@dataclass
class FilesetContextDTO(FilesetContext, DataClassJsonMixin, ABC):
    """Represents a Fileset Context DTO (Data Transfer Object)."""

    _fileset: FilesetDTO = field(metadata=config(field_name="fileset"))
    _actual_paths: List[str] = field(metadata=config(field_name="actualPaths"))

    def fileset(self) -> FilesetDTO:
        return self._fileset

    def actual_paths(self) -> List[str]:
        return self._actual_paths
