"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.fileset_context_dto import FilesetContextDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class FilesetContextResponse(BaseResponse):
    """Response for fileset context."""

    _context: FilesetContextDTO = field(metadata=config(field_name="filesetContext"))

    def context(self) -> FilesetContextDTO:
        return self._context

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()
        assert self._context is not None, "context must not be null"
