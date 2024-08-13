"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum


class ClientType(Enum):
    """An enum class containing fileset data operations client type that supported."""

    PYTHON_GVFS = "python_gvfs"
    UNKNOWN = "unknown"
