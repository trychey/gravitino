"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum


class SourceEngineType(Enum):
    """An enum class containing fileset data operations source engine type that supported."""

    PYSPARK = 1
    UNKNOWN = 2
