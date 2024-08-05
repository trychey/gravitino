"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum


class FilesetDataOperation(Enum):
    """An enum class containing fileset data operations that supported."""

    LIST_STATUS = 1
    GET_FILE_STATUS = 2
    EXISTS = 3
    RENAME = 4
    APPEND = 5
    CREATE = 6
    DELETE = 7
    OPEN = 8
    MKDIRS = 9
    CREATED_TIME = 10
    MODIFIED_TIME = 11
    COPY_FILE = 12
    CAT_FILE = 13
    GET_FILE = 14
    UNKNOWN = 15
