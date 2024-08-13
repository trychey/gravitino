"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum


class FilesetDataOperation(Enum):
    """An enum class containing fileset data operations that supported."""

    CREATE = "create"
    OPEN = "open"
    APPEND = "append"
    RENAME = "rename"
    DELETE = "delete"
    GET_FILE_STATUS = "get_file_status"
    LIST_STATUS = "list_status"
    MKDIRS = "mkdirs"
    EXISTS = "exists"
    CREATED_TIME = "created_time"
    MODIFIED_TIME = "modified_time"
    COPY_FILE = "copy_file"
    CAT_FILE = "cat_file"
    GET_FILE = "get_file"
    UNKNOWN = "unknown"
