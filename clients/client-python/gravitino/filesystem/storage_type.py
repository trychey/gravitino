"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum


class StorageType(Enum):
    HDFS = "hdfs"
    LAVAFS = "lavafs"
    JUICEFS = "jfs"
    LOCAL = "file"
