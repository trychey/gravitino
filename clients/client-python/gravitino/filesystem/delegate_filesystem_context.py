"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import base64
import logging
import uuid
from pathlib import Path

from fsspec import AbstractFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.implementations.local import LocalFileSystem
from pyarrow.fs import HadoopFileSystem

from gravitino.api.catalog import UnsupportedOperationException
from gravitino.api.secret import Secret
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException
from gravitino.filesystem.filesystem_context import FileSystemContext
from gravitino.filesystem.hadoop_environment import HadoopEnvironment

logger = logging.getLogger(__name__)

_DEFAULT_CRED_PATH = "/tmp/gravitino/cred"


def init_temp_dir():
    tmp_dir = Path(_DEFAULT_CRED_PATH)
    if not tmp_dir.exists():
        try:
            tmp_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise GravitinoRuntimeException(
                f"Failed to create temp dir: {_DEFAULT_CRED_PATH}, msg: {e}"
            ) from e


# init the temp cred dir when import the module
init_temp_dir()


class DelegateFileSystemContext(FileSystemContext):
    _filesystem: AbstractFileSystem
    _secret: Secret
    _local_credential_path: str
    _hadoop_env: HadoopEnvironment

    def __init__(self, secret: Secret, uri: str, config: str):
        if secret.type() != "kerberos":
            raise UnsupportedOperationException(
                f"Unsupported secret type: {secret.type()}"
            )
        self._secret = secret
        self._local_credential_path = (
            f"{_DEFAULT_CRED_PATH}/{self._secret.name()}_{uuid.uuid4().hex}"
        )

        self._delete_temp_cred_file()

        # save the ticket
        try:
            with open(self._local_credential_path, "wb") as out:
                out.write(base64.b64decode(secret.value()))
        except IOError as e:
            raise GravitinoRuntimeException(
                f"Failed to create local credential file: {self._local_credential_path}, msg: {e}"
            ) from e

        # init the fs with the local credential file
        # TODO need support juicefs when we can get the ak/sk
        if uri.startswith("hdfs://") or uri.startswith("lavafs://"):
            # init the hadoop env through python module import
            self._hadoop_env = HadoopEnvironment()
            concat_uri = f"{uri}?kerb_ticket={self._local_credential_path}"
            if config is not None and len(config) > 0:
                concat_uri = concat_uri + "&" + config
            self._filesystem = ArrowFSWrapper(HadoopFileSystem.from_uri(concat_uri))
        elif uri.startswith("file:/"):
            self._filesystem = LocalFileSystem()
        else:
            raise GravitinoRuntimeException(
                f"Storage type doesn't support now. Path:{uri}"
            )

        self._delete_temp_cred_file()

    def _delete_temp_cred_file(self):
        tmp_file = Path(self._local_credential_path)
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except Exception as e:
            logger.warning(
                "Failed to delete temp credential file: %s, msg: %s", tmp_file, e
            )

    def get_filesystem(self) -> AbstractFileSystem:
        return self._filesystem

    def get_secret(self) -> Secret:
        return self._secret
