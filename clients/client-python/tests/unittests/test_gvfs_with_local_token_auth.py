"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import datetime
import time
from unittest.mock import patch

from fsspec.implementations.local import LocalFileSystem
from gravitino.filesystem import gvfs

import mock_base
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.storage_type import StorageType

from tests.unittests.test_gvfs_with_local import TestLocalFilesystem


@mock_base.mock_data
class TestLocalWithTokenAuth(TestLocalFilesystem):
    extra_options = {
        GVFSConfig.AUTH_TYPE: GVFSConfig.TOKEN_AUTH_TYPE,
        GVFSConfig.TOKEN_VALUE: "token/test",
    }

    def setUp(self) -> None:
        super().setUp()
        self._options.update(self.extra_options)

    def tearDown(self) -> None:
        super().tearDown()
        self._options = {}

    def test_token_cache(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cache"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cache"
        actual_paths = [fileset_storage_location]
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_cache", f"{self._fileset_dir}/test_cache", actual_paths
            ),
        ) as mock_fileset:
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)
            self.assertTrue(local_fs.exists(fileset_storage_location))
            with patch(
                "gravitino.client.gravitino_metalake.GravitinoMetalake.get_secret",
                return_value=mock_base.mock_get_secret(
                    int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
                    + 1000
                ),
            ) as mock_secret:
                fs = gvfs.GravitinoVirtualFileSystem(
                    server_uri="http://localhost:9090",
                    metalake_name="metalake_demo",
                    options=self._options,
                    skip_instance_cache=True,
                )
                self.assertTrue(fs.exists(fileset_virtual_location))
                # wait 2 seconds
                time.sleep(2)
                self.assertIsNone(fs._filesystem_manager._cache.get(StorageType.LOCAL))
