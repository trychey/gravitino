"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import random
import string
import unittest
from unittest.mock import patch

import mock_base
from gravitino import GravitinoClient, NameIdentifier
from gravitino.api.base_fileset_data_operation_ctx import BaseFilesetDataOperationCtx
from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.api.source_engine_type import SourceEngineType


def generate_unique_random_string(length):
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.sample(characters, length))
    return random_string


@mock_base.mock_data
class TestFilesetCatalog(unittest.TestCase):
    _local_base_dir_path: str = "file:/tmp/fileset"
    _fileset_dir: str = (
        f"{_local_base_dir_path}/{generate_unique_random_string(10)}/fileset_catalog/tmp"
    )

    def test_get_fileset_context(self, *mock_methods):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_base.mock_get_fileset_context_response(
                "test_get_context",
                f"{self._fileset_dir}/test_get_context",
                [f"{self._fileset_dir}/test_get_context/test1"],
            ),
        ) as mock_normally:
            client = GravitinoClient(
                uri="http://localhost:8090", metalake_name="test_metalake"
            )
            catalog = client.load_catalog(
                NameIdentifier.of_catalog("test_metalake", "test_catalog")
            )
            ctx = BaseFilesetDataOperationCtx(
                sub_path="/test1",
                operation=FilesetDataOperation.MKDIRS,
                client_type=ClientType.PYTHON_GVFS,
                ip="127.0.0.1",
                source_engine_type=SourceEngineType.PYSPARK,
                app_id="unknown",
            )
            context = catalog.as_fileset_catalog().get_fileset_context(
                NameIdentifier.of_fileset(
                    "test_metalake", "test_catalog", "test_schema", "test_fileset"
                ),
                ctx,
            )
            self.assertEqual(context.fileset().name(), "test_get_context")
            self.assertEqual(
                context.fileset().storage_location(),
                f"{self._fileset_dir}/test_get_context",
            )
            self.assertEqual(
                context.actual_paths()[0], f"{self._fileset_dir}/test_get_context/test1"
            )
