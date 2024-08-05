"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging
from random import randint
from typing import Dict, List

from gravitino import (
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
    Fileset,
    FilesetChange,
)
from gravitino.api.base_fileset_data_operation_ctx import BaseFilesetDataOperationCtx
from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.api.source_engine_type import SourceEngineType
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestFilesetCatalog(IntegrationTestEnv):
    metalake_name: str = "TestFilesetCatalog-metalake" + str(randint(1, 10000))
    catalog_name: str = "catalog"
    catalog_location_prop: str = "location"  # Fileset Catalog must set `location`
    catalog_provider: str = "hadoop"

    schema_name: str = "schema"

    fileset_name: str = "fileset"
    fileset_alter_name: str = fileset_name + "Alter"
    fileset_comment: str = "fileset_comment"

    fileset_location: str = "/tmp/TestFilesetCatalog"
    fileset_properties_key1: str = "fileset_properties_key1"
    fileset_properties_value1: str = "fileset_properties_value1"
    fileset_properties_key2: str = "fileset_properties_key2"
    fileset_properties_value2: str = "fileset_properties_value2"
    fileset_properties: Dict[str, str] = {
        fileset_properties_key1: fileset_properties_value1,
        fileset_properties_key2: fileset_properties_value2,
        "dir-max-level": "3",
        "lifecycle-time-num": "-1",
        "lifecycle-time-unit": "RETENTION_DAY",
        "prefix-pattern": "ANY",
    }
    fileset_new_name = fileset_name + "_new"

    metalake_ident: NameIdentifier = NameIdentifier.of(metalake_name)
    catalog_ident: NameIdentifier = NameIdentifier.of_catalog(
        metalake_name, catalog_name
    )
    schema_ident: NameIdentifier = NameIdentifier.of_schema(
        metalake_name, catalog_name, schema_name
    )
    fileset_ident: NameIdentifier = NameIdentifier.of_fileset(
        metalake_name, catalog_name, schema_name, fileset_name
    )
    fileset_new_ident: NameIdentifier = NameIdentifier.of_fileset(
        metalake_name, catalog_name, schema_name, fileset_new_name
    )

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    gravitino_client: GravitinoClient = None

    def setUp(self):
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):
        try:
            self.gravitino_client = GravitinoClient(
                uri="http://localhost:8090", metalake_name=self.metalake_name
            )
            catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
            logger.info(
                "Drop fileset %s[%s]",
                self.fileset_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident),
            )
            logger.info(
                "Drop fileset %s[%s]",
                self.fileset_new_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_new_ident),
            )
            logger.info(
                "Drop schema %s[%s]",
                self.schema_ident,
                catalog.as_schemas().drop_schema(ident=self.schema_ident, cascade=True),
            )
            logger.info(
                "Drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(ident=self.catalog_ident),
            )
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_ident,
                self.gravitino_admin_client.drop_metalake(self.metalake_ident),
            )
        except Exception as e:
            logger.error("Clean test data failed: %s", e)

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            ident=self.metalake_ident, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        catalog = self.gravitino_client.create_catalog(
            ident=self.catalog_ident,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment="",
            properties={self.catalog_location_prop: "/tmp/test1"},
        )
        catalog.as_schemas().create_schema(
            ident=self.schema_ident, comment="", properties={}
        )

    def create_fileset(self) -> Fileset:
        catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
        return catalog.as_fileset_catalog().create_fileset(
            ident=self.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=self.fileset_location,
            properties=self.fileset_properties,
        )

    def create_custom_fileset(
        self, ident: NameIdentifier, location: str, properties: Dict
    ) -> Fileset:
        catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
        return catalog.as_fileset_catalog().create_fileset(
            ident=ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=location,
            properties=properties,
        )

    def test_create_fileset(self):
        fileset = self.create_fileset()
        self.assertIsNotNone(fileset)
        self.assertEqual(fileset.type(), Fileset.Type.MANAGED)
        self.assertEqual(fileset.comment(), self.fileset_comment)
        self.assertEqual(fileset.properties(), self.fileset_properties)

    def test_drop_fileset(self):
        self.create_fileset()
        catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
        self.assertTrue(
            catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident)
        )

    def test_list_fileset(self):
        self.create_fileset()
        catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
        fileset_list: List[NameIdentifier] = catalog.as_fileset_catalog().list_filesets(
            namespace=self.fileset_ident.namespace()
        )
        self.assertTrue(any(item.name() == self.fileset_name for item in fileset_list))

    def test_load_fileset(self):
        self.create_fileset()
        fileset = (
            self.gravitino_client.load_catalog(ident=self.catalog_ident)
            .as_fileset_catalog()
            .load_fileset(ident=self.fileset_ident)
        )
        self.assertIsNotNone(fileset)
        self.assertEqual(fileset.name(), self.fileset_name)
        self.assertEqual(fileset.comment(), self.fileset_comment)
        self.assertEqual(fileset.properties(), self.fileset_properties)
        self.assertEqual(fileset.audit_info().creator(), "anonymous")

    def test_alter_fileset(self):
        self.create_fileset()
        fileset_properties_new_value = self.fileset_properties_value2 + "_new"
        fileset_new_comment = self.fileset_comment + "_new"

        changes = (
            FilesetChange.remove_property(self.fileset_properties_key1),
            FilesetChange.set_property(
                self.fileset_properties_key2, fileset_properties_new_value
            ),
            FilesetChange.update_comment(fileset_new_comment),
        )
        catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
        fileset_new = catalog.as_fileset_catalog().alter_fileset(
            self.fileset_ident, *changes
        )
        self.assertEqual(
            fileset_new.properties().get(self.fileset_properties_key2),
            fileset_properties_new_value,
        )
        self.assertTrue(self.fileset_properties_key1 not in fileset_new.properties())
        self.assertEqual(fileset_new.comment(), fileset_new_comment)

        fileset_comment_removed = catalog.as_fileset_catalog().alter_fileset(
            self.fileset_ident, FilesetChange.remove_comment()
        )
        self.assertEqual(fileset_comment_removed.name(), self.fileset_name)
        self.assertIsNone(fileset_comment_removed.comment())

    def test_get_fileset_context(self):
        fileset_name = "test_get_context"
        f_ident = NameIdentifier.of_fileset(
            self.metalake_name, self.catalog_name, self.schema_name, fileset_name
        )
        try:
            f_location = "/tmp/test_get_fileset_context"
            properties = {}
            self.create_custom_fileset(f_ident, f_location, properties)
            fileset = (
                self.gravitino_client.load_catalog(ident=self.catalog_ident)
                .as_fileset_catalog()
                .load_fileset(ident=f_ident)
            )
            self.assertIsNotNone(fileset)
            self.assertEqual(fileset.name(), fileset_name)
            self.assertEqual(fileset.comment(), self.fileset_comment)
            self.assertEqual(fileset.audit_info().creator(), "anonymous")

            ctx = BaseFilesetDataOperationCtx(
                sub_path="/test",
                operation=FilesetDataOperation.MKDIRS,
                client_type=ClientType.PYTHON_GVFS,
                ip="127.0.0.1",
                source_engine_type=SourceEngineType.PYSPARK,
                app_id="application_1_1",
            )
            context = (
                self.gravitino_client.load_catalog(ident=self.catalog_ident)
                .as_fileset_catalog()
                .get_fileset_context(ident=f_ident, ctx=ctx)
            )
            self.assertIsNotNone(context)
            self.assertEqual(context.fileset().name(), fileset_name)
            self.assertEqual(context.fileset().comment(), self.fileset_comment)
            self.assertEqual(context.fileset().audit_info().creator(), "anonymous")
            self.assertEqual(context.actual_paths()[0], f_location + "/test")
        finally:
            self.gravitino_client.load_catalog(
                ident=self.catalog_ident
            ).as_fileset_catalog().drop_fileset(ident=f_ident)
