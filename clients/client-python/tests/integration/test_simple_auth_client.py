"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import logging
import os
from random import randint
from typing import Dict

from gravitino import (
    NameIdentifier,
    GravitinoClient,
    GravitinoAdminClient,
    Catalog,
    Fileset,
)
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestSimpleAuthClient(IntegrationTestEnv):
    creator: str = "test_client"
    metalake_name: str = "TestClient_metalake" + str(randint(1, 10000))
    catalog_name: str = "fileset_catalog"
    catalog_location_prop: str = "location"  # Fileset Catalog must set `location`
    catalog_provider: str = "hadoop"

    schema_name: str = "fileset_schema"

    fileset_name: str = "test_client_fileset"
    fileset_comment: str = "fileset_comment"

    fileset_location: str = "/tmp/TestFilesetCatalog"
    fileset_properties_key1: str = "fileset_properties_key1"
    fileset_properties_value1: str = "fileset_properties_value1"
    fileset_properties_key2: str = "fileset_properties_key2"
    fileset_properties_value2: str = "fileset_properties_value2"
    fileset_properties: Dict[str, str] = {
        fileset_properties_key1: fileset_properties_value1,
        fileset_properties_key2: fileset_properties_value2,
    }

    catalog_ident: NameIdentifier = NameIdentifier.of(metalake_name, catalog_name)
    schema_ident: NameIdentifier = NameIdentifier.of(
        metalake_name, catalog_name, schema_name
    )
    fileset_ident: NameIdentifier = NameIdentifier.of(schema_name, fileset_name)

    def setUp(self):
        os.environ["GRAVITINO_USER"] = self.creator
        self.gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090", auth_data_provider=SimpleAuthProvider()
        )
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):
        try:
            catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
            logger.info(
                "Drop fileset %s[%s]",
                self.fileset_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident),
            )
            logger.info(
                "Drop schema %s[%s]",
                self.schema_ident,
                catalog.as_schemas().drop_schema(
                    schema_name=self.schema_name, cascade=True
                ),
            )
            logger.info(
                "Drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(name=self.catalog_name),
            )
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_name,
                self.gravitino_admin_client.drop_metalake(self.metalake_name),
            )
        except Exception as e:
            logger.error("Clean test data failed: %s", e)
        finally:
            os.environ["GRAVITINO_USER"] = ""

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            auth_data_provider=SimpleAuthProvider(),
        )
        catalog = self.gravitino_client.create_catalog(
            name=self.catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment="",
            properties={self.catalog_location_prop: "/tmp/test1"},
        )
        catalog.as_schemas().create_schema(
            schema_name=self.schema_name, comment="", properties={}
        )
        catalog.as_fileset_catalog().create_fileset(
            ident=self.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=self.fileset_location,
            properties=self.fileset_properties,
        )

    def test_metalake_creator(self):
        metalake = self.gravitino_admin_client.load_metalake(self.metalake_name)
        self.assertEqual(metalake.audit_info().creator(), self.creator)

    def test_catalog_creator(self):
        catalog = self.gravitino_client.load_catalog(self.catalog_name)
        self.assertEqual(catalog.audit_info().creator(), self.creator)

    def test_schema_creator(self):
        catalog = self.gravitino_client.load_catalog(self.catalog_name)
        schema = catalog.as_schemas().load_schema(self.schema_name)
        self.assertEqual(schema.audit_info().creator(), self.creator)

    def test_fileset_creator(self):
        catalog = self.gravitino_client.load_catalog(self.catalog_name)
        fileset = catalog.as_fileset_catalog().load_fileset(self.fileset_ident)
        self.assertEqual(fileset.audit_info().creator(), self.creator)
