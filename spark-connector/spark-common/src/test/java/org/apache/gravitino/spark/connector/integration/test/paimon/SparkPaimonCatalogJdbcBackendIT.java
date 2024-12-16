/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.spark.connector.integration.test.paimon;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.spark.connector.paimon.PaimonPropertiesConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** This class use Apache Paimon JdbcCatalog for backend catalog. */
@Tag("gravitino-docker-test")
public abstract class SparkPaimonCatalogJdbcBackendIT extends SparkPaimonCatalogIT {

  @Override
  protected Map<String, String> getCatalogConfigs() {
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND,
        PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_JDBC);
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_URI,
        mySQLContainer.getJdbcUrl(TEST_DB_NAME));
    catalogProperties.put(PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_WAREHOUSE, warehouse);
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_JDBC_USER, mySQLContainer.getUsername());
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_JDBC_PASSWORD,
        mySQLContainer.getPassword());
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver");
    return catalogProperties;
  }

  @Test
  @Override
  protected void testAlterSchema() {
    String testDatabaseName = "t_alter";
    dropDatabaseIfExists(testDatabaseName);
    sql(
        "CREATE DATABASE "
            + testDatabaseName
            + " COMMENT 'db comment' WITH DBPROPERTIES (ID=001);");
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertTrue(databaseMeta.get("Properties").contains("(ID,001)"));
    Assertions.assertEquals("db comment", databaseMeta.get("Comment"));

    // The Paimon filesystem backend do not support alter database operation.
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            sql(
                String.format(
                    "ALTER DATABASE %s SET DBPROPERTIES ('ID'='002')", testDatabaseName)));
  }
}
