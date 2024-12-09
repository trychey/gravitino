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
package org.apache.gravitino.catalog.clickhouse.operation;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/** Database operations for ClickHouse. */
public class ClickHouseDatabaseOperations extends JdbcDatabaseOperations {

  @Override
  protected boolean supportSchemaComment() {
    return false;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of("information_schema", "INFORMATION_SCHEMA", "default", "system");
  }

  @Override
  public boolean delete(String databaseName, boolean cascade) {
    LOG.info("Beginning to drop database {}", databaseName);
    try {
      dropDatabase(databaseName, cascade);
      LOG.info("Finished dropping database {}", databaseName);
    } catch (NoSuchSchemaException e) {
      return false;
    } catch (Exception e) {
      if (e.getMessage() != null
          && (e.getMessage().contains("Database " + databaseName + " does not exist.")
              || e.getMessage().contains("Database `" + databaseName + "` does not exist."))) {
        return false;
      }

      if (e.getMessage() != null
          && e.getMessage()
              .contains(
                  "Database "
                      + databaseName
                      + " is not empty, the value of cascade should be true.")) {
        throw e;
      }
    }
    return true;
  }

  @Override
  public List<String> listDatabases() {
    List<String> databaseNames = new ArrayList<>();
    try (final Connection connection = getConnection()) {
      // It is possible that other catalogs have been deleted,
      // causing the following statement to error,
      // so here we manually set a system catalog
      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
          while (resultSet.next()) {
            String databaseName = resultSet.getString(1);
            if (!isSystemDatabase(databaseName)) {
              databaseNames.add(databaseName);
            }
          }
        }
      }
      return databaseNames;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }
}
