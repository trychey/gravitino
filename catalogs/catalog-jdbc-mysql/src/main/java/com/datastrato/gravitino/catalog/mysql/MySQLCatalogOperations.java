/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql;

import com.datastrato.gravitino.catalog.jdbc.JdbcCatalogOperations;
import com.datastrato.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import java.sql.Driver;
import java.sql.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLCatalogOperations extends JdbcCatalogOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalogOperations.class);

  public MySQLCatalogOperations(
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations databaseOperation,
      JdbcTableOperations tableOperation,
      JdbcTablePropertiesMetadata jdbcTablePropertiesMetadata,
      JdbcColumnDefaultValueConverter columnDefaultValueConverter) {
    super(
        exceptionConverter,
        jdbcTypeConverter,
        databaseOperation,
        tableOperation,
        jdbcTablePropertiesMetadata,
        columnDefaultValueConverter);
  }

  @Override
  public void close() {
    super.close();

    try {
      // Close thread AbandonedConnectionCleanupThread
      Class.forName(MYSQL_CATALOG_CONNECTION_CLEAN_UP_THREAD)
          .getMethod("uncheckedShutdown")
          .invoke(null);
      LOG.info("AbandonedConnectionCleanupThread has been shutdown...");

      // Deregister the MySQL driver, only deregister the driver if it is loaded by
      // IsolatedClassLoader.
      Driver mysqlDriver = DriverManager.getDriver("jdbc:mysql://127.0.0.1:3306");
      LOG.info(
          "MySQL driver class loader: {}",
          mysqlDriver.getClass().getClassLoader().getClass().getName());

      deregisterDriver(mysqlDriver);
    } catch (Exception e) {
      // Ignore
      LOG.warn("Failed to shutdown AbandonedConnectionCleanupThread or Deregister MySQL driver", e);
    }
  }
}
