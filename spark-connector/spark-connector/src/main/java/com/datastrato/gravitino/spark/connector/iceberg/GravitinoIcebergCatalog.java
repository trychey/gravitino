/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.catalog.BaseCatalog;
import java.util.Map;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The GravitinoIcebergCatalog class extends the BaseCatalog to integrate with the Iceberg table
 * format, providing specialized support for Iceberg-specific functionalities within Spark's
 * ecosystem. This implementation can further adapt to specific interfaces such as
 * StagingTableCatalog and FunctionCatalog, allowing for advanced operations like table staging and
 * function management tailored to the needs of Iceberg tables.
 */
public class GravitinoIcebergCatalog extends BaseCatalog implements FunctionCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    TableCatalog icebergCatalog = new SparkCatalog();
    icebergCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    return icebergCatalog;
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    return new SparkIcebergTable(
        identifier,
        gravitinoTable,
        (SparkTable) sparkTable,
        (SparkCatalog) sparkIcebergCatalog,
        propertiesConverter,
        sparkTransformConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return IcebergPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(true);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return ((SparkCatalog) sparkCatalog).listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return ((SparkCatalog) sparkCatalog).loadFunction(ident);
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, String version)
      throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident, version);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, long timestamp)
      throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident, timestamp);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  private com.datastrato.gravitino.rel.Table loadGravitinoTable(Identifier ident)
      throws NoSuchTableException {
    try {
      String database = getDatabase(ident);
      return gravitinoCatalogClient
          .asTableCatalog()
          .loadTable(NameIdentifier.of(metalakeName, catalogName, database, ident.name()));
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  private org.apache.spark.sql.connector.catalog.Table loadSparkTable(
      Identifier ident, String version) {
    try {
      return sparkCatalog.loadTable(ident, version);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }

  private org.apache.spark.sql.connector.catalog.Table loadSparkTable(
      Identifier ident, long timestamp) {
    try {
      return sparkCatalog.loadTable(ident, timestamp);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }
}
