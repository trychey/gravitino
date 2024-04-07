/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class SparkIcebergTable extends SparkBaseTable {

  public SparkIcebergTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter) {
    super(identifier, gravitinoTable, sparkIcebergCatalog, propertiesConverter);
  }

  @Override
  public Map<String, String> properties() {
    return getSparkTable().properties();
  }
}
