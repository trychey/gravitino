/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class HadoopCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  // Property "location" is used to specify the storage location managed by Hadoop fileset catalog.
  // If specified, the location will be used as the default storage location for all
  // managed filesets created under this catalog.
  //
  // If not, users have to specify the storage location in the Schema or Fileset level.
  public static final String LOCATION = "location";

  // match location like hdfs://zjyprc-hadoop/user/h_data_platform/fileset/catalog1/db1/fileset1
  // split by ","
  public static final String VALID_MANAGED_PATHS = "gvfs.valid.managed-paths";
  public static final String VALID_EXTERNAL_PATHS = "gvfs.valid.external-paths";

  private static final Map<String, PropertyEntry<?>> HADOOP_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              LOCATION,
              PropertyEntry.stringOptionalPropertyEntry(
                  LOCATION,
                  "The storage location managed by Hadoop fileset catalog",
                  true /* immutable */,
                  null,
                  false /* hidden */))
          .putAll(BASIC_CATALOG_PROPERTY_ENTRIES)
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return HADOOP_CATALOG_PROPERTY_ENTRIES;
  }
}
