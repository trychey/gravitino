/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link PropertiesMetadata} that represents Paimon table properties metadata.
 */
public class PaimonTablePropertiesMetadata extends BasePropertiesMetadata {

  public static final String COMMENT = "comment";
  public static final String OWNER = "owner";
  public static final String BUCKET_KEY = "bucket-key";
  public static final String MERGE_ENGINE = "merge-engine";
  public static final String SEQUENCE_FIELD = "sequence.field";
  public static final String ROWKIND_FIELD = "rowkind.field";
  public static final String PRIMARY_KEY = "primary-key";
  public static final String PARTITION = "partition";
  public static final String TABLE_CREATE_TIMESTAMP_MS = "table.create.timestamp-ms";
  public static final String TABLE_MODIFY_TIMESTAMP_MS = "table.modify.timestamp-ms";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "The table comment", true),
            stringOptionalPropertyEntry(OWNER, "The table owner", false, null, false),
            stringImmutablePropertyEntry(
                BUCKET_KEY, "The table bucket key", false, null, false, false),
            stringImmutablePropertyEntry(
                MERGE_ENGINE, "The table merge engine", false, null, false, false),
            stringImmutablePropertyEntry(
                SEQUENCE_FIELD, "The table sequence field", false, null, false, false),
            stringImmutablePropertyEntry(
                ROWKIND_FIELD, "The table rowkind field", false, null, false, false),
            stringImmutablePropertyEntry(
                PRIMARY_KEY, "The table primary key", false, null, false, false),
            stringImmutablePropertyEntry(
                PARTITION, "The table partition", false, null, false, false),
            stringImmutablePropertyEntry(
                TABLE_CREATE_TIMESTAMP_MS, "The table create timestamp", false, null, false, false),
            stringReservedPropertyEntry(
                TABLE_MODIFY_TIMESTAMP_MS, "The table modify timestamp", true));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
