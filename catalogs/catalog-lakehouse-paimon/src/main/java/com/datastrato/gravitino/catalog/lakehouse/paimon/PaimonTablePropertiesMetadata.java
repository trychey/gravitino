/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

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

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "The table comment", true),
            stringReservedPropertyEntry(OWNER, "The table owner", false),
            stringReservedPropertyEntry(BUCKET_KEY, "The table bucket key", false),
            stringReservedPropertyEntry(MERGE_ENGINE, "The table merge engine", false),
            stringReservedPropertyEntry(SEQUENCE_FIELD, "The table sequence field", false),
            stringReservedPropertyEntry(ROWKIND_FIELD, "The table rowkind field", false),
            stringReservedPropertyEntry(PRIMARY_KEY, "The table primary key", false),
            stringReservedPropertyEntry(PARTITION, "The table partition", false));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
