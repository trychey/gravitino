/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.PropertyEntry;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class TestFilesetPropertiesMetadata extends TestBasePropertiesMetadata {

  public static final String TEST_FILESET_HIDDEN_KEY = "fileset_key";

  private static final Map<String, PropertyEntry<?>> TEST_FILESET_PROPERTY;

  static {
    List<PropertyEntry<?>> tablePropertyMetadata =
        ImmutableList.of(
            PropertyEntry.enumPropertyEntry(
                FilesetProperties.PREFIX_PATTERN_KEY,
                "The fileset dir prefix pattern",
                false,
                false,
                FilesetPrefixPattern.class,
                FilesetPrefixPattern.ANY,
                false,
                false),
            PropertyEntry.integerPropertyEntry(
                FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
                "The fileset lifecycle time number",
                false,
                false,
                -1,
                false,
                false),
            PropertyEntry.enumPropertyEntry(
                FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
                "The fileset lifecycle time unit",
                false,
                false,
                FilesetLifecycleUnit.class,
                FilesetLifecycleUnit.RETENTION_DAY,
                false,
                false),
            PropertyEntry.integerPropertyEntry(
                FilesetProperties.DIR_MAX_LEVEL_KEY,
                "The fileset dir max level",
                false,
                false,
                -1,
                false,
                false));

    TEST_FILESET_PROPERTY = Maps.uniqueIndex(tablePropertyMetadata, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .putAll(super.specificPropertyEntries())
        .putAll(TEST_FILESET_PROPERTY)
        .build();
  }
}
