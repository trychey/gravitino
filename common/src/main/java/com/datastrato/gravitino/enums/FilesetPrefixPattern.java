/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.enums;

/** Enum for fileset directory prefix patterns. */
public enum FilesetPrefixPattern {
  // If the path after fileset ident is empty (like gvfs://fileset/catalog/db/fileset_1),
  // cannot use this regex to match, need to be handled independently
  ANY(
      "^/[^/]+(?:/[^/]+){0,}$",
      1,
      "No specific dir prefix requirement, is like /, /xxx or /xxx/yyy",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/xxx"),
  DATE(
      "^/\\d{8}(?:/[^/]+){0,}$",
      1,
      "Dir prefix is like /20240408/xxx",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/20240408/xxx"),
  DATE_HOUR(
      "^/\\d{10}(?:/[^/]+){0,}$",
      1,
      "Dir prefix is like /2024040812/xxx",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/2024040812/xxx"),
  // US means Underscore "_" symbol
  DATE_US_HOUR(
      "^/\\d{8}_\\d{2}(?:/[^/]+){0,}$",
      1,
      "Dir prefix is like /20240408_12/xxx",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/20240408_12/xxx"),
  // US means Underscore "_" symbol
  DATE_US_HOUR_US_MINUTE(
      "^/\\d{8}_\\d{2}_\\d{2}(?:/[^/]+){0,}$",
      1,
      "Dir prefix is like /20240408_12_00/xxx",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/20240408_12_00/xxx"),
  DATE_WITH_STRING(
      "^/date=\\d{8}(?:/[^/]+){0,}$",
      1,
      "Dir prefix is like /date=20240408/xxx",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/date=20240408/xxx"),
  YEAR_MONTH_DAY(
      "^/year=\\d{4}/month=\\d{2}/day=\\d{2}(?:/[^/]+){0,}$",
      3,
      "Dir prefix is like /year=2024/month=04/day=08/xxx",
      "gvfs://fileset/${catalog_name}/${db_name}/${fileset_name}/year=2024/month=04/day=08/xxx");

  private final String prefixRegex;
  private final Integer dirLevel;
  private final String description;
  private final String example;

  FilesetPrefixPattern(String prefixRegex, Integer dirLevel, String description, String example) {
    this.prefixRegex = prefixRegex;
    this.dirLevel = dirLevel;
    this.description = description;
    this.example = example;
  }

  public String getPrefixRegex() {
    return prefixRegex;
  }

  public Integer getDirLevel() {
    return dirLevel;
  }

  public String getDescription() {
    return description;
  }

  public String getExample() {
    return example;
  }
}
