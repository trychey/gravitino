/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.enums;

public enum FilesetBackupLocKeyPattern {
  NUMERIC_SUFFIX(
      "^backup.storage.location.\\d+$",
      "Fileset backup storage location key pattern in Fileset properties",
      "backup.storage.location.1, backup.storage.location.2, etc.");

  private final String backupLocKeyRegex;
  private final String description;
  private final String example;

  FilesetBackupLocKeyPattern(String backupLocKeyRegex, String description, String example) {
    this.backupLocKeyRegex = backupLocKeyRegex;
    this.description = description;
    this.example = example;
  }

  public String getBackupLocKeyRegex() {
    return backupLocKeyRegex;
  }

  public String getDescription() {
    return description;
  }

  public String getExample() {
    return example;
  }
}
