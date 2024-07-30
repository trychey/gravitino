/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.file;

import com.datastrato.gravitino.file.FilesetContext;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

/** Represents a Fileset context DTO (Data Transfer Object). */
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FilesetContextDTO implements FilesetContext {

  @JsonProperty("fileset")
  private FilesetDTO fileset;

  @JsonProperty("actualPaths")
  private String[] actualPaths;

  public FilesetDTO fileset() {
    return fileset;
  }

  public String[] actualPaths() {
    return actualPaths;
  }

  @Builder(builderMethodName = "builder")
  private static FilesetContextDTO internalBuilder(FilesetDTO fileset, String[] actualPaths) {
    Preconditions.checkNotNull(fileset, "fileset cannot be null");
    Preconditions.checkNotNull(actualPaths, "actual paths cannot be null");

    return new FilesetContextDTO(fileset, actualPaths);
  }
}
