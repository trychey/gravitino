/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.google.common.base.Objects;

/** This class is used to save some properties related to Fileset prefix properties */
public class FilesetPrefixProperties {
  private FilesetPrefixPattern pattern;
  private Integer maxLevel;

  private FilesetPrefixProperties() {}

  public FilesetPrefixPattern getPattern() {
    return pattern;
  }

  public Integer getMaxLevel() {
    return maxLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FilesetPrefixProperties)) return false;
    FilesetPrefixProperties that = (FilesetPrefixProperties) o;
    return getPattern() == that.getPattern() && Objects.equal(getMaxLevel(), that.getMaxLevel());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getPattern(), getMaxLevel());
  }

  public static FilesetPrefixProperties.Builder builder() {
    return new FilesetPrefixProperties.Builder();
  }

  /** A builder class for {@link FilesetPrefixProperties}. */
  public static class Builder {

    private final FilesetPrefixProperties prefixProperties;

    private Builder() {
      this.prefixProperties = new FilesetPrefixProperties();
    }

    public FilesetPrefixProperties.Builder withPattern(FilesetPrefixPattern pattern) {
      prefixProperties.pattern = pattern;
      return this;
    }

    public FilesetPrefixProperties.Builder withMaxLevel(Integer maxLevel) {
      prefixProperties.maxLevel = maxLevel;
      return this;
    }

    public FilesetPrefixProperties build() {
      Preconditions.checkArgument(prefixProperties.pattern != null, "Prefix pattern is required");
      Preconditions.checkArgument(prefixProperties.maxLevel != null, "Max level is required");
      return prefixProperties;
    }
  }
}
