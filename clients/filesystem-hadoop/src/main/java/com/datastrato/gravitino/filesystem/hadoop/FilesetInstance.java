/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.file.Fileset;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;

public class FilesetInstance {
  private Fileset fileset;
  private FileSystem fileSystem;

  private FilesetInstance(Fileset fileset, FileSystem fileSystem) {
    this.fileset = fileset;
    this.fileSystem = fileSystem;
  }

  public Fileset getFileset() {
    return fileset;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilesetInstance)) {
      return false;
    }
    FilesetInstance that = (FilesetInstance) o;
    return Objects.equal(getFileset(), that.getFileset())
        && Objects.equal(getFileSystem(), that.getFileSystem());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFileset(), getFileSystem());
  }

  /**
   * Creates a new builder for constructing a FilesetInstance.
   *
   * @return A new instance of the Builder class for constructing a FilesetInstance.
   */
  public static FilesetInstance.Builder builder() {
    return new FilesetInstance.Builder();
  }

  public static class Builder {
    private Fileset fileset;
    private FileSystem fileSystem;

    private Builder() {}

    public Builder withFileset(Fileset fileset) {
      this.fileset = fileset;
      return this;
    }

    public Builder withFileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public FilesetInstance build() {
      Preconditions.checkArgument(
          fileset != null && fileSystem != null, "Fileset and FileSystem must be set");
      return new FilesetInstance(fileset, fileSystem);
    }
  }
}
