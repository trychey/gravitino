/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop.context;

import com.datastrato.gravitino.secret.Secret;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class SimpleFileSystemContext implements FileSystemContext {
  private final FileSystem fileSystem;

  public SimpleFileSystemContext(URI uri, Configuration configuration) throws IOException {
    this.fileSystem = FileSystem.newInstance(uri, configuration);
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public Secret getSecret() {
    throw new UnsupportedOperationException("Unsupported operation for SimpleFileSystemContext");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SimpleFileSystemContext)) return false;
    SimpleFileSystemContext that = (SimpleFileSystemContext) o;
    return Objects.equals(fileSystem, that.fileSystem);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fileSystem);
  }

  @Override
  public void close() {
    try {
      fileSystem.close();
    } catch (IOException e) {
      // Ignore
    }
  }
}
