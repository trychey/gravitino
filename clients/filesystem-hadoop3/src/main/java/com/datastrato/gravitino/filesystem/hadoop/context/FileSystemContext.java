/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop.context;

import com.datastrato.gravitino.secret.Secret;
import org.apache.hadoop.fs.FileSystem;

/** Context for FileSystem. */
public interface FileSystemContext {

  /**
   * Get the FileSystem.
   *
   * @return FileSystem
   */
  FileSystem getFileSystem();

  /**
   * Get the Secret.
   *
   * @return Secret
   */
  Secret getSecret();

  /** Close the context */
  void close();
}
