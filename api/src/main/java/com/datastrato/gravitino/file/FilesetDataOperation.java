/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

/** An enum class containing fileset data operations that supported. */
public enum FilesetDataOperation {
  CREATE,
  OPEN,
  APPEND,
  RENAME,
  DELETE,
  GET_FILE_STATUS,
  LIST_STATUS,
  MKDIRS,
  TRUNCATE,
  GET_DEFAULT_REPLICATION,
  GET_DEFAULT_BLOCK_SIZE,
  GET_FILE_CHECKSUM,
  SET_WORKING_DIR,
  EXISTS,
  CREATED_TIME,
  MODIFIED_TIME,
  COPY_FILE,
  CAT_FILE,
  GET_FILE,
  UNKNOWN;
}
