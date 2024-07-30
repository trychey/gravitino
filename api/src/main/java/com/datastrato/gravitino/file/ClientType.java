/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

/** An enum class containing fileset data operations client type that supported. */
public enum ClientType {
  HADOOP_GVFS,
  PYTHON_GVFS,
  UNKNOWN;
}
