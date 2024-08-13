/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

/** An enum class containing fileset data operations source engine type that supported. */
public enum SourceEngineType {
  SPARK,
  PYSPARK,
  FLINK,
  CLOUDML,
  NOTEBOOK,
  UNKNOWN;
}
