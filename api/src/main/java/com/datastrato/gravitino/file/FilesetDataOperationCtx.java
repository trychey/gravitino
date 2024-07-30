/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Map;

/**
 * An interface representing a fileset data operation context. This interface defines some
 * information need to report to the server.
 *
 * <p>{@link FilesetDataOperationCtx} defines the basic properties of a fileset data operation
 * context object.
 */
@Evolving
public interface FilesetDataOperationCtx {
  /**
   * @return The sub path which is operated by the data operation. For example: /test.txt, test.txt.
   */
  String subPath();

  /** @return The data operation type. For example: CREATE, APPEND, DELETE, etc. */
  FilesetDataOperation operation();

  /**
   * @return The client type of the data operation happened. For example: HADOOP_GVFS, PYTHON_GVFS,
   *     UNKNOWN.
   */
  ClientType clientType();

  /** @return The client ip of the data operation. */
  String ip();

  /**
   * @return The source engine type of the data operation happened. For example: SPARK, FLINK,
   *     CLOUDML, UNKNOWN.
   */
  SourceEngineType sourceEngineType();

  /** @return The application id of the data operation. */
  String appId();

  /** @return The extra info of the data operation. */
  Map<String, String> extraInfo();
}
