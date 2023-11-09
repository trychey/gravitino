/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import java.io.IOException;

public interface KvTransactionManager extends KvBackend {

  /** Begin the transaction. */
  void begin();

  /** Commit the transaction. */
  void commit() throws IOException;

  /** Rollback the transaction if something goes wrong. */
  void rollback() throws IOException;
}
