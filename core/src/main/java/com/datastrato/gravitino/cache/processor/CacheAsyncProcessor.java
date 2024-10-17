/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache.processor;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.cache.CacheOperation;

/**
 * This interface is used for operate the server cache asynchronously. For example, to avoid cache
 * deletion failure and synchronization operations from causing unnecessary impact on the main
 * process.
 */
public interface CacheAsyncProcessor extends AutoCloseable {

  /**
   * This method is used to process a cache operation asynchronously.
   *
   * @param operation The cache operation type.
   * @param type The entity type.
   * @param identifier The identifier of the entity.
   */
  void process(CacheOperation operation, Entity.EntityType type, NameIdentifier identifier);
}
