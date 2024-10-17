/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache;

public enum CacheOperation {
  /** Delete an entity from the cache. */
  DELETE,
  /**
   * Delete an entity from the cache recursively. For example, if the entity is a metalake, then the
   * metalake and the resources under it will be deleted.
   */
  DELETE_RECURSIVE;
}
