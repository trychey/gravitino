/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache.processor;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.cache.CacheOperation;
import java.util.Objects;

public class AsyncCacheEvent {
  private CacheOperation operation;
  private Entity.EntityType type;
  private NameIdentifier identifier;

  private AsyncCacheEvent() {}

  public AsyncCacheEvent(
      CacheOperation operation, Entity.EntityType type, NameIdentifier identifier) {
    this.operation = operation;
    this.type = type;
    this.identifier = identifier;
  }

  public CacheOperation operation() {
    return operation;
  }

  public Entity.EntityType type() {
    return type;
  }

  public NameIdentifier identifier() {
    return identifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AsyncCacheEvent)) return false;
    AsyncCacheEvent that = (AsyncCacheEvent) o;
    return operation == that.operation
        && type == that.type
        && Objects.equals(identifier, that.identifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operation, type, identifier);
  }
}
