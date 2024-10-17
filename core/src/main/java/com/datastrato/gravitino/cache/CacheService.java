/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;

/** This interface defines the implementation for a cache service. */
public interface CacheService extends AutoCloseable {
  /**
   * Insert an entity into the cache.
   *
   * @param identifier The identifier of the entity to insert.
   * @param entity The entity to insert.
   */
  void insert(NameIdentifier identifier, Entity entity);

  /**
   * Delete an entity from the cache.
   *
   * @param type The type of the entity to delete.
   * @param identifier The identifier of the entity to delete.
   * @return The delete count.
   */
  long delete(Entity.EntityType type, NameIdentifier identifier);

  /**
   * Delete an entity from the cache recursively.
   *
   * @param type The type of the entity to delete.
   * @param identifier The identifier of the entity to delete.
   * @return The delete count.
   */
  long deleteRecursively(Entity.EntityType type, NameIdentifier identifier);

  /**
   * Get an entity from the cache.
   *
   * @param type The type of the entity to get.
   * @param identifier The identifier of the entity to get.
   * @return The entity.
   */
  <E extends Entity & HasIdentifier> E get(Entity.EntityType type, NameIdentifier identifier);
}
