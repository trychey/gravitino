/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntitySerDe;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.cache.CacheOperation;
import com.datastrato.gravitino.cache.CacheService;
import com.datastrato.gravitino.cache.RedisCacheService;
import com.datastrato.gravitino.cache.ServerCacheManager;
import com.datastrato.gravitino.cache.processor.CacheAsyncProcessor;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.utils.Executable;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationalBackend} interface
 */
public class RelationalEntityStore implements EntityStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEntityStore.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  public static final ImmutableMap<String, String> CACHE_IMPLS =
      ImmutableMap.of(Configs.REDIS_CACHE_KEY, RedisCacheService.class.getCanonicalName());
  private RelationalBackend backend;
  private RelationalGarbageCollector garbageCollector;
  private CacheService cacheService;
  private CacheAsyncProcessor cacheAsyncProcessor;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createRelationalEntityBackend(config);
    this.garbageCollector = new RelationalGarbageCollector(backend, config);
    this.garbageCollector.start();
    boolean enableServerCache =
        config.get(Configs.SERVER_CACHE_ENABLE) != null
            ? config.get(Configs.SERVER_CACHE_ENABLE)
            : false;
    if (enableServerCache) {
      this.cacheService = ServerCacheManager.getInstance().cacheService();
      this.cacheAsyncProcessor = ServerCacheManager.getInstance().asyncCacheProcessor();
    }
  }

  private static RelationalBackend createRelationalEntityBackend(Config config) {
    String backendName = config.get(ENTITY_RELATIONAL_STORE);
    String className =
        RELATIONAL_BACKENDS.getOrDefault(backendName, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);

    try {
      RelationalBackend relationalBackend =
          (RelationalBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      relationalBackend.initialize(config);
      return relationalBackend;
    } catch (Exception e) {
      LOGGER.error(
          "Failed to create and initialize RelationalBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize RelationalBackend by name: " + backendName, e);
    }
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType) throws IOException {
    return backend.list(namespace, entityType);
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    boolean supportedEntityCache = supportedEntityCache(entityType);
    try {
      if (cacheService != null && supportedEntityCache) {
        Entity entity = cacheService.get(entityType, ident);
        if (entity != null) {
          return true;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to get the entity: {} from the cache.", ident, ex);
    }
    boolean exists;
    Entity entity;
    try {
      entity = backend.get(ident, entityType);
      exists = entity != null;
    } catch (NoSuchEntityException ne) {
      return false;
    }
    if (exists) {
      try {
        if (cacheService != null && supportedEntityCache) {
          cacheService.insert(ident, entity);
        }
      } catch (Exception ex) {
        LOGGER.error("Failed to put the entity: {} into the cache.", ident, ex);
      }
    }
    return exists;
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    // Always insert, never overwrite in relational store, because when `Gaea` uses `insert into ...
    // on duplicate key update (xxx)`, xxx cannot contain the shard key, so overwritten is disabled.
    backend.insert(e, false);
    try {
      if (cacheAsyncProcessor != null && supportedEntityCache(e.type())) {
        cacheAsyncProcessor.process(CacheOperation.DELETE, e.type(), e.nameIdentifier());
      }
    } catch (Exception ex) {
      LOGGER.error(
          "Failed to delete the entity: {} from the cache asynchronously.", e.nameIdentifier(), ex);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    E entity = backend.update(ident, entityType, updater);
    try {
      if (cacheAsyncProcessor != null && supportedEntityCache(entityType)) {
        cacheAsyncProcessor.process(CacheOperation.DELETE, entityType, ident);
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to delete the entity: {} from the cache asynchronously.", ident, ex);
    }
    return entity;
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    try {
      if (cacheService != null && supportedEntityCache(entityType)) {
        E entity = cacheService.get(entityType, ident);
        if (entity != null) {
          return entity;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to get the entity: {} from the cache.", ident, ex);
    }
    E entity = backend.get(ident, entityType);
    try {
      if (cacheService != null && supportedEntityCache(entityType)) {
        cacheService.insert(ident, entity);
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to put the entity: {} into the cache.", ident, ex);
    }
    return entity;
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    boolean deleted = backend.delete(ident, entityType, cascade);
    try {
      if (deleted && cacheAsyncProcessor != null && supportedEntityCache(entityType)) {
        cacheAsyncProcessor.process(
            cascade ? CacheOperation.DELETE_RECURSIVE : CacheOperation.DELETE, entityType, ident);
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to delete the entity: {} from the cache asynchronously.", ident, ex);
    }
    return deleted;
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public String fetchExternalFilesetName(String storageLocation) {
    return backend.fetchExternalFilesetName(storageLocation);
  }

  @Override
  public List<UserEntity> listUsersByRole(NameIdentifier ident) {
    return backend.listUsersByRole(ident);
  }

  @Override
  public List<GroupEntity> listGroupsByRole(NameIdentifier ident) {
    return backend.listGroupsByRole(ident);
  }

  @Override
  public void close() throws IOException {
    garbageCollector.close();
    backend.close();
  }

  private boolean supportedEntityCache(Entity.EntityType type) {
    // we only support caching of the resource entities now,
    // do not support the auth entities like user, group, role to avoid consistency issues
    return type == Entity.EntityType.METALAKE
        || type == Entity.EntityType.CATALOG
        || type == Entity.EntityType.SCHEMA
        || type == Entity.EntityType.FILESET
        || type == Entity.EntityType.TABLE
        || type == Entity.EntityType.TOPIC;
  }
}
