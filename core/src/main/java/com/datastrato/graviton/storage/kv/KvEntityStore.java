/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Entity.EntityType.CATALOG;
import static com.datastrato.graviton.Entity.EntityType.SCHEMA;
import static com.datastrato.graviton.Entity.EntityType.TABLE;
import static com.datastrato.graviton.storage.kv.BinaryEntityKeyEncoder.LOG;
import static com.datastrato.graviton.storage.kv.BinaryEntityKeyEncoder.NAMESPACE_SEPARATOR;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.exceptions.AlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchEntityException;
import com.datastrato.graviton.exceptions.NonEmptyEntityException;
import com.datastrato.graviton.storage.EntityKeyEncoder;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.utils.Bytes;
import com.datastrato.graviton.utils.Executable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV store to store entities. This means we can store entities in a key value store. I.e., RocksDB,
 * Cassandra, etc. If you want to use a different backend, you can implement the {@link KvBackend}
 * interface
 */
public class KvEntityStore implements EntityStore {
  public static final Logger LOGGER = LoggerFactory.getLogger(KvEntityStore.class);
  public static final ImmutableMap<String, String> KV_BACKENDS =
      ImmutableMap.of("RocksDBKvBackend", RocksDBKvBackend.class.getCanonicalName());

  @Getter @VisibleForTesting private KvBackend backend;
  private EntityKeyEncoder<byte[]> entityKeyEncoder;
  private NameMappingService nameMappingService;
  private EntitySerDe serDe;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createKvEntityBackend(config);
    // TODO(yuqi) Currently, KvNameMappingService and KvEntityStore shares the same backend
    //  instance, We should make it configurable in the future.
    this.nameMappingService = new KvNameMappingService(backend);
    this.entityKeyEncoder = new BinaryEntityKeyEncoder(nameMappingService);
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    this.serDe = entitySerDe;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> e, EntityType type, int maxToList) throws IOException {
    // Star means it's a wildcard
    List<E> entities = Lists.newArrayList();
    NameIdentifier identifier = NameIdentifier.of(namespace, BinaryEntityKeyEncoder.WILD_CARD);
    byte[] startKey = entityKeyEncoder.encode(identifier, type, true);
    if (startKey == null) {
      return entities;
    }

    byte[] endKey = Bytes.increment(Bytes.wrap(startKey)).get();
    List<Pair<byte[], byte[]>> kvs =
        backend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(startKey)
                .end(endKey)
                .startInclusive(true)
                .endInclusive(false)
                .limit(maxToList)
                .build());

    for (Pair<byte[], byte[]> pairs : kvs) {
      entities.add(serDe.deserialize(pairs.getRight(), e));
    }
    return entities;
  }

  @Override
  public boolean exists(NameIdentifier ident, EntityType entityType) throws IOException {
    byte[] key = entityKeyEncoder.encode(ident, entityType, true);
    if (key == null) {
      return false;
    }

    return backend.get(key) != null;
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    byte[] key = entityKeyEncoder.encode(e.nameIdentifier(), e.type());
    byte[] value = serDe.serialize(e);
    backend.put(key, value, overwritten);
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    byte[] key = entityKeyEncoder.encode(ident, entityType);
    return executeInTransaction(
        () -> {
          byte[] value = backend.get(key);
          if (value == null) {
            throw new NoSuchEntityException(ident.toString());
          }

          E e = serDe.deserialize(value, type);
          E updatedE = updater.apply(e);
          if (updatedE.nameIdentifier().equals(ident)) {
            backend.put(key, serDe.serialize(updatedE), true);
            return updatedE;
          }

          // If we have changed the name of the entity, We would do the following steps:
          // Check whether the new entities already existed
          boolean newEntityExist = exists(updatedE.nameIdentifier(), entityType);
          if (newEntityExist) {
            throw new AlreadyExistsException(
                String.format(
                    "Entity %s already exist, please check again", updatedE.nameIdentifier()));
          }

          // Update the name mapping
          nameMappingService.updateName(
              generateKeyForMapping(ident), generateKeyForMapping(updatedE.nameIdentifier()));

          // Update the entity to store
          backend.put(key, serDe.serialize(updatedE), true);
          return updatedE;
        });
  }

  private String concatIdAndName(long[] namespaceIds, String name) {
    String context =
        Joiner.on(NAMESPACE_SEPARATOR)
            .join(
                Arrays.stream(namespaceIds).mapToObj(String::valueOf).collect(Collectors.toList()));
    return StringUtils.isBlank(context) ? name : context + NAMESPACE_SEPARATOR + name;
  }

  /**
   * Generate the key for name to id mapping. Currently, the mapping is as following.
   *
   * <pre>
   *   Assume we have the following entities:
   *   metalake: a1        ----> 1
   *   catalog : a1.b1     ----> 2
   *   schema  : a1.b1.c   ----> 3
   *
   *   metalake: a2        ----> 4
   *   catalog : a2.b2     ----> 5
   *   schema  : a2.b2.c   ----> 6
   *   schema  : a2.b2.c1  ----> 7
   *
   *   metalake: a1        ----> 1 means the name of metalake is a1 and the corresponding id is 1
   * </pre>
   *
   * Then we will store the name to id mapping as follows
   *
   * <pre>
   *  a1         --> 1
   * 	1/b1       --> 2
   * 	1/2/c      --> 3
   * 	a2         --> 4
   * 	4/b2       --> 5
   * 	4/5/c      --> 6
   * 	4/5/c1     --> 7
   * </pre>
   *
   * @param nameIdentifier name of a specific entity
   * @return key that maps to the id of a specific entity. See above, The key maybe like '4/5/c1'
   */
  public String generateKeyForMapping(NameIdentifier nameIdentifier) throws IOException {
    if (nameIdentifier.namespace().isEmpty()) {
      return nameIdentifier.name();
    }
    Namespace namespace = nameIdentifier.namespace();
    String name = nameIdentifier.name();

    long[] ids = new long[namespace.length()];
    for (int i = 0; i < ids.length; i++) {
      ids[i] =
          nameMappingService.getIdByName(
              concatIdAndName(ArrayUtils.subarray(ids, 0, i), namespace.level(i)));
    }

    return concatIdAndName(ids, name);
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    byte[] key = entityKeyEncoder.encode(ident, entityType, true);
    if (key == null) {
      throw new NoSuchEntityException(ident.toString());
    }

    byte[] value = backend.get(key);
    if (value == null) {
      throw new NoSuchEntityException(ident.toString());
    }
    return serDe.deserialize(value, e);
  }

  /**
   * Get key prefix of all sub-entities under a specific entities. For example, as a metalake will
   * start with `ml_{metalake_id}`, sub-entities under this metalake will have the prefix
   *
   * <pre>
   *   catalog: ca_{metalake_id}
   *   schema:  sc_{metalake_id}
   *   table:   ta_{metalake_id}
   * </pre>
   *
   * Why the sub-entities under this metalake start with those prefixes, please see {@link
   * KvEntityStore} java class doc.
   *
   * @param ident identifier of an entity
   * @param type type of entity
   * @return list of sub-entities prefix
   * @throws IOException if error occurs
   */
  private List<byte[]> getSubEntitiesPrefix(NameIdentifier ident, EntityType type)
      throws IOException {
    List<byte[]> prefixs = Lists.newArrayList();
    byte[] encode = entityKeyEncoder.encode(ident, type, true);
    switch (type) {
      case METALAKE:
        prefixs.add(replacePrefixTypeInfo(encode, CATALOG.getShortName()));
        prefixs.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
        prefixs.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        break;
      case CATALOG:
        prefixs.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
        prefixs.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        break;
      case SCHEMA:
        prefixs.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        break;
      case TABLE:
        break;
      default:
        LOG.warn("Currently unknown type: {}, please check it", type);
    }
    Collections.reverse(prefixs);
    return prefixs;
  }

  private byte[] replacePrefixTypeInfo(byte[] encode, String subTypePrefix) {
    byte[] result = new byte[encode.length];
    System.arraycopy(encode, 0, result, 0, encode.length);
    byte[] bytes = subTypePrefix.getBytes();
    result[0] = bytes[0];
    result[1] = bytes[1];

    return result;
  }

  @Override
  public boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade)
      throws IOException {
    if (!exists(ident, entityType)) {
      return false;
    }

    byte[] dataKey = entityKeyEncoder.encode(ident, entityType, true);
    List<byte[]> subEntityPrefix = getSubEntitiesPrefix(ident, entityType);
    if (subEntityPrefix.isEmpty()) {
      // has no sub-entities
      return backend.delete(dataKey);
    }

    byte[] directChild = Iterables.getLast(subEntityPrefix);
    byte[] endKey = Bytes.increment(Bytes.wrap(directChild)).get();
    List<Pair<byte[], byte[]>> kvs =
        backend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(directChild)
                .end(endKey)
                .startInclusive(true)
                .endInclusive(false)
                .limit(1)
                .build());

    if (!cascade && !kvs.isEmpty()) {
      throw new NonEmptyEntityException(
          String.format("Entity %s has sub-entities, you should remove sub-entities first", ident));
    }

    for (byte[] prefix : subEntityPrefix) {
      backend.deleteRange(
          new KvRangeScan.KvRangeScanBuilder()
              .start(prefix)
              .startInclusive(true)
              .end(Bytes.increment(Bytes.wrap(prefix)).get())
              .build());
    }

    return backend.delete(dataKey);
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException {
    return backend.executeInTransaction(executable);
  }

  @Override
  public void close() throws IOException {
    backend.close();
  }

  private static KvBackend createKvEntityBackend(Config config) {
    String backendName = config.get(ENTITY_KV_STORE);
    String className = KV_BACKENDS.getOrDefault(backendName, backendName);
    if (Objects.isNull(className)) {
      throw new RuntimeException("Unsupported backend type..." + backendName);
    }

    try {
      KvBackend kvBackend = (KvBackend) Class.forName(className).newInstance();
      kvBackend.initialize(config);
      return kvBackend;
    } catch (Exception e) {
      LOGGER.error("Failed to create and initialize KvBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize KvBackend by name: " + backendName, e);
    }
  }
}
