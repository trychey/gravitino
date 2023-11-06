/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.NameMappingService;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link KvNameMappingService} is an implementation that use {@link KvBackend} to store name to id
 * mapping.
 */
@ThreadSafe
public class KvNameMappingService implements NameMappingService {

  // TODO(yuqi) Make this configurable
  @VisibleForTesting final KvBackend backend;
  @VisibleForTesting final IdGenerator idGenerator = new RandomIdGenerator();

  // name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX = "name_".getBytes();

  // id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX = "id_".getBytes();

  public KvNameMappingService(KvBackend backend) {
    this.backend = backend;
  }

  @Override
  public Long getIdByName(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    byte[] idByte = backend.get(nameByte);
    return idByte == null ? null : ByteUtils.byteToLong(idByte);
  }

  private long bindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    long id = idGenerator.nextId();
    return backend.executeInTransaction(
        () -> {
          backend.put(nameByte, ByteUtils.longToByte(id), false);
          byte[] idByte = Bytes.concat(ID_PREFIX, ByteUtils.longToByte(id));
          backend.put(idByte, name.getBytes(), false);
          return id;
        });
  }

  @Override
  public boolean updateName(String oldName, String newName) throws IOException {
    return backend.executeInTransaction(
        () -> {
          byte[] nameByte = Bytes.concat(NAME_PREFIX, oldName.getBytes());
          byte[] oldIdValue = backend.get(nameByte);

          // Old mapping has been deleted, no need to do it;
          if (oldIdValue == null) {
            return false;
          }
          // Delete old name --> id mapping
          backend.delete(nameByte);

          backend.put(Bytes.concat(NAME_PREFIX, newName.getBytes()), oldIdValue, false);
          backend.put(Bytes.concat(ID_PREFIX, oldIdValue), newName.getBytes(), true);
          return true;
        });
  }

  @Override
  public boolean unbindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    return backend.delete(nameByte);
  }

  @Override
  public void close() throws Exception {
    backend.close();
  }

  @Override
  public long getOrCreateIdFromName(String name) throws IOException {
    Long id = getIdByName(name);
    if (id == null) {
      synchronized (this) {
        if ((id = getIdByName(name)) == null) {
          id = bindNameAndId(name);
        }
      }
    }

    return id;
  }
}
