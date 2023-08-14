/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import static com.datastrato.graviton.Configs.DEFUALT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.storage.kv.KvBackend;
import com.datastrato.graviton.storage.kv.RocksDBKvBackend;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKvNameMappingService {

  public static final String ROCKS_DB_STORE_PATH = "/tmp/graviton_name_mapping";

  @BeforeEach
  @AfterEach
  public void cleanEnv() {
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testNameMappingService() throws IOException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);

    KvBackend backend = new RocksDBKvBackend();
    backend.initialize(config);
    NameMappingService nameMappingService = new KvNameMappingService(backend);

    // First test read and write
    Assertions.assertNull(nameMappingService.get("name1"));
    Long name1Id = nameMappingService.create("name1");
    Long name1IdRead = nameMappingService.get("name1");
    Assertions.assertEquals(name1Id, name1IdRead);

    Assertions.assertNull(nameMappingService.get("name2"));
    Long name2Id = nameMappingService.create("name2");
    Long name2IdRead = nameMappingService.get("name2");
    Assertions.assertEquals(name2Id, name2IdRead);

    Assertions.assertNotEquals(name1Id, name2Id);

    // Test update
    nameMappingService.update("name1", "name3");
    Long name3Id = nameMappingService.get("name3");
    Assertions.assertEquals(name1Id, name3Id);
    Assertions.assertNull(nameMappingService.get("name1"));

    // Test or create
    Assertions.assertNull(nameMappingService.get("name4"));
    Long name4Id = nameMappingService.getOrCreateId("name4");
    Assertions.assertNotEquals(name4Id, name1Id);

    // Test delete
    nameMappingService.delete("name4");
    Assertions.assertNull(nameMappingService.get("name4"));

    // Test execute in transaction
    Assertions.assertThrowsExactly(
        ArithmeticException.class,
        () ->
            nameMappingService.executeInTransaction(
                () -> {
                  nameMappingService.create("name5");
                  nameMappingService.create("name6");
                  // Mock exception
                  int d = 1 / 0;
                  nameMappingService.create("name7");
                  return null;
                }));

    Assertions.assertNull(nameMappingService.get("name5"));
    Assertions.assertNull(nameMappingService.get("name6"));

    Assertions.assertTrue(nameMappingService.getIdGenerator().nextId() > 0);
  }
}
