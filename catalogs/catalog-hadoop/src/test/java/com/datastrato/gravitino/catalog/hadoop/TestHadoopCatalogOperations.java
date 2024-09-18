/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.catalog.hadoop.HadoopCatalogOperations.getStorageLocations;
import static com.datastrato.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.CHECK_UNIQUE_STORAGE_LOCATION_SCHEME;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestHadoopCatalogOperations {

  private static final String ROCKS_DB_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String UNFORMALIZED_TEST_ROOT_PATH =
      "/tmp/gravitino_test_catalog_" + UUID.randomUUID().toString().replace("-", "");

  private static final String TEST_ROOT_PATH = "file:" + UNFORMALIZED_TEST_ROOT_PATH;

  private static EntityStore store;

  private static IdGenerator idGenerator;

  @BeforeAll
  public static void setUp() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);

    Assertions.assertEquals(ROCKS_DB_STORE_PATH, config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    store.setSerDe(EntitySerDeFactory.createEntitySerDe(config));
    idGenerator = new RandomIdGenerator();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    store.close();
    FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    new Path(TEST_ROOT_PATH)
        .getFileSystem(new Configuration())
        .delete(new Path(TEST_ROOT_PATH), true);
  }

  @Test
  public void testHadoopCatalogConfiguration() {
    Map<String, String> emptyProps = Maps.newHashMap();
    HadoopCatalogOperations ops = new HadoopCatalogOperations(store);
    ops.initialize(emptyProps, null);
    Configuration conf = ops.hadoopConf;
    String value = conf.get("fs.defaultFS");
    Assertions.assertEquals("file:///", value);

    emptyProps.put(CATALOG_BYPASS_PREFIX + "fs.defaultFS", "hdfs://localhost:9000");
    ops.initialize(emptyProps, null);
    Configuration conf1 = ops.hadoopConf;
    String value1 = conf1.get("fs.defaultFS");
    Assertions.assertEquals("hdfs://localhost:9000", value1);

    Assertions.assertFalse(ops.catalogStorageLocation.isPresent());

    emptyProps.put(HadoopCatalogPropertiesMetadata.LOCATION, "file:///tmp/catalog");
    ops.initialize(emptyProps, null);
    Assertions.assertTrue(ops.catalogStorageLocation.isPresent());
    Path expectedPath = new Path("file:///tmp/catalog");
    Assertions.assertEquals(expectedPath, ops.catalogStorageLocation.get());
  }

  @Test
  public void testCreateSchemaWithNoLocation() throws IOException {
    String name = "schema11";
    String comment = "comment11";
    Schema schema = createSchema(name, comment, null, null);
    Assertions.assertEquals(name, schema.name());
    Assertions.assertEquals(comment, schema.comment());

    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class, () -> createSchema(name, comment, null, null));
    Assertions.assertEquals("Schema m1.c1.schema11 already exists", exception.getMessage());
  }

  @Test
  public void testCreateSchemaWithCatalogLocation() throws IOException {
    String name = "schema12";
    String comment = "comment12";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog12";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath = new Path(catalogPath, name);
    FileSystem fs = schemaPath.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath));
    Assertions.assertTrue(fs.getFileStatus(schemaPath).isDirectory());
    Assertions.assertTrue(fs.listStatus(schemaPath).length == 0);
  }

  @Test
  public void testCreateSchemaWithSchemaLocation() throws IOException {
    String name = "schema13";
    String comment = "comment13";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog13";
    String schemaPath = catalogPath + "/" + name;
    Schema schema = createSchema(name, comment, null, schemaPath);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath1 = new Path(schemaPath);
    FileSystem fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath1));
    Assertions.assertTrue(fs.getFileStatus(schemaPath1).isDirectory());
    Assertions.assertTrue(fs.listStatus(schemaPath1).length == 0);
  }

  @Test
  public void testCreateSchemaWithCatalogAndSchemaLocation() throws IOException {
    String name = "schema14";
    String comment = "comment14";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog14";
    String schemaPath = TEST_ROOT_PATH + "/" + "schema14";
    Schema schema = createSchema(name, comment, catalogPath, schemaPath);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath1 = new Path(schemaPath);
    FileSystem fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath1));
    Assertions.assertTrue(fs.getFileStatus(schemaPath1).isDirectory());
    Assertions.assertTrue(fs.listStatus(schemaPath1).length == 0);

    Assertions.assertFalse(fs.exists(new Path(catalogPath)));
    Assertions.assertFalse(fs.exists(new Path(catalogPath, name)));
  }

  @Test
  public void testLoadSchema() throws IOException {
    String name = "schema15";
    String comment = "comment15";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog15";
    Schema schema = createSchema(name, comment, catalogPath, null);
    NameIdentifier schema16 = NameIdentifier.ofSchema("m1", "c1", "schema16");

    Assertions.assertEquals(name, schema.name());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      Schema schema1 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(props.containsKey(ID_KEY));

      Throwable exception =
          Assertions.assertThrows(NoSuchSchemaException.class, () -> ops.loadSchema(schema16));
      Assertions.assertEquals("Schema m1.c1.schema16 does not exist", exception.getMessage());
    }
  }

  @Test
  public void testListSchema() throws IOException {
    String name = "schema17";
    String comment = "comment17";
    String name1 = "schema18";
    String comment1 = "comment18";
    createSchema(name, comment, null, null);
    createSchema(name1, comment1, null, null);

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      Set<NameIdentifier> idents =
          Arrays.stream(ops.listSchemas(Namespace.of("m1", "c1"))).collect(Collectors.toSet());
      Assertions.assertTrue(idents.size() >= 2);
      Assertions.assertTrue(idents.contains(NameIdentifier.ofSchema("m1", "c1", name)));
      Assertions.assertTrue(idents.contains(NameIdentifier.ofSchema("m1", "c1", name1)));
    }
  }

  @Test
  public void testAlterSchema() throws IOException {
    String name = "schema19";
    String comment = "comment19";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog19";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      Schema schema1 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(props.containsKey(ID_KEY));

      String newKey = "k1";
      String newValue = "v1";
      SchemaChange setProperty = SchemaChange.setProperty(newKey, newValue);
      Schema schema2 = ops.alterSchema(NameIdentifier.ofSchema("m1", "c1", name), setProperty);
      Assertions.assertEquals(name, schema2.name());
      Assertions.assertEquals(comment, schema2.comment());
      Map<String, String> props2 = schema2.properties();
      Assertions.assertTrue(props2.containsKey(newKey));
      Assertions.assertEquals(newValue, props2.get(newKey));

      Schema schema3 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Map<String, String> props3 = schema3.properties();
      Assertions.assertTrue(props3.containsKey(newKey));
      Assertions.assertEquals(newValue, props3.get(newKey));

      SchemaChange removeProperty = SchemaChange.removeProperty(newKey);
      Schema schema4 = ops.alterSchema(NameIdentifier.ofSchema("m1", "c1", name), removeProperty);
      Assertions.assertEquals(name, schema4.name());
      Assertions.assertEquals(comment, schema4.comment());
      Map<String, String> props4 = schema4.properties();
      Assertions.assertFalse(props4.containsKey(newKey));

      Schema schema5 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Map<String, String> props5 = schema5.properties();
      Assertions.assertFalse(props5.containsKey(newKey));
    }
  }

  @Test
  public void testDropSchema() throws IOException {
    String name = "schema20";
    String comment = "comment20";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog20";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());
    NameIdentifier id = NameIdentifier.ofSchema("m1", "c1", name);

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(ImmutableMap.of(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath), null);
      Schema schema1 = ops.loadSchema(id);
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(props.containsKey(ID_KEY));

      ops.dropSchema(id, false);

      Path schemaPath = new Path(new Path(catalogPath), name);
      FileSystem fs = schemaPath.getFileSystem(new Configuration());
      Assertions.assertFalse(fs.exists(schemaPath));

      // Test drop non-empty schema with cascade = false
      Path subPath = new Path(schemaPath, "test1");
      fs.mkdirs(subPath);
      Assertions.assertTrue(fs.exists(subPath));

      Throwable exception1 =
          Assertions.assertThrows(NonEmptySchemaException.class, () -> ops.dropSchema(id, false));
      Assertions.assertEquals(
          "Schema m1.c1.schema20 with location " + schemaPath + " is not empty",
          exception1.getMessage());

      // Test drop non-empty schema with cascade = true
      ops.dropSchema(id, true);
      Assertions.assertFalse(fs.exists(schemaPath));

      // Test drop empty schema
      Assertions.assertFalse(ops.dropSchema(id, true));
      Assertions.assertFalse(ops.dropSchema(id, false));
    }
  }

  @ParameterizedTest
  @MethodSource("locationArguments")
  public void testCreateLoadAndDeleteFilesetWithLocations(
      String name,
      Fileset.Type type,
      String catalogPath,
      String schemaPath,
      String storageLocation,
      String expectedPrimaryStorageLocation,
      Map<String, String> filesetProps,
      Map<String, String> expectedFilesetProps)
      throws IOException {
    String schemaName = "s1_" + name;
    String comment = "comment_s1";

    List<String> backupStorageLocations =
        filesetProps.entrySet().stream()
            .filter(
                entry -> entry.getKey().startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());

    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put(CATALOG_BYPASS_PREFIX + CHECK_UNIQUE_STORAGE_LOCATION_SCHEME, "false");
    if (catalogPath != null) {
      catalogProps.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifier.ofSchema("m1", "c1", schemaName);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, null);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(schemaName, comment, catalogPath, schemaPath);
      }
      if (type == Fileset.Type.EXTERNAL) {
        // we can skip this step for backup storage locations since the catalog will create them
        // automatically
        Path storePath = new Path(storageLocation);
        try (FileSystem fs = storePath.getFileSystem(new Configuration())) {
          fs.mkdirs(storePath);
        }
      }
      Fileset fileset =
          createFileset(
              name, schemaName, "comment", type, catalogPath, storageLocation, filesetProps);

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());
      Assertions.assertEquals(expectedPrimaryStorageLocation, fileset.storageLocation());
      Map<String, String> properties = fileset.properties();
      expectedFilesetProps.forEach((k, v) -> Assertions.assertEquals(v, properties.get(k)));
      Path primaryStorageLocationPath = new Path(fileset.storageLocation());
      FileSystem fs = primaryStorageLocationPath.getFileSystem(new Configuration());
      Assertions.assertTrue(fs.exists(primaryStorageLocationPath));
      backupStorageLocations.forEach(
          location -> {
            Path expectedBackupStorageLocationPath = new Path(location);
            try {
              Assertions.assertTrue(fs.exists(expectedBackupStorageLocationPath));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      // Test load
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Fileset loadedFileset = ops.loadFileset(filesetIdent);
      Assertions.assertEquals(name, loadedFileset.name());
      Assertions.assertEquals(type, loadedFileset.type());
      Assertions.assertEquals("comment", loadedFileset.comment());
      Assertions.assertEquals(expectedPrimaryStorageLocation, loadedFileset.storageLocation());
      Map<String, String> loadedFilesetProperties = loadedFileset.properties();
      expectedFilesetProps.forEach(
          (k, v) -> Assertions.assertEquals(v, loadedFilesetProperties.get(k)));
      primaryStorageLocationPath = new Path(loadedFileset.storageLocation());
      Assertions.assertTrue(fs.exists(primaryStorageLocationPath));
      backupStorageLocations.forEach(
          location -> {
            Path expectedBackupStorageLocationPath = new Path(location);
            try {
              Assertions.assertTrue(fs.exists(expectedBackupStorageLocationPath));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      // Test drop
      ops.dropFileset(filesetIdent);
      Path expectedPath = new Path(expectedPrimaryStorageLocation);
      if (type == Fileset.Type.MANAGED) {
        Assertions.assertFalse(fs.exists(expectedPath));
        backupStorageLocations.forEach(
            location -> {
              Path expectedBackupStorageLocationPath = new Path(location);
              try {
                Assertions.assertFalse(fs.exists(expectedBackupStorageLocationPath));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      } else {
        Assertions.assertTrue(fs.exists(expectedPath));
        backupStorageLocations.forEach(
            location -> {
              Path expectedBackupStorageLocationPath = new Path(location);
              try {
                Assertions.assertTrue(fs.exists(expectedBackupStorageLocationPath));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }
  }

  @Test
  public void testCreateFilesetWithExceptions() throws IOException {
    String schemaName = "schema22";
    String comment = "comment22";
    createSchema(schemaName, comment, null, null);
    String name = "fileset22";
    NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

    // If neither catalog location, nor schema location and storageLocation is specified.
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createFileset(
                    name,
                    schemaName,
                    comment,
                    Fileset.Type.MANAGED,
                    null,
                    null,
                    ImmutableMap.of()));
    Assertions.assertEquals(
        "Storage location must be set for fileset: " + filesetIdent, exception.getMessage());
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      Throwable e =
          Assertions.assertThrows(
              NoSuchFilesetException.class, () -> ops.loadFileset(filesetIdent));
      Assertions.assertEquals("Fileset m1.c1.schema22.fileset22 does not exist", e.getMessage());
    }

    // For external fileset, if storageLocation is not specified.
    Throwable exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createFileset(
                    name,
                    schemaName,
                    comment,
                    Fileset.Type.EXTERNAL,
                    null,
                    null,
                    ImmutableMap.of()));
    Assertions.assertEquals(
        "Storage location must be set for external fileset " + filesetIdent,
        exception1.getMessage());
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      Throwable e =
          Assertions.assertThrows(
              NoSuchFilesetException.class, () -> ops.loadFileset(filesetIdent));
      Assertions.assertEquals("Fileset " + filesetIdent + " does not exist", e.getMessage());
    }
  }

  @Test
  public void testListFilesets() throws IOException {
    String schemaName = "schema23";
    String comment = "comment23";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);
    String[] filesets = new String[] {"fileset23_1", "fileset23_2", "fileset23_3"};
    String catalogName = "c1";

    for (String fileset : filesets) {
      String storageLocation =
          TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + fileset;
      createFileset(
          fileset,
          schemaName,
          comment,
          Fileset.Type.MANAGED,
          null,
          storageLocation,
          ImmutableMap.of());
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      Set<NameIdentifier> idents =
          Arrays.stream(ops.listFilesets(Namespace.of("m1", "c1", schemaName)))
              .collect(Collectors.toSet());
      Assertions.assertTrue(idents.size() >= 3);
      for (String fileset : filesets) {
        Assertions.assertTrue(idents.contains(NameIdentifier.of("m1", "c1", schemaName, fileset)));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("testRenameArguments")
  public void testRenameFileset(
      String name,
      String newName,
      Fileset.Type type,
      String catalogPath,
      String schemaPath,
      String storageLocation,
      String expect)
      throws IOException {
    String schemaName = "s24_" + name;
    String comment = "comment_s24";
    Map<String, String> catalogProps = Maps.newHashMap();
    if (catalogPath != null) {
      catalogProps.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifier.ofSchema("m1", "c1", schemaName);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, null);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(schemaName, comment, catalogPath, schemaPath);
      }
      Path storePath = new Path(storageLocation);
      try (FileSystem fs = storePath.getFileSystem(new Configuration())) {
        fs.mkdirs(storePath);
      }
      Fileset fileset =
          createFileset(
              name, schemaName, "comment", type, catalogPath, storageLocation, ImmutableMap.of());

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Fileset loadedFileset = ops.loadFileset(filesetIdent);
      Assertions.assertEquals(name, loadedFileset.name());
      Assertions.assertEquals(type, loadedFileset.type());
      Assertions.assertEquals("comment", loadedFileset.comment());

      Fileset renamedFileset = ops.alterFileset(filesetIdent, FilesetChange.rename(newName));
      Assertions.assertEquals(newName, renamedFileset.name());
      Assertions.assertEquals(type, renamedFileset.type());
      Assertions.assertEquals("comment", renamedFileset.comment());
      Assertions.assertEquals(expect, renamedFileset.storageLocation());

      Fileset loadedRenamedFileset =
          ops.loadFileset(NameIdentifier.of("m1", "c1", schemaName, newName));
      Assertions.assertEquals(newName, loadedRenamedFileset.name());
      Assertions.assertEquals(type, loadedRenamedFileset.type());
      Assertions.assertEquals("comment", loadedRenamedFileset.comment());
      Assertions.assertEquals(expect, loadedRenamedFileset.storageLocation());
    }
  }

  @Test
  public void testAlterFilesetProperties() throws IOException {
    String schemaName = "schema25";
    String comment = "comment25";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String catalogName = "c1";
    String name = "fileset25";
    String storageLocation = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name;
    Fileset fileset =
        createFileset(
            name,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            storageLocation,
            ImmutableMap.of());

    FilesetChange change1 = FilesetChange.setProperty("k1", "v1");
    FilesetChange change2 = FilesetChange.removeProperty("k1");

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals("comment25", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
      Map<String, String> props1 = fileset1.properties();
      Assertions.assertTrue(props1.containsKey("k1"));
      Assertions.assertEquals("v1", props1.get("k1"));

      Fileset fileset2 = ops.alterFileset(filesetIdent, change2);
      Assertions.assertEquals(name, fileset2.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset2.type());
      Assertions.assertEquals("comment25", fileset2.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset2.storageLocation());
      Map<String, String> props2 = fileset2.properties();
      Assertions.assertFalse(props2.containsKey("k1"));
    }
  }

  @Test
  public void testAlterFilesetOwner() throws IOException {
    String schemaName = "schema_1";
    String comment = "comment_1";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String catalogName = "c1";
    String name = "fileset_1";
    String storageLocation = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name;
    Fileset fileset =
        createFileset(
            name,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            storageLocation,
            ImmutableMap.of());

    // add owner
    FilesetChange change1 = FilesetChange.setProperty(FilesetProperties.OWNER_KEY, "test_owner");
    // remove owner
    FilesetChange change2 = FilesetChange.removeProperty(FilesetProperties.OWNER_KEY);
    // add other properties to check if owner property is lost
    FilesetChange change3 = FilesetChange.setProperty("k1", "v1");
    // update owner
    FilesetChange change4 = FilesetChange.setProperty(FilesetProperties.OWNER_KEY, "test_owner_1");

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals("comment_1", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
      Map<String, String> props1 = fileset1.properties();
      Assertions.assertTrue(props1.containsKey(FilesetProperties.OWNER_KEY));
      Assertions.assertEquals("test_owner", props1.get(FilesetProperties.OWNER_KEY));

      Assertions.assertThrows(
          UnsupportedOperationException.class, () -> ops.alterFileset(filesetIdent, change2));

      Fileset fileset2 = ops.alterFileset(filesetIdent, change3);
      Assertions.assertEquals(name, fileset2.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset2.type());
      Assertions.assertEquals("comment_1", fileset2.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset2.storageLocation());
      Map<String, String> props2 = fileset2.properties();
      Assertions.assertTrue(props2.containsKey(FilesetProperties.OWNER_KEY));
      Assertions.assertEquals("test_owner", props2.get(FilesetProperties.OWNER_KEY));
      Assertions.assertTrue(props2.containsKey("k1"));
      Assertions.assertEquals("v1", props2.get("k1"));

      Fileset fileset3 = ops.alterFileset(filesetIdent, change4);
      Assertions.assertEquals(name, fileset3.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset3.type());
      Assertions.assertEquals("comment_1", fileset3.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset3.storageLocation());
      Map<String, String> props3 = fileset3.properties();
      Assertions.assertTrue(props3.containsKey(FilesetProperties.OWNER_KEY));
      Assertions.assertEquals("test_owner_1", props3.get(FilesetProperties.OWNER_KEY));
    }
  }

  @Test
  public void testFormalizePath() throws IOException {

    String[] paths =
        new String[] {
          "tmp/catalog",
          "/tmp/catalog",
          "file:/tmp/catalog",
          "file:///tmp/catalog",
          "hdfs://localhost:9000/tmp/catalog",
          "s3://bucket/tmp/catalog",
          "gs://bucket/tmp/catalog"
        };

    String[] expected =
        new String[] {
          "file:" + Paths.get("").toAbsolutePath() + "/tmp/catalog",
          "file:/tmp/catalog",
          "file:/tmp/catalog",
          "file:/tmp/catalog",
          "hdfs://localhost:9000/tmp/catalog",
          "s3://bucket/tmp/catalog",
          "gs://bucket/tmp/catalog"
        };

    for (int i = 0; i < paths.length; i++) {
      Path actual = HadoopCatalogOperations.formalizePath(new Path(paths[i]), new Configuration());
      Assertions.assertEquals(expected[i], actual.toString());
    }
  }

  @Test
  public void testUpdateFilesetComment() throws IOException {
    String schemaName = "schema26";
    String comment = "comment26";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String name = "fileset26";
    String catalogName = "c1";
    String storageLocation = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name;
    Fileset fileset =
        createFileset(
            name,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            storageLocation,
            ImmutableMap.of());

    FilesetChange change1 = FilesetChange.updateComment("comment26_new");
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals("comment26_new", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
    }
  }

  @Test
  public void testFilesetStorageLocation() throws IOException {
    String schemaName = "schema_z1";
    String comment = "comment26";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      Map<String, String> catalogProps = Maps.newHashMap();
      ops.initialize(catalogProps, null);

      // test invalid managed storage location with oss
      String name = "fileset_z1";
      String catalogName = "c1";
      String storageLocation = "oss://localhost/" + catalogName + "/" + schemaName + "/" + name;
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Map<String, String> filesetProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      Assertions.assertThrows(
          RuntimeException.class,
          () ->
              ops.createFileset(
                  filesetIdent,
                  comment,
                  Fileset.Type.MANAGED,
                  storageLocation,
                  Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps))));

      // test invalid external storage location with oss
      Assertions.assertThrows(
          RuntimeException.class,
          () ->
              ops.createFileset(
                  filesetIdent,
                  comment,
                  Fileset.Type.EXTERNAL,
                  storageLocation,
                  Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps))));

      // test valid managed storage location with file
      String name1 = "fileset_z11";
      String storageLocation1 = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name1;
      NameIdentifier filesetIdent1 = NameIdentifier.of("m1", "c1", schemaName, name1);
      Map<String, String> filesetProps1 = Maps.newHashMap();
      StringIdentifier stringId1 = StringIdentifier.fromId(idGenerator.nextId());
      ops.createFileset(
          filesetIdent1,
          comment,
          Fileset.Type.MANAGED,
          storageLocation1,
          Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId1, filesetProps1)));
      Assertions.assertNotNull(ops.loadFileset(filesetIdent1));

      // test valid external storage location with file
      String name2 = "fileset_z12";
      String storageLocation2 = TEST_ROOT_PATH + "/" + catalogName + "/" + name2;
      Path storageLocation2Path = new Path(storageLocation2);
      try (FileSystem fs = storageLocation2Path.getFileSystem(new Configuration())) {
        fs.mkdirs(storageLocation2Path);
      }
      NameIdentifier filesetIdent2 = NameIdentifier.of("m1", "c1", schemaName, name2);
      Map<String, String> filesetProps2 = Maps.newHashMap();
      StringIdentifier stringId2 = StringIdentifier.fromId(idGenerator.nextId());
      ops.createFileset(
          filesetIdent2,
          comment,
          Fileset.Type.EXTERNAL,
          storageLocation2,
          Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId2, filesetProps2)));
      Assertions.assertNotNull(ops.loadFileset(filesetIdent2));
    }
  }

  @Test
  public void testRemoveFilesetComment() throws IOException {
    String schemaName = "schema27";
    String comment = "comment27";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String name = "fileset27";
    String catalogName = "c1";
    String storageLocation = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name;
    Fileset fileset =
        createFileset(
            name,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            storageLocation,
            ImmutableMap.of());

    FilesetChange change1 = FilesetChange.removeComment();
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertNull(fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
    }
  }

  @Test
  public void testAlterFilesetPropertiesWithException() throws IOException {
    String schemaName = "test_schema";
    String comment = "test_comment";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String catalogName = "c1";
    String name = "test_fileset";
    String storageLocation = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name;
    Fileset fileset =
        createFileset(
            name,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            storageLocation,
            ImmutableMap.of());

    FilesetChange change1 = FilesetChange.setProperty("k1", "v1");
    FilesetChange change2 = FilesetChange.removeProperty("k1");
    FilesetChange change3 =
        FilesetChange.setProperty(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, storageLocation + "_bak_1");
    FilesetChange change4 =
        FilesetChange.removeProperty(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1);
    FilesetChange change5 =
        FilesetChange.addBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + "aaaa", storageLocation + "_bak_aaaa");

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals("test_comment", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
      Map<String, String> props1 = fileset1.properties();
      Assertions.assertTrue(props1.containsKey("k1"));
      Assertions.assertEquals("v1", props1.get("k1"));

      Fileset fileset2 = ops.alterFileset(filesetIdent, change2);
      Assertions.assertEquals(name, fileset2.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset2.type());
      Assertions.assertEquals("test_comment", fileset2.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset2.storageLocation());
      Map<String, String> props2 = fileset2.properties();
      Assertions.assertFalse(props2.containsKey("k1"));

      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class, () -> ops.alterFileset(filesetIdent, change3));
      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class, () -> ops.alterFileset(filesetIdent, change4));
      Assertions.assertThrowsExactly(
          IllegalArgumentException.class, () -> ops.alterFileset(filesetIdent, change5));
    }
  }

  @ParameterizedTest
  @MethodSource("filesetTypes")
  public void testStorageLocationFilesetChanges(Fileset.Type type) throws IOException {
    String schemaName = "test_schema_" + type.name();
    String comment = "test_comment";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String catalogName = "c1";
    String name = "test_fileset_" + type.name();
    String storageLocation = TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + name;
    String backupLoc1 = storageLocation + "_bak_1";
    String backupLoc2 = storageLocation + "_bak_2";
    String backupLoc1_new = storageLocation + "_bak_1_new";
    String backupLoc2_new = storageLocation + "_bak_2_new";
    String newStorageLocation = storageLocation + "_new";

    FilesetChange change1 =
        FilesetChange.addBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupLoc1);
    FilesetChange change2 =
        FilesetChange.addBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2, backupLoc2);
    FilesetChange change3 =
        FilesetChange.updateBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupLoc1_new);
    FilesetChange change4 =
        FilesetChange.updateBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2, backupLoc2_new);
    FilesetChange change5 =
        FilesetChange.switchBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1);
    FilesetChange change6 =
        FilesetChange.switchPrimaryAndBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1);
    FilesetChange change7 =
        FilesetChange.removeBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1);
    FilesetChange change8 =
        FilesetChange.removeBackupStorageLocation(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2);
    FilesetChange change9 = FilesetChange.updatePrimaryStorageLocation(newStorageLocation);
    FilesetChange change10 = FilesetChange.updatePrimaryStorageLocation(storageLocation);

    if (type == Fileset.Type.EXTERNAL) {
      Arrays.asList(
              new Path(storageLocation),
              new Path(backupLoc1),
              new Path(backupLoc2),
              new Path(backupLoc1_new),
              new Path(backupLoc2_new),
              new Path(newStorageLocation))
          .forEach(this::createStorageLocation);
    }

    Fileset fileset =
        createFileset(name, schemaName, comment, type, null, storageLocation, ImmutableMap.of());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      HashMap<String, String> props = Maps.newHashMap();
      props.put(CATALOG_BYPASS_PREFIX + CHECK_UNIQUE_STORAGE_LOCATION_SCHEME, "false");
      ops.initialize(props, null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(type, fileset1.type());
      Assertions.assertEquals("test_comment", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
      Map<String, String> props1 = fileset1.properties();
      Assertions.assertTrue(props1.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          backupLoc1, props1.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));

      Fileset fileset2 = ops.alterFileset(filesetIdent, change2);
      Assertions.assertEquals(name, fileset2.name());
      Assertions.assertEquals(type, fileset2.type());
      Assertions.assertEquals("test_comment", fileset2.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset2.storageLocation());
      Map<String, String> props2 = fileset2.properties();
      Assertions.assertTrue(props2.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          backupLoc1, props2.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertTrue(props2.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      Assertions.assertEquals(
          backupLoc2, props2.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

      Fileset fileset3 = ops.alterFileset(filesetIdent, change3);
      Assertions.assertEquals(name, fileset3.name());
      Assertions.assertEquals(type, fileset3.type());
      Assertions.assertEquals("test_comment", fileset3.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset3.storageLocation());
      Map<String, String> props3 = fileset3.properties();
      Assertions.assertTrue(props3.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          backupLoc1_new, props3.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertTrue(props3.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      Assertions.assertEquals(
          backupLoc2, props3.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

      Fileset fileset4 = ops.alterFileset(filesetIdent, change4);
      Assertions.assertEquals(name, fileset4.name());
      Assertions.assertEquals(type, fileset4.type());
      Assertions.assertEquals("test_comment", fileset4.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset4.storageLocation());
      Map<String, String> props4 = fileset4.properties();
      Assertions.assertTrue(props4.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          backupLoc1_new, props4.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertTrue(props4.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      Assertions.assertEquals(
          backupLoc2_new, props4.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      IllegalArgumentException exception =
          Assertions.assertThrowsExactly(
              IllegalArgumentException.class, () -> ops.alterFileset(filesetIdent, change4));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains("The new backup storage location should be different from the old one"));

      Fileset fileset5 = ops.alterFileset(filesetIdent, change5);
      Assertions.assertEquals(name, fileset5.name());
      Assertions.assertEquals(type, fileset5.type());
      Assertions.assertEquals("test_comment", fileset5.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset5.storageLocation());
      Map<String, String> props5 = fileset5.properties();
      Assertions.assertTrue(props5.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          backupLoc2_new, props5.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertTrue(props5.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      Assertions.assertEquals(
          backupLoc1_new, props5.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

      Fileset fileset6 = ops.alterFileset(filesetIdent, change6);
      Assertions.assertEquals(name, fileset6.name());
      Assertions.assertEquals(type, fileset6.type());
      Assertions.assertEquals("test_comment", fileset6.comment());
      Assertions.assertEquals(backupLoc2_new, fileset6.storageLocation());
      Map<String, String> props6 = fileset6.properties();
      Assertions.assertTrue(props6.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          fileset.storageLocation(), props6.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertTrue(props6.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      Assertions.assertEquals(
          backupLoc1_new, props6.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

      Fileset fileset7 = ops.alterFileset(filesetIdent, change7);
      Assertions.assertEquals(name, fileset7.name());
      Assertions.assertEquals(type, fileset7.type());
      Assertions.assertEquals("test_comment", fileset7.comment());
      Assertions.assertEquals(backupLoc2_new, fileset7.storageLocation());
      Map<String, String> props7 = fileset7.properties();
      Assertions.assertFalse(props7.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertTrue(props7.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
      Assertions.assertEquals(
          backupLoc1_new, props7.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

      Fileset fileset8 = ops.alterFileset(filesetIdent, change8);
      Assertions.assertEquals(name, fileset8.name());
      Assertions.assertEquals(type, fileset8.type());
      Assertions.assertEquals("test_comment", fileset8.comment());
      Assertions.assertEquals(backupLoc2_new, fileset8.storageLocation());
      Map<String, String> props8 = fileset8.properties();
      Assertions.assertFalse(props8.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertFalse(props8.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

      Fileset fileset9 = ops.alterFileset(filesetIdent, change9);
      Assertions.assertEquals(name, fileset9.name());
      Assertions.assertEquals(type, fileset9.type());
      Assertions.assertEquals("test_comment", fileset9.comment());
      Assertions.assertEquals(newStorageLocation, fileset9.storageLocation());
      ops.alterFileset(filesetIdent, change10);
      exception =
          Assertions.assertThrowsExactly(
              IllegalArgumentException.class, () -> ops.alterFileset(filesetIdent, change10));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains("The new primary storage location should be different from the old one"));

      Fileset fileset10 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset10.name());
      Assertions.assertEquals(type, fileset10.type());
      Assertions.assertEquals("test_comment", fileset10.comment());
      Assertions.assertEquals(storageLocation, fileset10.storageLocation());
      Map<String, String> props10 = fileset10.properties();
      Assertions.assertTrue(props10.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
      Assertions.assertEquals(
          backupLoc1, props10.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));

      createStorageLocation(new Path(backupLoc1 + "/subDir"));
      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class, () -> ops.alterFileset(filesetIdent, change3));
      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class, () -> ops.alterFileset(filesetIdent, change7));

      createStorageLocation(new Path(storageLocation + "/subDir"));
      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class, () -> ops.alterFileset(filesetIdent, change9));
    }
  }

  @Test
  public void testCreateMultipleDirectories() throws IOException {
    // test create a new directory when its parent dir does not exist.
    String catalogLocation = TEST_ROOT_PATH + "/catalog1";
    String filesetLocation = TEST_ROOT_PATH + "/catalog1/db1/fileset1";

    Path catalogPath = new Path(catalogLocation);
    Path filesetPath = new Path(filesetLocation);
    FileSystem fs = catalogPath.getFileSystem(new Configuration());
    Assertions.assertFalse(fs.exists(catalogPath));
    Assertions.assertTrue(fs.mkdirs(filesetPath));
    Assertions.assertTrue(fs.exists(filesetPath));
  }

  static Object[][] validFilesetContextEnvProvider() {
    return new Object[][] {
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.OPEN,
        "/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.MKDIRS,
        "",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.CREATE,
        "test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.RENAME,
        "/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.CREATE,
        "/test/.temp/qqq/zzz/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.OPEN,
        "/date=20240501/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.CREATE,
        "date=20240501/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.RENAME,
        "/date=20240501/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.RENAME,
        "/date=20240501/.temp/zzz/qqq/ddd/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3")
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.OPEN,
        "year=2024/month=06/day=01/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5")
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.CREATE,
        "/year=2024/month=06/day=01/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5")
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.CREATE,
        "/year=2024/month=06/day=01/.temp/xxx/qqq/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5")
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.RENAME,
        "/year=2024/month=06/day=01/test/t.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5")
      }
    };
  }

  static Object[][] invalidFilesetContextEnvProvider() {
    return new Object[][] {
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.CREATE,
        // test over max dir level
        "/test/t/zz/xx/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.RENAME,
        // test sub path is `/`
        "/",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.RENAME,
        // test over max dir level
        "test/t/zz/xx/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.RENAME,
        // test empty sub path
        "",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.ANY,
        FilesetDataOperation.RENAME,
        "/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        // test rename with mount a single file
        true
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.CREATE,
        // test over max dir level
        "/date=20240506/test/t/zz/xx/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.RENAME,
        // test sub path is `/`
        "/",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.DATE_WITH_STRING,
        FilesetDataOperation.RENAME,
        // test over max dir level
        "date=20240506/test/t/zz/xx/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "3"),
        false
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.CREATE,
        // test over max dir level
        "/year=2024/month=06/day=01/test/t/zz/xx/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5"),
        false
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.RENAME,
        // test sub dir is `/`
        "/",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5"),
        false
      },
      {
        FilesetPrefixPattern.YEAR_MONTH_DAY,
        FilesetDataOperation.RENAME,
        // test over max dir level
        "year=2024/month=06/day=01/test/t/zz/xx/q.parquet",
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.YEAR_MONTH_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "5"),
        false
      }
    };
  }

  @ParameterizedTest
  @MethodSource("validFilesetContextEnvProvider")
  public void testGetFilesetContext(
      FilesetPrefixPattern pattern,
      FilesetDataOperation operation,
      String subPath,
      Map<String, String> properties)
      throws IOException {
    String schemaName =
        String.format(
            "schema_get_ctx_%s_prefix_%s",
            pattern.name(), UUID.randomUUID().toString().replace("-", ""));
    String comment = "comment1024";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);
    String catalogName = "c1";
    String filesetName =
        String.format(
            "fileset_get_ctx_%s_prefix_%s",
            pattern.name(), UUID.randomUUID().toString().replace("-", ""));
    String filesetLocation =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName;
    Fileset fileset =
        createFileset(
            filesetName,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            filesetLocation,
            properties);

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);
      // test operation valid
      BaseFilesetDataOperationCtx ctx =
          BaseFilesetDataOperationCtx.builder()
              .withSubPath(subPath)
              .withOperation(operation)
              .withClientType(ClientType.HADOOP_GVFS)
              .withIp("127.0.0.1")
              .withSourceEngineType(SourceEngineType.UNKNOWN)
              .withAppId("application_1_1")
              .build();
      FilesetContext context1 = ops.getFilesetContext(filesetIdent, ctx);

      Assertions.assertEquals(filesetName, context1.fileset().name());
      Assertions.assertEquals(Fileset.Type.MANAGED, context1.fileset().type());
      Assertions.assertEquals("comment1024", context1.fileset().comment());
      Assertions.assertEquals(fileset.storageLocation(), context1.fileset().storageLocation());
      if (StringUtils.isBlank(subPath)) {
        Assertions.assertEquals(context1.fileset().storageLocation(), context1.actualPaths()[0]);
      } else {
        Assertions.assertEquals(
            subPath.startsWith("/")
                ? String.format("%s%s", context1.fileset().storageLocation(), subPath)
                : String.format("%s/%s", context1.fileset().storageLocation(), subPath),
            context1.actualPaths()[0]);
      }
    }

    String filesetName1 =
        String.format(
            "fileset_get_ctx_%s_prefix_%s",
            pattern.name(), UUID.randomUUID().toString().replace("-", ""));
    String filesetLocation1 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName1 + "/";
    Fileset fileset1 =
        createFileset(
            filesetName1,
            schemaName,
            comment,
            Fileset.Type.MANAGED,
            null,
            filesetLocation1,
            properties);

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName1);
      // test operation valid
      BaseFilesetDataOperationCtx ctx =
          BaseFilesetDataOperationCtx.builder()
              .withSubPath(subPath)
              .withOperation(operation)
              .withClientType(ClientType.HADOOP_GVFS)
              .withIp("127.0.0.1")
              .withSourceEngineType(SourceEngineType.UNKNOWN)
              .withAppId("application_1_1")
              .build();
      FilesetContext context1 = ops.getFilesetContext(filesetIdent, ctx);

      Assertions.assertEquals(filesetName1, context1.fileset().name());
      Assertions.assertEquals(Fileset.Type.MANAGED, context1.fileset().type());
      Assertions.assertEquals("comment1024", context1.fileset().comment());
      Assertions.assertEquals(fileset1.storageLocation(), context1.fileset().storageLocation());
      if (StringUtils.isBlank(subPath)) {
        Assertions.assertEquals(context1.fileset().storageLocation(), context1.actualPaths()[0]);
      } else {
        String storageLocation =
            context1.fileset().storageLocation().endsWith("/")
                ? context1
                    .fileset()
                    .storageLocation()
                    .substring(0, context1.fileset().storageLocation().length() - 1)
                : context1.fileset().storageLocation();
        Assertions.assertEquals(
            subPath.startsWith("/")
                ? String.format("%s%s", storageLocation, subPath)
                : String.format("%s/%s", storageLocation, subPath),
            context1.actualPaths()[0]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("invalidFilesetContextEnvProvider")
  public void testGetFilesetContextInvalid(
      FilesetPrefixPattern pattern,
      FilesetDataOperation operation,
      String subPath,
      Map<String, String> properties,
      boolean mountSingleFile)
      throws IOException {
    String schemaName =
        String.format(
            "schema_get_ctx_%s_prefix_%s",
            pattern.name(), UUID.randomUUID().toString().replace("-", ""));
    String comment = "comment1024";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);
    String catalogName = "c1";
    String filesetName =
        String.format(
            "fileset_get_ctx_%s_prefix_%s",
            pattern.name(), UUID.randomUUID().toString().replace("-", ""));
    String filesetLocation =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName;
    createFileset(
        filesetName, schemaName, comment, Fileset.Type.MANAGED, null, filesetLocation, properties);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);
      if (mountSingleFile) {
        Path locationPath = new Path(filesetLocation);
        try (FileSystem localFileSystem = locationPath.getFileSystem(new Configuration())) {
          // replace fileset location to a single file
          Assertions.assertTrue(localFileSystem.exists(locationPath));
          Assertions.assertTrue(localFileSystem.getFileStatus(locationPath).isDirectory());
          localFileSystem.delete(locationPath, true);
          localFileSystem.create(locationPath);
          Assertions.assertTrue(localFileSystem.exists(locationPath));
          Assertions.assertTrue(localFileSystem.getFileStatus(locationPath).isFile());

          BaseFilesetDataOperationCtx ctx1 =
              BaseFilesetDataOperationCtx.builder()
                  .withSubPath(subPath)
                  .withOperation(operation)
                  .withClientType(ClientType.HADOOP_GVFS)
                  .withIp("127.0.0.1")
                  .withSourceEngineType(SourceEngineType.UNKNOWN)
                  .withAppId("application_1_1")
                  .build();
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> ops.getFilesetContext(filesetIdent, ctx1));
        }
      } else {
        BaseFilesetDataOperationCtx ctx =
            BaseFilesetDataOperationCtx.builder()
                .withSubPath(subPath)
                .withOperation(operation)
                .withClientType(ClientType.HADOOP_GVFS)
                .withIp("127.0.0.1")
                .withSourceEngineType(SourceEngineType.UNKNOWN)
                .withAppId("application_1_1")
                .build();
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> ops.getFilesetContext(filesetIdent, ctx));

        // test sub path is null
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                BaseFilesetDataOperationCtx.builder()
                    .withSubPath(null)
                    .withOperation(operation)
                    .withClientType(ClientType.HADOOP_GVFS)
                    .withIp("127.0.0.1")
                    .withSourceEngineType(SourceEngineType.UNKNOWN)
                    .withAppId("application_1_1")
                    .build());
      }
    }
  }

  @Test
  public void testGetStorageLocations() {
    Map<String, String> props =
        ImmutableMap.of(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, "mocked_backup_storage_location1",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2, "mocked_backup_storage_location2",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 3, "mocked_backup_storage_location3",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 4, "mocked_backup_storage_location4");
    List<String> expectedStorageLocations =
        Arrays.asList(
            "mocked_primary_storage_location",
            "mocked_backup_storage_location1",
            "mocked_backup_storage_location2",
            "mocked_backup_storage_location3",
            "mocked_backup_storage_location4");
    Fileset fileset =
        HadoopFileset.builder()
            .withName("fileset")
            .withType(Fileset.Type.MANAGED)
            .withComment("comment")
            .withStorageLocation("mocked_primary_storage_location")
            .withProperties(props)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    List<String> storageLocations = getStorageLocations(fileset);
    Assertions.assertEquals(expectedStorageLocations, storageLocations);

    props =
        ImmutableMap.of(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 3, "mocked_backup_storage_location3",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, "mocked_backup_storage_location1",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 4, "mocked_backup_storage_location4",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 11, "mocked_backup_storage_location11",
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2, "mocked_backup_storage_location2");
    expectedStorageLocations =
        Arrays.asList(
            "mocked_primary_storage_location",
            "mocked_backup_storage_location1",
            "mocked_backup_storage_location2",
            "mocked_backup_storage_location3",
            "mocked_backup_storage_location4",
            "mocked_backup_storage_location11");
    fileset =
        HadoopFileset.builder()
            .withName("fileset")
            .withType(Fileset.Type.MANAGED)
            .withComment("comment")
            .withStorageLocation("mocked_primary_storage_location")
            .withProperties(props)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    storageLocations = getStorageLocations(fileset);
    Assertions.assertEquals(expectedStorageLocations, storageLocations);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/test/test.parquet", "test/test.parquet"})
  public void testFilesetCtxToGetActualPaths(String subPath) throws IOException {
    String schemaName = "schema_" + UUID.randomUUID().toString().replace("-", "");
    String comment = "comment";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String catalogName = "c1";

    String filesetName1 = "test_get_actual_paths_from_fileset_context";
    String filesetLocation1 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName1;
    String filesetBackupLocation1 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName1 + "_bak_1";
    String filesetBackupLocation2 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName1 + "_bak_2";
    String filesetBackupLocation3 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName1 + "_bak_3";
    String filesetBackupLocation4 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName1 + "_bak_4";
    Map<String, String> props =
        ImmutableMap.of(
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 3, filesetBackupLocation3,
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, filesetBackupLocation1,
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2, filesetBackupLocation2,
            FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 4, filesetBackupLocation4);

    List<String> storageLocations =
        Stream.of(
                filesetLocation1,
                filesetBackupLocation1,
                filesetBackupLocation2,
                filesetBackupLocation3,
                filesetBackupLocation4)
            .map(loc -> subPath.startsWith("/") ? loc + subPath : loc + "/" + subPath)
            .collect(Collectors.toList());

    Fileset fileset1 =
        createFileset(
            filesetName1, schemaName, comment, Fileset.Type.MANAGED, null, filesetLocation1, props);

    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put(CATALOG_BYPASS_PREFIX + CHECK_UNIQUE_STORAGE_LOCATION_SCHEME, "false");
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, null);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName1);
      BaseFilesetDataOperationCtx dataOperationCtx1 =
          BaseFilesetDataOperationCtx.builder()
              .withSubPath("/test/test.parquet")
              .withOperation(FilesetDataOperation.OPEN)
              .withClientType(ClientType.UNKNOWN)
              .withIp("127.0.0.1")
              .withSourceEngineType(SourceEngineType.UNKNOWN)
              .withAppId("application_1_1")
              .build();
      FilesetContext context1 = ops.getFilesetContext(filesetIdent, dataOperationCtx1);
      Assertions.assertEquals(filesetName1, context1.fileset().name());
      Assertions.assertEquals(Fileset.Type.MANAGED, context1.fileset().type());
      Assertions.assertEquals(comment, context1.fileset().comment());
      Assertions.assertEquals(fileset1.storageLocation(), context1.fileset().storageLocation());
      Assertions.assertEquals(
          4,
          (int)
              context1.fileset().properties().keySet().stream()
                  .filter(loc -> loc.startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY))
                  .count());
      Assertions.assertArrayEquals(storageLocations.toArray(new String[0]), context1.actualPaths());
    }
  }

  private static Stream<Arguments> locationArguments() {
    return Stream.of(
        // Honor the catalog location
        Arguments.of(
            "fileset11",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog21",
            null,
            TEST_ROOT_PATH + "/c1/s1_fileset11/fileset11",
            TEST_ROOT_PATH + "/c1/s1_fileset11/fileset11",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset11/fileset11_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset11/fileset11_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset11/fileset11_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset11/fileset11_bak_2")),
        Arguments.of(
            // honor the schema location
            "fileset12",
            Fileset.Type.MANAGED,
            null,
            TEST_ROOT_PATH + "/s1_fileset12",
            TEST_ROOT_PATH + "/c1/s1_fileset12/fileset12",
            TEST_ROOT_PATH + "/c1/s1_fileset12/fileset12",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset12/fileset12_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset12/fileset12_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset12/fileset12_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset12/fileset12_bak_2")),
        Arguments.of(
            // honor the schema location
            "fileset13",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog22",
            TEST_ROOT_PATH + "/s1_fileset13",
            TEST_ROOT_PATH + "/c1/s1_fileset13/fileset13",
            TEST_ROOT_PATH + "/c1/s1_fileset13/fileset13",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset13/fileset13_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset13/fileset13_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset13/fileset13_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset13/fileset13_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset14",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog23",
            TEST_ROOT_PATH + "/s1_fileset14",
            TEST_ROOT_PATH + "/c1/s1_fileset14/fileset14",
            TEST_ROOT_PATH + "/c1/s1_fileset14/fileset14",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset14/fileset14_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset14/fileset14_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset14/fileset14_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset14/fileset14_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset15",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/c1/s1_fileset15/fileset15",
            TEST_ROOT_PATH + "/c1/s1_fileset15/fileset15",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset15/fileset15_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset15/fileset15_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset15/fileset15_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset15/fileset15_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset16",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog24",
            null,
            TEST_ROOT_PATH + "/c1/s1_fileset16/fileset16",
            TEST_ROOT_PATH + "/c1/s1_fileset16/fileset16",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset16/fileset16_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset16/fileset16_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset16/fileset16_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset16/fileset16_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset17",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/catalog25",
            TEST_ROOT_PATH + "/s1_fileset17",
            TEST_ROOT_PATH + "/fileset17",
            TEST_ROOT_PATH + "/fileset17",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset17_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset17_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset17_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset17_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset18",
            Fileset.Type.EXTERNAL,
            null,
            TEST_ROOT_PATH + "/s1_fileset18",
            TEST_ROOT_PATH + "/fileset18",
            TEST_ROOT_PATH + "/fileset18",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset18_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset18_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset18_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset18_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset19",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/fileset19",
            TEST_ROOT_PATH + "/fileset19",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset19_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset19_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset19_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset19_bak_2")),
        // Honor the catalog location
        Arguments.of(
            "fileset101",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog201",
            null,
            TEST_ROOT_PATH + "/c1/s1_fileset101/fileset101",
            TEST_ROOT_PATH + "/c1/s1_fileset101/fileset101",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset101/fileset101_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset101/fileset101_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset101/fileset101_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset101/fileset101_bak_2")),
        Arguments.of(
            // honor the schema location
            "fileset102",
            Fileset.Type.MANAGED,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset102",
            TEST_ROOT_PATH + "/c1/s1_fileset102/fileset102",
            TEST_ROOT_PATH + "/c1/s1_fileset102/fileset102",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset102/fileset102_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset102/fileset102_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset102/fileset102_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset102/fileset102_bak_2")),
        Arguments.of(
            // honor the schema location
            "fileset103",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog202",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset103",
            TEST_ROOT_PATH + "/c1/s1_fileset103/fileset103",
            TEST_ROOT_PATH + "/c1/s1_fileset103/fileset103",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset103/fileset103_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset103/fileset103_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset103/fileset103_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset103/fileset103_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset104",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog203",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset104",
            TEST_ROOT_PATH + "/c1/s1_fileset104/fileset104",
            TEST_ROOT_PATH + "/c1/s1_fileset104/fileset104",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset104/fileset104_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset104/fileset104_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset104/fileset104_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset104/fileset104_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset105",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/c1/s1_fileset105/fileset105",
            TEST_ROOT_PATH + "/c1/s1_fileset105/fileset105",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset105/fileset105_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset105/fileset105_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset105/fileset105_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset105/fileset105_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset106",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog204",
            null,
            TEST_ROOT_PATH + "/c1/s1_fileset106/fileset106",
            TEST_ROOT_PATH + "/c1/s1_fileset106/fileset106",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset106/fileset106_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset106/fileset106_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/c1/s1_fileset106/fileset106_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/c1/s1_fileset106/fileset106_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset107",
            Fileset.Type.EXTERNAL,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog205",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset107",
            TEST_ROOT_PATH + "/fileset107",
            TEST_ROOT_PATH + "/fileset107",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset107_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset107_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset107_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset107_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset108",
            Fileset.Type.EXTERNAL,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset108",
            TEST_ROOT_PATH + "/fileset108",
            TEST_ROOT_PATH + "/fileset108",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset108_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset108_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset108_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset108_bak_2")),
        Arguments.of(
            // honor the storage location
            "fileset109",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/fileset109",
            TEST_ROOT_PATH + "/fileset109",
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset109_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset109_bak_2"),
            ImmutableMap.of(
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1,
                TEST_ROOT_PATH + "/fileset109_bak_1",
                FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2,
                TEST_ROOT_PATH + "/fileset109_bak_2")));
  }

  private static Stream<Arguments> testRenameArguments() {
    return Stream.of(
        // Honor the catalog location
        Arguments.of(
            "fileset31",
            "fileset31_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog21",
            null,
            TEST_ROOT_PATH + "/c1/s24_fileset31/fileset31",
            TEST_ROOT_PATH + "/c1/s24_fileset31/fileset31"),
        Arguments.of(
            // honor the schema location
            "fileset32",
            "fileset32_new",
            Fileset.Type.MANAGED,
            null,
            TEST_ROOT_PATH + "/s24_fileset32",
            TEST_ROOT_PATH + "/c1/s24_fileset32/fileset32",
            TEST_ROOT_PATH + "/c1/s24_fileset32/fileset32"),
        Arguments.of(
            // honor the schema location
            "fileset33",
            "fileset33_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog22",
            TEST_ROOT_PATH + "/s24_fileset33",
            TEST_ROOT_PATH + "/c1/s24_fileset33/fileset33",
            TEST_ROOT_PATH + "/c1/s24_fileset33/fileset33"),
        Arguments.of(
            // honor the storage location
            "fileset34",
            "fileset34_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog23",
            TEST_ROOT_PATH + "/s24_fileset34",
            TEST_ROOT_PATH + "/c1/s24_fileset34/fileset34",
            TEST_ROOT_PATH + "/c1/s24_fileset34/fileset34"),
        Arguments.of(
            // honor the storage location
            "fileset35",
            "fileset35_new",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/c1/s24_fileset35/fileset35",
            TEST_ROOT_PATH + "/c1/s24_fileset35/fileset35"),
        Arguments.of(
            // honor the storage location
            "fileset36",
            "fileset36_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog24",
            null,
            TEST_ROOT_PATH + "/c1/s24_fileset36/fileset36",
            TEST_ROOT_PATH + "/c1/s24_fileset36/fileset36"),
        Arguments.of(
            // honor the storage location
            "fileset37",
            "fileset37_new",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/catalog25",
            TEST_ROOT_PATH + "/s24_fileset37",
            TEST_ROOT_PATH + "/fileset37",
            TEST_ROOT_PATH + "/fileset37"),
        Arguments.of(
            // honor the storage location
            "fileset38",
            "fileset38_new",
            Fileset.Type.EXTERNAL,
            null,
            TEST_ROOT_PATH + "/s24_fileset38",
            TEST_ROOT_PATH + "/fileset38",
            TEST_ROOT_PATH + "/fileset38"),
        Arguments.of(
            // honor the storage location
            "fileset39",
            "fileset39_new",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/fileset39",
            TEST_ROOT_PATH + "/fileset39"));
  }

  private static Stream<Arguments> filesetTypes() {
    return Stream.of(Arguments.of(Fileset.Type.EXTERNAL), Arguments.of(Fileset.Type.MANAGED));
  }

  private void createStorageLocation(Path storageLocationPath) {
    try {
      FileSystem fs = storageLocationPath.getFileSystem(new Configuration());
      fs.mkdirs(storageLocationPath);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to create storage location: %s", storageLocationPath.toString()),
          e);
    }
  }

  private Schema createSchema(String name, String comment, String catalogPath, String schemaPath)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    if (catalogPath != null) {
      props.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(props, null);

      NameIdentifier schemaIdent = NameIdentifier.ofSchema("m1", "c1", name);
      Map<String, String> schemaProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      schemaProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, schemaProps));

      if (schemaPath != null) {
        schemaProps.put(HadoopSchemaPropertiesMetadata.LOCATION, schemaPath);
      }

      return ops.createSchema(schemaIdent, comment, schemaProps);
    }
  }

  private Fileset createFileset(
      String name,
      String schemaName,
      String comment,
      Fileset.Type type,
      String catalogPath,
      String storageLocation,
      Map<String, String> filesetProperties)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(CATALOG_BYPASS_PREFIX + CHECK_UNIQUE_STORAGE_LOCATION_SCHEME, "false");
    if (catalogPath != null) {
      props.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store)) {
      ops.initialize(props, null);

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Map<String, String> filesetProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      filesetProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps));
      filesetProps.putAll(filesetProperties);

      return ops.createFileset(filesetIdent, comment, type, storageLocation, filesetProps);
    }
  }

  private Fileset createFileset(
      String name,
      String schemaName,
      String comment,
      Fileset.Type type,
      String catalogPath,
      String storageLocation)
      throws IOException {
    return createFileset(
        name, schemaName, comment, type, catalogPath, storageLocation, Maps.newHashMap());
  }
}
