/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestFilesetOperationDispatcher extends TestOperationDispatcher {
  static FilesetOperationDispatcher filesetOperationDispatcher;
  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator);
  }

  @Test
  public void testCreateAndListFilesets() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema81");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset1");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "test", props);
    Assertions.assertEquals("fileset1", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("test", fileset1.storageLocation());

    NameIdentifier[] idents = filesetOperationDispatcher.listFilesets(filesetNs);
    Assertions.assertEquals(1, idents.length);
    Assertions.assertEquals(filesetIdent1, idents[0]);

    Map<String, String> illegalProps = ImmutableMap.of("k2", "v2");
    testPropertyException(
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps),
        "Properties are required and must be set");

    Map<String, String> illegalProps2 = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps2),
        "Properties are reserved and cannot be set",
        "gravitino.identifier");
  }

  @Test
  public void testCreateAndLoadFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema91");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "location", "schema91");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset11");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, null, props);
    Assertions.assertEquals("fileset11", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertNull(fileset1.storageLocation());

    Fileset loadedFileset1 = filesetOperationDispatcher.loadFileset(filesetIdent1);
    Assertions.assertEquals(fileset1.name(), loadedFileset1.name());
    Assertions.assertEquals(fileset1.comment(), loadedFileset1.comment());
    testProperties(props, loadedFileset1.properties());
    Assertions.assertEquals(fileset1.type(), loadedFileset1.type());
    Assertions.assertEquals(fileset1.storageLocation(), loadedFileset1.storageLocation());
  }

  @Test
  public void testCreateAndAlterFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema101");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset21");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "fileset21", props);
    Assertions.assertEquals("fileset21", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("fileset21", fileset1.storageLocation());

    FilesetChange[] changes =
        new FilesetChange[] {
          FilesetChange.setProperty("k3", "v3"), FilesetChange.removeProperty("k1")
        };

    Fileset alteredFileset = filesetOperationDispatcher.alterFileset(filesetIdent1, changes);
    Assertions.assertEquals(fileset1.name(), alteredFileset.name());
    Assertions.assertEquals(fileset1.comment(), alteredFileset.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredFileset.properties());

    FilesetChange[] changes2 = new FilesetChange[] {FilesetChange.updateComment("new comment")};

    Fileset alteredFileset2 = filesetOperationDispatcher.alterFileset(filesetIdent1, changes2);
    Assertions.assertEquals(fileset1.name(), alteredFileset2.name());
    Assertions.assertEquals("new comment", alteredFileset2.comment());

    FilesetChange[] changes3 = new FilesetChange[] {FilesetChange.removeComment()};

    Fileset alteredFileset3 = filesetOperationDispatcher.alterFileset(filesetIdent1, changes3);
    Assertions.assertEquals(fileset1.name(), alteredFileset3.name());
    Assertions.assertNull(alteredFileset3.comment());

    FilesetChange[] changes4 =
        new FilesetChange[] {
          FilesetChange.setProperty(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, "1111")
        };
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> filesetOperationDispatcher.alterFileset(filesetIdent1, changes4));

    FilesetChange[] changes5 =
        new FilesetChange[] {
          FilesetChange.removeProperty(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1)
        };
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> filesetOperationDispatcher.alterFileset(filesetIdent1, changes5));

    // Test immutable fileset properties
    FilesetChange[] illegalChange = new FilesetChange[] {FilesetChange.setProperty(ID_KEY, "test")};
    testPropertyException(
        () -> filesetOperationDispatcher.alterFileset(filesetIdent1, illegalChange),
        "Property gravitino.identifier is immutable or reserved, cannot be set");
  }

  @Test
  public void testCreateAndDropFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema111");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset31");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "fileset31", props);
    Assertions.assertEquals("fileset31", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("fileset31", fileset1.storageLocation());

    boolean dropped = filesetOperationDispatcher.dropFileset(filesetIdent1);
    Assertions.assertTrue(dropped);
  }

  @Test
  public void testCreateFilesetWithAnyPrefixPattern() {
    Namespace ns = Namespace.of(metalake, catalog, "fileset_schema_1");
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(ns.levels()), "comment", schemaProps);
    NameIdentifier filesetIdent1 = NameIdentifier.of(ns, "fileset1");

    // should set life cycle time be negative when prefix is any prefix
    Map<String, String> invalidLifeCycleWithAnyPrefixProps =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "30",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1,
                "comment",
                Fileset.Type.MANAGED,
                "test",
                invalidLifeCycleWithAnyPrefixProps));

    // test any prefix's life cycle number and dir max level
    Map<String, String> Props1 =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "30",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", Props1));
    Map<String, String> Props2 =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "-1",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "-1");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", Props2));
  }

  @Test
  public void testCreateFilesetWithDateWithStringPrefixPattern() {
    Namespace ns = Namespace.of(metalake, catalog, "fileset_schema_2");
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(ns.levels()), "comment", schemaProps);
    NameIdentifier filesetIdent1 = NameIdentifier.of(ns, "fileset1");

    // test date with string prefix's dir max level
    Map<String, String> Props2 =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "-1",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "0");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", Props2));
  }

  @Test
  public void testCreateFilesetWithYearMonthDayPrefixPattern() {
    Namespace ns = Namespace.of(metalake, catalog, "fileset_schema_3");
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(ns.levels()), "comment", schemaProps);
    NameIdentifier filesetIdent1 = NameIdentifier.of(ns, "fileset1");

    // test date with string prefix's dir max level
    Map<String, String> Props2 =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "-1",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            "0");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", Props2));
  }

  @Test
  public void testCreateFilesetWithExtraProperties() {
    Namespace ns = Namespace.of(metalake, catalog, "fileset_schema_4");
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(ns.levels()), "comment", schemaProps);

    Map<String, String> props =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "30",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name());
    NameIdentifier filesetIdent1 = NameIdentifier.of(ns, "fileset1");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "test", props);

    Fileset loadedFileset1 = filesetOperationDispatcher.loadFileset(filesetIdent1);
    Assertions.assertEquals(fileset1.name(), loadedFileset1.name());
    Assertions.assertEquals(fileset1.comment(), loadedFileset1.comment());
    testProperties(props, loadedFileset1.properties());
    Assertions.assertEquals(fileset1.type(), loadedFileset1.type());
    Assertions.assertEquals(fileset1.storageLocation(), loadedFileset1.storageLocation());

    // invalid dir prefix
    Map<String, String> invalidDirPrefixProps =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            "123",
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "30",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name());
    NameIdentifier filesetIdent3 = NameIdentifier.of(ns, "fileset3");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent3, "comment", Fileset.Type.MANAGED, "test", invalidDirPrefixProps));

    // invalid lifecycle time num
    Map<String, String> invalidLifeCycleTimeNumProps =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "123.1",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            FilesetLifecycleUnit.RETENTION_DAY.name());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent3,
                "comment",
                Fileset.Type.MANAGED,
                "test",
                invalidLifeCycleTimeNumProps));

    // invalid lifecycle time unit
    Map<String, String> invalidLifeCycleTimeUnitProps =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "30",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            "test");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent3,
                "comment",
                Fileset.Type.MANAGED,
                "test",
                invalidLifeCycleTimeUnitProps));

    // should set life cycle time num and correct time unit
    Map<String, String> invalidLifeCycleProps =
        ImmutableMap.of(
            "k1",
            "v1",
            "k2",
            "v2",
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.DATE_WITH_STRING.name(),
            FilesetProperties.LIFECYCLE_TIME_NUM_KEY,
            "30",
            FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
            "hello");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent3, "comment", Fileset.Type.MANAGED, "test", invalidLifeCycleProps));
  }

  @Test
  public void testCreateFilesetAndGetContext() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema_test_get_context");
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(
        NameIdentifier.of(filesetNs.levels()), "comment", schemaProps);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset_test_get_context");
    Map<String, String> filesetProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1,
            "comment",
            Fileset.Type.MANAGED,
            "/tmp/test_" + UUID.randomUUID().toString().replace("-", ""),
            filesetProps);
    Assertions.assertEquals("fileset_test_get_context", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertNotNull(fileset1.storageLocation());

    BaseFilesetDataOperationCtx ctx =
        BaseFilesetDataOperationCtx.builder()
            .withOperation(FilesetDataOperation.OPEN)
            .withSubPath("/test/123")
            .withClientType(ClientType.HADOOP_GVFS)
            .withIp("127.0.0.1")
            .withSourceEngineType(SourceEngineType.SPARK)
            .withAppId("application_1_1")
            .build();
    FilesetContext context = filesetOperationDispatcher.getFilesetContext(filesetIdent1, ctx);
    Assertions.assertEquals(fileset1.name(), context.fileset().name());
    Assertions.assertEquals(fileset1.comment(), context.fileset().comment());
    Assertions.assertEquals(fileset1.type(), context.fileset().type());
    Assertions.assertEquals(fileset1.storageLocation(), context.fileset().storageLocation());
    Assertions.assertEquals(fileset1.storageLocation() + "/test/123", context.actualPaths()[0]);
    File file = new File(fileset1.storageLocation());
    try {
      file.delete();
    } catch (Exception e) {
      // ignore
    }
  }

  @ParameterizedTest
  @MethodSource("filesetTypes")
  public void testCreateLoadAndDropFilesetWithMultipleLocs(Fileset.Type type) {
    String schemaName = "schema91_" + type;
    Namespace filesetNs = Namespace.of(metalake, catalog, schemaName);
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "location", schemaName);
    schemaOperationDispatcher.createSchema(
        NameIdentifier.of(filesetNs.levels()), "comment", schemaProps);

    // Create fileset
    String filesetName = "fileset11";
    NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, filesetName);
    String filesetComment = "fileset11 comment";
    String storageLocation = "fileset11_path";
    String backupStorageLocation = "fileset11_path_bak";
    Map<String, String> filesetProps =
        ImmutableMap.of(
            "k1", "v1", FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    Fileset fileset =
        filesetOperationDispatcher.createFileset(
            filesetIdent, filesetComment, type, storageLocation, filesetProps);
    Assertions.assertEquals(filesetName, fileset.name());
    Assertions.assertEquals(filesetComment, fileset.comment());
    testProperties(filesetProps, fileset.properties());
    Assertions.assertEquals(type, fileset.type());
    Assertions.assertEquals(storageLocation, fileset.storageLocation());
    Assertions.assertEquals(
        backupStorageLocation,
        fileset.properties().get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));

    String filesetName1 = "fileset12";
    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, filesetName1);
    String storageLocation1 = filesetName1 + "_path";
    Map<String, String> filesetProps1 =
        ImmutableMap.of(
            "k1", "v1", FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + "aaaaaa", "aaaaaa");
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, filesetComment, type, storageLocation1, filesetProps1));

    // Load fileset
    Fileset loadedFileset = filesetOperationDispatcher.loadFileset(filesetIdent);
    Assertions.assertEquals(fileset.name(), loadedFileset.name());
    Assertions.assertEquals(fileset.comment(), loadedFileset.comment());
    testProperties(fileset.properties(), loadedFileset.properties());
    Assertions.assertEquals(fileset.type(), loadedFileset.type());
    Assertions.assertEquals(fileset.storageLocation(), loadedFileset.storageLocation());
    Assertions.assertEquals(
        backupStorageLocation,
        loadedFileset.properties().get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));

    // Drop fileset
    Assertions.assertTrue(filesetOperationDispatcher.dropFileset(filesetIdent));
    Assertions.assertThrowsExactly(
        NoSuchFilesetException.class, () -> filesetOperationDispatcher.loadFileset(filesetIdent));
  }

  @ParameterizedTest
  @MethodSource("filesetTypes")
  public void testStorageLocationFilesetChanges(Fileset.Type type) {
    String schemaName = "schema92_" + type;
    Namespace filesetNs = Namespace.of(metalake, catalog, schemaName);
    Map<String, String> schemaProps = ImmutableMap.of("k1", "v1", "location", schemaName);
    schemaOperationDispatcher.createSchema(
        NameIdentifier.of(filesetNs.levels()), "comment", schemaProps);

    // Create fileset
    String name = "fileset11_" + type.name();
    String filesetComment = "test_comment";
    NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, name);
    String storageLocation = "fileset11_path";
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

    Fileset fileset =
        filesetOperationDispatcher.createFileset(
            filesetIdent, filesetComment, type, storageLocation, ImmutableMap.of("k1", "v1"));

    Fileset fileset1 = filesetOperationDispatcher.alterFileset(filesetIdent, change1);
    Assertions.assertEquals(name, fileset1.name());
    Assertions.assertEquals(type, fileset1.type());
    Assertions.assertEquals("test_comment", fileset1.comment());
    Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
    Map<String, String> props1 = fileset1.properties();
    Assertions.assertTrue(props1.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    Assertions.assertEquals(
        backupLoc1, props1.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));

    Fileset fileset2 = filesetOperationDispatcher.alterFileset(filesetIdent, change2);
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

    Fileset fileset3 = filesetOperationDispatcher.alterFileset(filesetIdent, change3);
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

    Fileset fileset4 = filesetOperationDispatcher.alterFileset(filesetIdent, change4);
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

    Fileset fileset5 = filesetOperationDispatcher.alterFileset(filesetIdent, change5);
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

    Fileset fileset6 = filesetOperationDispatcher.alterFileset(filesetIdent, change6);
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

    Fileset fileset7 = filesetOperationDispatcher.alterFileset(filesetIdent, change7);
    Assertions.assertEquals(name, fileset7.name());
    Assertions.assertEquals(type, fileset7.type());
    Assertions.assertEquals("test_comment", fileset7.comment());
    Assertions.assertEquals(backupLoc2_new, fileset7.storageLocation());
    Map<String, String> props7 = fileset7.properties();
    Assertions.assertFalse(props7.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    Assertions.assertTrue(props7.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));
    Assertions.assertEquals(
        backupLoc1_new, props7.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

    Fileset fileset8 = filesetOperationDispatcher.alterFileset(filesetIdent, change8);
    Assertions.assertEquals(name, fileset8.name());
    Assertions.assertEquals(type, fileset8.type());
    Assertions.assertEquals("test_comment", fileset8.comment());
    Assertions.assertEquals(backupLoc2_new, fileset8.storageLocation());
    Map<String, String> props8 = fileset8.properties();
    Assertions.assertFalse(props8.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    Assertions.assertFalse(props8.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 2));

    Fileset fileset9 = filesetOperationDispatcher.alterFileset(filesetIdent, change9);
    Assertions.assertEquals(name, fileset9.name());
    Assertions.assertEquals(type, fileset9.type());
    Assertions.assertEquals("test_comment", fileset9.comment());
    Assertions.assertEquals(newStorageLocation, fileset9.storageLocation());
    filesetOperationDispatcher.alterFileset(filesetIdent, change10);

    Fileset fileset10 = filesetOperationDispatcher.alterFileset(filesetIdent, change1);
    Assertions.assertEquals(name, fileset10.name());
    Assertions.assertEquals(type, fileset10.type());
    Assertions.assertEquals("test_comment", fileset10.comment());
    Assertions.assertEquals(storageLocation, fileset10.storageLocation());
    Map<String, String> props10 = fileset10.properties();
    Assertions.assertTrue(props10.containsKey(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    Assertions.assertEquals(
        backupLoc1, props10.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
  }

  @Override
  Map<String, String> getCatalogProps() {
    return ImmutableMap.of(CATALOG_BYPASS_PREFIX + "check.unique.storage.location.schema", "false");
  }

  private static Stream<Arguments> filesetTypes() {
    return Stream.of(Arguments.of(Fileset.Type.EXTERNAL), Arguments.of(Fileset.Type.MANAGED));
  }
}
