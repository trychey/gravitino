/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
}
