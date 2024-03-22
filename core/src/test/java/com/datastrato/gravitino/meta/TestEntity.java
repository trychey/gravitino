/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.file.Fileset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntity {
  private final Instant now = Instant.now();

  private final SchemaVersion version = SchemaVersion.V_0_1;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(now).build();

  // Metalake test data
  private final Long metalakeId = 1L;
  private final String metalakeName = "testMetalake";
  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");

  // Catalog test data
  private final Long catalogId = 1L;
  private final String catalogName = "testCatalog";
  private final Catalog.Type type = Catalog.Type.RELATIONAL;
  private final String provider = "test";

  // Schema test data
  private final Long schemaId = 1L;
  private final String schemaName = "testSchema";

  // Table test data
  private final Long tableId = 1L;
  private final String tableName = "testTable";

  // File test data
  private final Long fileId = 1L;
  private final String fileName = "testFile";

  // Topic test data
  private final Long topicId = 1L;
  private final String topicName = "testTopic";

  // User test data
  private final Long userId = 1L;
  private final String userName = "testUser";

  @Test
  public void testMetalake() {
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .withVersion(version)
            .build();

    Map<Field, Object> fields = metalake.fields();
    Assertions.assertEquals(metalakeId, fields.get(BaseMetalake.ID));
    Assertions.assertEquals(metalakeName, fields.get(BaseMetalake.NAME));
    Assertions.assertEquals(map, fields.get(BaseMetalake.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(BaseMetalake.AUDIT_INFO));
    Assertions.assertNull(fields.get(BaseMetalake.COMMENT));
    Assertions.assertEquals(version, fields.get(BaseMetalake.SCHEMA_VERSION));
  }

  @Test
  public void testCatalog() {
    String catalogComment = "testComment";
    CatalogEntity testCatalog =
        CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withComment(catalogComment)
            .withType(type)
            .withProvider(provider)
            .withProperties(map)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testCatalog.fields();
    Assertions.assertEquals(catalogId, fields.get(CatalogEntity.ID));
    Assertions.assertEquals(catalogName, fields.get(CatalogEntity.NAME));
    Assertions.assertEquals(catalogComment, fields.get(CatalogEntity.COMMENT));
    Assertions.assertEquals(type, fields.get(CatalogEntity.TYPE));
    Assertions.assertEquals(map, fields.get(CatalogEntity.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(CatalogEntity.AUDIT_INFO));
  }

  @Test
  public void testSchema() {
    SchemaEntity testSchema =
        SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testSchema.fields();
    Assertions.assertEquals(schemaId, fields.get(SchemaEntity.ID));
    Assertions.assertEquals(schemaName, fields.get(SchemaEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(SchemaEntity.AUDIT_INFO));

    SchemaEntity testSchema1 =
        SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .withComment("testComment")
            .withProperties(map)
            .build();
    Map<Field, Object> fields1 = testSchema1.fields();
    Assertions.assertEquals("testComment", fields1.get(SchemaEntity.COMMENT));
    Assertions.assertEquals(map, fields1.get(SchemaEntity.PROPERTIES));
  }

  @Test
  public void testTable() {
    TableEntity testTable =
        TableEntity.builder().withId(tableId).withName(tableName).withAuditInfo(auditInfo).build();

    Map<Field, Object> fields = testTable.fields();
    Assertions.assertEquals(tableId, fields.get(TableEntity.ID));
    Assertions.assertEquals(tableName, fields.get(TableEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(TableEntity.AUDIT_INFO));
  }

  @Test
  public void testFile() {
    FilesetEntity testFile =
        FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .withProperties(map)
            .build();

    Map<Field, Object> fields = testFile.fields();
    Assertions.assertEquals(fileId, fields.get(FilesetEntity.ID));
    Assertions.assertEquals(fileName, fields.get(FilesetEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(FilesetEntity.AUDIT_INFO));
    Assertions.assertEquals(Fileset.Type.MANAGED, fields.get(FilesetEntity.TYPE));
    Assertions.assertEquals(map, fields.get(FilesetEntity.PROPERTIES));
    Assertions.assertNull(fields.get(FilesetEntity.COMMENT));
    Assertions.assertEquals("testLocation", fields.get(FilesetEntity.STORAGE_LOCATION));

    FilesetEntity testFile1 =
        FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.MANAGED)
            .withComment("testComment")
            .withStorageLocation("testLocation")
            .build();
    Assertions.assertEquals("testComment", testFile1.comment());
    Assertions.assertNull(testFile1.properties());

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              FilesetEntity.builder()
                  .withId(fileId)
                  .withName(fileName)
                  .withAuditInfo(auditInfo)
                  .withFilesetType(Fileset.Type.EXTERNAL)
                  .withProperties(map)
                  .withComment("testComment")
                  .build();
            });
    Assertions.assertEquals("Field storage_location is required", exception.getMessage());
  }

  @Test
  public void testTopic() {
    TopicEntity testTopic =
        TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withAuditInfo(auditInfo)
            .withComment("test topic comment")
            .withProperties(map)
            .build();

    Map<Field, Object> fields = testTopic.fields();
    Assertions.assertEquals(topicId, fields.get(TopicEntity.ID));
    Assertions.assertEquals(topicName, fields.get(TopicEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(TopicEntity.AUDIT_INFO));
    Assertions.assertEquals("test topic comment", fields.get(TopicEntity.COMMENT));
    Assertions.assertEquals(map, fields.get(TopicEntity.PROPERTIES));

    TopicEntity testTopic1 =
        TopicEntity.builder().withId(topicId).withName(topicName).withAuditInfo(auditInfo).build();
    Assertions.assertNull(testTopic1.comment());
    Assertions.assertNull(testTopic1.properties());
  }

  @Test
  public void testUser() {
    ManagedUser testManagedUser =
        ManagedUser.builder()
            .withMetalake("metalake")
            .withId(userId)
            .withName(userName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .withFirstName("first")
            .withLastName("last")
            .withDisplayName("display")
            .withEmailAddress("123@abc.com")
            .withComment("comment")
            .withDefaultRole("role")
            .withActive(true)
            .withGroups(Lists.newArrayList("group"))
            .withRoles(Lists.newArrayList("role"))
            .build();

    Map<Field, Object> fields = testManagedUser.fields();
    Assertions.assertEquals(userId, fields.get(ManagedUser.ID));
    Assertions.assertEquals(userName, fields.get(ManagedUser.NAME));
    Assertions.assertEquals(auditInfo, fields.get(ManagedUser.AUDIT_INFO));
    Assertions.assertEquals(map, fields.get(ManagedUser.PROPERTIES));
    Assertions.assertEquals("first", fields.get(ManagedUser.FIRST_NAME));
    Assertions.assertEquals("last", fields.get(ManagedUser.LAST_NAME));
    Assertions.assertEquals("display", fields.get(ManagedUser.DISPLAY_NAME));
    Assertions.assertEquals("123@abc.com", fields.get(ManagedUser.EMAIL_ADDRESS));
    Assertions.assertEquals(Lists.newArrayList("role"), fields.get(ManagedUser.ROLES));
    Assertions.assertEquals(Lists.newArrayList("group"), fields.get(ManagedUser.GROUPS));
    Assertions.assertTrue((Boolean) fields.get(ManagedUser.ACTIVE));
    Assertions.assertEquals("comment", fields.get(ManagedUser.COMMENT));
    Assertions.assertEquals("role", fields.get(ManagedUser.DEFAULT_ROLE));

    ManagedUser testManagedUserWithoutFields =
        ManagedUser.builder()
            .withMetalake("metalake")
            .withId(userId)
            .withName(userName)
            .withAuditInfo(auditInfo)
            .withFirstName("first")
            .withLastName("last")
            .withDisplayName("display")
            .withEmailAddress("123@abc.com")
            .build();

    Assertions.assertEquals("first", fields.get(ManagedUser.FIRST_NAME));
    Assertions.assertEquals("last", fields.get(ManagedUser.LAST_NAME));
    Assertions.assertEquals("display", fields.get(ManagedUser.DISPLAY_NAME));
    Assertions.assertEquals("123@abc.com", fields.get(ManagedUser.EMAIL_ADDRESS));
    Assertions.assertNull(testManagedUserWithoutFields.properties());
    Assertions.assertNull(testManagedUserWithoutFields.roles());
    Assertions.assertNull(testManagedUserWithoutFields.groups());
  }
}
