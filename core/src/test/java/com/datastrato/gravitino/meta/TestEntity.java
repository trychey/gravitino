/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Field;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntity {
  private final Instant now = Instant.now();

  private final SchemaVersion version = SchemaVersion.V_0_1;
  private final AuditInfo auditInfo =
      new AuditInfo.Builder().withCreator("test").withCreateTime(now).build();

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

  @Test
  public void testMetalake() {
    BaseMetalake metalake =
        new BaseMetalake.Builder()
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
        new CatalogEntity.Builder()
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
        new SchemaEntity.Builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testSchema.fields();
    Assertions.assertEquals(schemaId, fields.get(SchemaEntity.ID));
    Assertions.assertEquals(schemaName, fields.get(SchemaEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(SchemaEntity.AUDIT_INFO));
  }

  @Test
  public void testTable() {
    TableEntity testTable =
        new TableEntity.Builder()
            .withId(tableId)
            .withName(tableName)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testTable.fields();
    Assertions.assertEquals(tableId, fields.get(TableEntity.ID));
    Assertions.assertEquals(tableName, fields.get(TableEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(TableEntity.AUDIT_INFO));
  }
}
