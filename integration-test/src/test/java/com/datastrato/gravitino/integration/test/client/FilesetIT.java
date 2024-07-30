/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class FilesetIT extends AbstractIT {

  private static final Logger LOG = LoggerFactory.getLogger(FilesetIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String metalakeName = GravitinoITUtils.genRandomName("fileset_it_metalake");

  private static GravitinoMetalake metalake;

  private static String hmsUri;

  @BeforeAll
  public static void startUp() {
    containerSuite.startHiveContainer();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    NameIdentifier ident = NameIdentifier.of(metalakeName);
    Assertions.assertFalse(client.metalakeExists(ident));
    metalake = client.createMetalake(ident, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(ident));
  }

  @AfterAll
  public static void tearDown() {
    client.dropMetalake(NameIdentifier.of(metalakeName));

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  @Test
  public void testGetFilesetContext() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogIdent));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    Catalog catalog =
        metalake.createCatalog(
            catalogIdent, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogIdent));

    String schemaName = GravitinoITUtils.genRandomName("schema");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schemaIdent));
    catalog.asSchemas().createSchema(schemaIdent, "schema comment", new HashMap<>());
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaIdent));

    String filesetName = GravitinoITUtils.genRandomName("fileset");
    NameIdentifier filesetIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    Assertions.assertFalse(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    Fileset expectedFileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                filesetIdent,
                "fileset comment",
                Fileset.Type.MANAGED,
                generateLocation(catalogName, schemaName, filesetName),
                new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    FilesetDataOperationCtx ctx =
        BaseFilesetDataOperationCtx.builder()
            .withSubPath("/test.par")
            .withOperation(FilesetDataOperation.CREATE)
            .withClientType(ClientType.HADOOP_GVFS)
            .withIp("127.0.0.1")
            .withSourceEngineType(SourceEngineType.SPARK)
            .withAppId("application_1_1")
            .build();
    FilesetContext context = catalog.asFilesetCatalog().getFilesetContext(filesetIdent, ctx);

    Fileset actualFileset = context.fileset();
    Assertions.assertEquals(expectedFileset.name(), actualFileset.name());
    Assertions.assertEquals(expectedFileset.comment(), actualFileset.comment());
    Assertions.assertEquals(expectedFileset.type(), actualFileset.type());
    Assertions.assertEquals(expectedFileset.storageLocation(), actualFileset.storageLocation());

    String[] actualPaths = context.actualPaths();
    Assertions.assertEquals(expectedFileset.storageLocation() + "/test.par", actualPaths[0]);
  }

  private static String generateLocation(
      String catalogName, String schemaName, String filesetName) {
    return String.format(
        "hdfs://%s:%d/user/hadoop/%s/%s/%s",
        containerSuite.getHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT,
        catalogName,
        schemaName,
        filesetName);
  }
}
