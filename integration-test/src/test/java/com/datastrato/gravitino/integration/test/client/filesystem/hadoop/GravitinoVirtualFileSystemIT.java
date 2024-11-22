/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.client.filesystem.hadoop;

import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class GravitinoVirtualFileSystemIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystemIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
  private static final String catalogName = GravitinoITUtils.genRandomName("catalog");
  private static final String schemaName = GravitinoITUtils.genRandomName("schema");
  private static GravitinoMetalake metalake;
  private static Configuration conf = new Configuration();
  private static final String CHECK_UNIQUE_STORAGE_LOCATION_SCHEME =
      "check.unique.storage.location.scheme";
  private static final String FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY =
      "fs.gravitino.fileset.write.primaryOnly";

  @BeforeAll
  public static void startUp() {
    containerSuite.startHiveContainer();
    NameIdentifier ident = NameIdentifier.of(metalakeName);
    Assertions.assertFalse(client.metalakeExists(ident));
    metalake = client.createMetalake(ident, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(ident));

    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, catalogName);

    Map<String, String> catalogProperties =
        ImmutableMap.of(CATALOG_BYPASS_PREFIX + CHECK_UNIQUE_STORAGE_LOCATION_SCHEME, "false");
    Catalog catalog =
        metalake.createCatalog(
            catalogIdent, Catalog.Type.FILESET, "hadoop", "catalog comment", catalogProperties);
    Assertions.assertTrue(metalake.catalogExists(catalogIdent));

    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    catalog.asSchemas().createSchema(schemaIdent, "schema comment", schemaProperties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaIdent));

    conf.set(
        "fs.gvfs.impl", "com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.AbstractFileSystem.gvfs.impl", "com.datastrato.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl.disable.cache", "true");
    conf.set("fs.gravitino.server.uri", serverUri);
    conf.set("fs.gravitino.client.metalake", metalakeName);
    conf.set("fs.gravitino.testing", "true");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
  }

  @AfterAll
  public static void tearDown() throws IOException {
    client.dropMetalake(NameIdentifier.of(metalakeName));

    if (client != null) {
      client.close();
      client = null;
    }

    Path hdfsPath = new Path(baseHdfsPath());
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      if (fs.exists(hdfsPath)) {
        fs.delete(hdfsPath, true);
      }
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  @Test
  public void testCreate() throws IOException {
    // create fileset
    String filesetName = "test_fileset_create";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs create
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        String fileName = "test.txt";
        Path createPath = new Path(gvfsPath + "/" + fileName);
        gvfs.create(createPath);
        Assertions.assertTrue(gvfs.exists(createPath));
        Assertions.assertTrue(gvfs.getFileStatus(createPath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
      }
    }
  }

  @Test
  public void testAppend() throws IOException {
    // create fileset
    String filesetName = "test_fileset_append";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs append
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path appendPath = new Path(gvfsPath + "/" + fileName);

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(appendPath);
        Assertions.assertTrue(gvfs.exists(appendPath));
        Assertions.assertTrue(gvfs.getFileStatus(appendPath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        try (FSDataOutputStream outputStream = gvfs.append(appendPath, 3)) {
          // Hello, World!
          byte[] wordsBytes =
              new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
          outputStream.write(wordsBytes);
        }
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        try (FSDataInputStream inputStream = gvfs.open(appendPath, 3)) {
          int bytesRead;
          byte[] buffer = new byte[1024];
          try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
            while ((bytesRead = inputStream.read(buffer)) != -1) {
              byteOutputStream.write(buffer, 0, bytesRead);
            }
            assertEquals(
                "Hello, World!",
                new String(byteOutputStream.toByteArray(), StandardCharsets.UTF_8));
          }
        }
      }
    }
  }

  @Test
  public void testDelete() throws IOException {
    // create fileset
    String filesetName = "test_fileset_delete";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs delete
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path deletePath = new Path(gvfsPath + "/" + fileName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(deletePath);
        Assertions.assertTrue(gvfs.exists(deletePath));
        Assertions.assertTrue(gvfs.getFileStatus(deletePath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
      }
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        gvfs.delete(deletePath, true);
        Assertions.assertFalse(gvfs.exists(deletePath));
        Assertions.assertFalse(fs.exists(new Path(storageLocation + "/" + fileName)));
      }
    }
  }

  @Test
  public void testGetStatus() throws IOException {
    // create fileset
    String filesetName = "test_fileset_get_status";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs get status
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path statusPath = new Path(gvfsPath + "/" + fileName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(statusPath);
        Assertions.assertTrue(gvfs.exists(statusPath));
        Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
        FileStatus gvfsStatus = gvfs.getFileStatus(statusPath);
        FileStatus hdfsStatus = fs.getFileStatus(new Path(storageLocation + "/" + fileName));
        Assertions.assertEquals(
            hdfsStatus.getPath().toString(),
            gvfsStatus
                .getPath()
                .toString()
                .replaceFirst(genGvfsPath(filesetName).toString(), storageLocation));
      }
    }
  }

  @Test
  public void testListStatus() throws IOException {
    // create fileset
    String filesetName = "test_fileset_list_status";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs list status
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      for (int i = 0; i < 10; i++) {
        String fileName = "test_" + i + ".txt";
        Path statusPath = new Path(gvfsPath + "/" + fileName);
        try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
          Assertions.assertTrue(gvfs.exists(gvfsPath));
          gvfs.create(statusPath);
          Assertions.assertTrue(gvfs.exists(statusPath));
          Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
          Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
        }
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        List<FileStatus> gvfsStatus = new ArrayList<>(Arrays.asList(gvfs.listStatus(gvfsPath)));
        gvfsStatus.sort(Comparator.comparing(FileStatus::getPath));
        assertEquals(10, gvfsStatus.size());

        List<FileStatus> hdfsStatus = new ArrayList<>(Arrays.asList(fs.listStatus(hdfsPath)));
        hdfsStatus.sort(Comparator.comparing(FileStatus::getPath));
        assertEquals(10, hdfsStatus.size());

        for (int i = 0; i < 10; i++) {
          assertEquals(
              hdfsStatus.get(i).getPath().toString(),
              gvfsStatus
                  .get(i)
                  .getPath()
                  .toString()
                  .replaceFirst(genGvfsPath(filesetName).toString(), storageLocation));
        }
      }
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    // create fileset
    String filesetName = "test_fileset_mkdirs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs mkdirs
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        String dirName = "test";
        Path dirPath = new Path(gvfsPath + "/" + dirName);
        gvfs.mkdirs(dirPath);
        Assertions.assertTrue(gvfs.exists(dirPath));
        Assertions.assertTrue(gvfs.getFileStatus(dirPath).isDirectory());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + dirName)));
      }
    }
  }

  @Test
  public void testRename() throws IOException {
    // create fileset
    String filesetName = "test_fileset_rename";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs rename
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String srcName = "test_src";
      Path srcPath = new Path(gvfsPath + "/" + srcName);

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.mkdirs(srcPath);
        Assertions.assertTrue(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(srcPath).isDirectory());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + srcName)));
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        String dstName = "test_dst";
        Path dstPath = new Path(gvfsPath + "/" + dstName);
        gvfs.rename(srcPath, dstPath);
        Assertions.assertTrue(gvfs.exists(dstPath));
        Assertions.assertFalse(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(dstPath).isDirectory());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + dstName)));
        Assertions.assertFalse(fs.exists(new Path(storageLocation + "/" + srcName)));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAppendWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    // create fileset
    String filesetName = "test_fileset_append_multiple_locs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    Path gvfsPath = genGvfsPath(filesetName);

    // Only create a fileset once
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs append
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    try (FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf);
        FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      String fileName = "test.txt";
      Path appendPath = new Path(gvfsPath + "/" + fileName);

      Assertions.assertTrue(gvfs.exists(gvfsPath));
      if (isTestPrimaryLocation) {
        gvfs.create(appendPath);
        Assertions.assertTrue(gvfs.exists(appendPath));
        Assertions.assertTrue(gvfs.getFileStatus(appendPath).isFile());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      } else {
        Assertions.assertTrue(gvfs.exists(appendPath));
        Assertions.assertTrue(gvfs.getFileStatus(appendPath).isFile());
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      }

      gvfs.close();

      if (isTestPrimaryLocation) {
        try (FileSystem gvfs1 = gvfsPath.getFileSystem(conf)) {
          try (FSDataOutputStream outputStream = gvfs1.append(appendPath, 3)) {
            // Hello, World!
            byte[] wordsBytes =
                new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
            outputStream.write(wordsBytes);
          }
          try (FSDataInputStream inputStream = gvfs1.open(appendPath, 3)) {
            int bytesRead;
            byte[] buffer = new byte[1024];
            try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
              while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteOutputStream.write(buffer, 0, bytesRead);
              }
              assertEquals(
                  "Hello, World!",
                  new String(byteOutputStream.toByteArray(), StandardCharsets.UTF_8));
            }
          }
        }
      } else {
        try (FileSystem gvfs2 = gvfsPath.getFileSystem(conf)) {

          // The appendPath exists at the backup storage location.
          try (FSDataOutputStream outputStream = gvfs2.append(appendPath, 3)) {
            // Hello, World!
            byte[] wordsBytes =
                new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
            outputStream.write(wordsBytes);
          }
          try (FSDataInputStream inputStream = gvfs2.open(appendPath, 3)) {
            int bytesRead;
            byte[] buffer = new byte[1024];
            try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
              while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteOutputStream.write(buffer, 0, bytesRead);
              }
              assertEquals(
                  "Hello, World!Hello, World!",
                  new String(byteOutputStream.toByteArray(), StandardCharsets.UTF_8));
            }
          }

          // The appendPath both exist at the primary and backup storage locations, throw
          // IllegalArgumentException.
          primaryFs.create(new Path(storageLocation + "/" + fileName), true);
          Assertions.assertTrue(gvfs2.exists(appendPath));
          Assertions.assertTrue(gvfs2.getFileStatus(appendPath).isFile());
          Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
          Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
          Assertions.assertThrowsExactly(
              IllegalArgumentException.class, () -> gvfs2.append(appendPath));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    // create fileset
    String filesetName = "test_fileset_create_multiple_locs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once.
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs create
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf);
        FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));

      Assertions.assertTrue(gvfs.exists(gvfsPath));
      String subDir = "sub_dir";
      Path mkdirPath = new Path(gvfsPath + "/" + subDir);
      String fileName = subDir + "/test.txt";
      Path createPath = new Path(gvfsPath + "/" + fileName);

      if (isTestPrimaryLocation) {
        gvfs.mkdirs(mkdirPath);
        Assertions.assertTrue(gvfs.exists(mkdirPath));
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + subDir)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + subDir)));

        gvfs.create(createPath);
        Assertions.assertTrue(gvfs.exists(createPath));
        Assertions.assertTrue(gvfs.getFileStatus(createPath).isFile());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      } else {
        gvfs.create(createPath, true);
        Assertions.assertTrue(gvfs.exists(createPath));
        Assertions.assertTrue(gvfs.getFileStatus(createPath).isFile());
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

        // Only support no more than one actual path exists.
        primaryFs.create(new Path(storageLocation + "/" + fileName));
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
        Assertions.assertTrue(gvfs.exists(createPath));
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> gvfs.create(createPath, true));
      }
    }
  }

  @Test
  public void testCreateWithDoubleWrite() throws IOException {
    // create fileset
    String filesetName = "test_fileset_create_with_double_write";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    storageLocation = genStorageLocation(filesetName);
    backupStorageLocation = genBackupStorageLocation(filesetName);
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs create
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf);
        FileSystem gvfs = gvfsPath.getFileSystem(newConf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));

      Assertions.assertTrue(gvfs.exists(gvfsPath));
      String fileName = "test.txt";
      Path createPath = new Path(gvfsPath + "/" + fileName);

      // Create a file in the primary location when double write is enabled
      backupFs.create(new Path(backupStorageLocation + "/" + fileName));
      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
      Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      gvfs.create(createPath, true);
      Assertions.assertTrue(gvfs.exists(createPath));
      Assertions.assertTrue(gvfs.getFileStatus(createPath).isFile());
      Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
      Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeleteWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    // create fileset
    String filesetName = "test_fileset_delete_multiple_locs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs delete
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf);
        FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      String fileName = "test.txt";
      Path deletePath = new Path(gvfsPath + "/" + fileName);

      Assertions.assertTrue(gvfs.exists(gvfsPath));
      if (isTestPrimaryLocation) {
        gvfs.create(deletePath);
        Assertions.assertTrue(gvfs.exists(deletePath));
        Assertions.assertTrue(gvfs.getFileStatus(deletePath).isFile());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      } else {
        Assertions.assertTrue(gvfs.exists(deletePath));
        Assertions.assertTrue(gvfs.getFileStatus(deletePath).isFile());
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      }

      if (isTestPrimaryLocation) {
        gvfs.delete(deletePath, true);
        Assertions.assertFalse(gvfs.exists(deletePath));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

        // test actual paths do not exist.
        Assertions.assertFalse(gvfs.delete(deletePath, true));
      } else {
        gvfs.delete(deletePath, true);
        Assertions.assertFalse(gvfs.exists(deletePath));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

        // Only create a fileset once
        primaryFs.create(new Path(storageLocation + "/" + fileName));
        backupFs.create(new Path(backupStorageLocation + "/" + fileName));
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
        Assertions.assertTrue(gvfs.exists(deletePath));
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> gvfs.delete(deletePath, true));
      }

      if (isTestPrimaryLocation) {
        gvfs.create(deletePath);
        Assertions.assertTrue(gvfs.exists(deletePath));
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      }
    }
  }

  @Test
  public void testDeleteWithDoubleWrite() throws IOException {
    // create fileset
    String filesetName = "test_fileset_delete_with_double_write";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    storageLocation = genStorageLocation(filesetName);
    backupStorageLocation = genBackupStorageLocation(filesetName);
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs delete
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf);
        FileSystem gvfs = gvfsPath.getFileSystem(newConf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      String fileName = "test.txt";
      Path deletePath = new Path(gvfsPath + "/" + fileName);

      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
      primaryFs.create(new Path(storageLocation + "/" + fileName));
      backupFs.create(new Path(backupStorageLocation + "/" + fileName));
      Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
      Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

      Assertions.assertTrue(gvfs.exists(deletePath));
      // Only delete the file in the primary location if enable double write.
      gvfs.delete(deletePath, true);
      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
      Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

      // Only delete the file in the primary location if enable double write.
      gvfs.delete(deletePath, true);
      Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetStatusWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    // create fileset
    String filesetName = "test_fileset_get_status_multiple_locs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs get fileStatus
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    try (FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path statusPath = new Path(gvfsPath + "/" + fileName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        if (isTestPrimaryLocation) {
          gvfs.create(statusPath);
          Assertions.assertTrue(gvfs.exists(statusPath));
          Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
          Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
          Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

          FileStatus gvfsStatus = gvfs.getFileStatus(statusPath);
          FileStatus primaryFileStatus =
              primaryFs.getFileStatus(new Path(storageLocation + "/" + fileName));
          Assertions.assertEquals(
              primaryFileStatus.getPath().toString(),
              gvfsStatus
                  .getPath()
                  .toString()
                  .replaceFirst(genGvfsPath(filesetName).toString(), storageLocation));
        } else {
          Assertions.assertTrue(gvfs.exists(statusPath));
          Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
          Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
          Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

          FileStatus gvfsStatus = gvfs.getFileStatus(statusPath);
          FileStatus backupFileStatus =
              backupFs.getFileStatus(new Path(backupStorageLocation + "/" + fileName));
          Assertions.assertEquals(
              backupFileStatus.getPath().toString(),
              gvfsStatus
                  .getPath()
                  .toString()
                  .replaceFirst(genGvfsPath(filesetName).toString(), backupStorageLocation));
        }
      }
    }
  }

  @Test
  public void testGetStatusWithDoubleWrite() throws IOException {
    // create fileset
    String filesetName = "test_fileset_get_status_with_double_write";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    storageLocation = genStorageLocation(filesetName);
    backupStorageLocation = genBackupStorageLocation(filesetName);
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    // test gvfs get fileStatus
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    try (FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path statusPath = new Path(gvfsPath + "/" + fileName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(newConf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));

        gvfs.create(statusPath);
        Assertions.assertTrue(gvfs.exists(statusPath));
        Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));

        FileStatus gvfsStatus = gvfs.getFileStatus(statusPath);
        FileStatus primaryFileStatus =
            primaryFs.getFileStatus(new Path(storageLocation + "/" + fileName));
        Assertions.assertEquals(
            primaryFileStatus.getPath().toString(),
            gvfsStatus
                .getPath()
                .toString()
                .replaceFirst(genGvfsPath(filesetName).toString(), storageLocation));

        // only support get file status in the primary location when enable double write.
        primaryFs.delete(new Path(storageLocation + "/" + fileName), false);
        backupFs.create(new Path(backupStorageLocation + "/" + fileName));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
        Assertions.assertFalse(gvfs.exists(statusPath));
        assertThrowsExactly(
            FileNotFoundException.class, () -> gvfs.getFileStatus(statusPath).isDirectory());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  public void testListStatusWithMultipleLocs(
      boolean isTestPrimaryLocation, boolean isWritePrimaryOnly) throws IOException {
    // create fileset
    String filesetName =
        String.format("%s_%s", "test_fileset_list_status_multiple_locs", isWritePrimaryOnly);
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, isWritePrimaryOnly);

    // test gvfs list status
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf);
        FileSystem gvfs = gvfsPath.getFileSystem(newConf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      for (int i = 0; i < 10; i++) {
        String fileName = "test_" + i + ".txt";
        Path statusPath = new Path(gvfsPath + "/" + fileName);
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(statusPath);
        Assertions.assertTrue(gvfs.exists(statusPath));
        Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
        if (isTestPrimaryLocation) {
          Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
          Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
        } else {
          if (isWritePrimaryOnly) {
            Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
            Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
            Assertions.assertTrue(
                backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
          } else {
            Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
            primaryFs.create(new Path(storageLocation + "/" + fileName));
            Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + fileName)));
            Assertions.assertTrue(
                backupFs.exists(new Path(backupStorageLocation + "/" + fileName)));
          }
        }
      }

      List<FileStatus> gvfsStatus = new ArrayList<>(Arrays.asList(gvfs.listStatus(gvfsPath)));
      gvfsStatus.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(10, gvfsStatus.size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMkdirsWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    // create fileset
    String filesetName = "test_fileset_mkdirs_multiple_locs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs mkdirs
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    try (FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        String dirName = "test";
        Path dirPath = new Path(gvfsPath + "/" + dirName);
        if (isTestPrimaryLocation) {
          gvfs.mkdirs(dirPath);
          Assertions.assertTrue(gvfs.exists(dirPath));
          Assertions.assertTrue(gvfs.getFileStatus(dirPath).isDirectory());
          Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dirName)));
          Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dirName)));
        } else {
          // Return true when the dir already exists.
          assertTrue(gvfs.mkdirs(dirPath));

          Assertions.assertTrue(gvfs.exists(dirPath));
          Assertions.assertTrue(gvfs.delete(dirPath, true));
          gvfs.mkdirs(dirPath);
          Assertions.assertTrue(gvfs.exists(dirPath));
          Assertions.assertTrue(gvfs.getFileStatus(dirPath).isDirectory());
          Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dirName)));
          Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dirName)));
        }
      }
    }
  }

  @Test
  public void testMkdirsWithDoubleWrite() throws IOException {
    // create fileset
    String filesetName = "test_fileset_mkdirs_with_double_write";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    storageLocation = genStorageLocation(filesetName);
    backupStorageLocation = genBackupStorageLocation(filesetName);
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    // test gvfs mkdirs
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    try (FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(newConf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        String dirName = "test";
        Path dirPath = new Path(gvfsPath + "/" + dirName);

        gvfs.mkdirs(dirPath);
        Assertions.assertTrue(gvfs.exists(dirPath));
        Assertions.assertTrue(gvfs.getFileStatus(dirPath).isDirectory());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dirName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dirName)));

        // Return true when the dir already exists.
        assertTrue(gvfs.mkdirs(dirPath));

        // support make a new dir in primary location when enable
        // fs.gravitino.fileset.write.primaryOnly.
        primaryFs.delete(new Path(storageLocation + "/" + dirName), true);
        backupFs.mkdirs(new Path(backupStorageLocation + "/" + dirName));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + dirName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + dirName)));
        assertTrue(gvfs.mkdirs(dirPath));
        Assertions.assertTrue(gvfs.exists(dirPath));
        Assertions.assertTrue(gvfs.getFileStatus(dirPath).isDirectory());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dirName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + dirName)));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRenameWithMultiLocs(boolean isTestPrimaryLocation) throws IOException {
    // create fileset
    String filesetName = "test_fileset_rename_multiple_locs";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    if (isTestPrimaryLocation) {
      storageLocation = genStorageLocation(filesetName);
      backupStorageLocation = genBackupStorageLocation(filesetName);
    } else {
      storageLocation = genBackupStorageLocation(filesetName);
      backupStorageLocation = genStorageLocation(filesetName);
    }
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);

    // Only create a fileset once
    if (isTestPrimaryLocation) {
      catalog
          .asFilesetCatalog()
          .createFileset(
              filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    } else {
      catalog
          .asFilesetCatalog()
          .alterFileset(
              filesetIdent,
              FilesetChange.switchPrimaryAndBackupStorageLocation(
                  FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1));
    }
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs rename
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf);
        FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));

      String srcName = "test_src";
      Path srcPath = new Path(gvfsPath + "/" + srcName);
      Assertions.assertTrue(gvfs.exists(gvfsPath));
      if (isTestPrimaryLocation) {
        gvfs.mkdirs(srcPath);
        Assertions.assertTrue(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(srcPath).isDirectory());
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
      } else {
        Assertions.assertTrue(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(srcPath).isDirectory());
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
      }

      String dstName = "test_dst";
      Path dstPath = new Path(gvfsPath + "/" + dstName);
      if (isTestPrimaryLocation) {
        gvfs.rename(srcPath, dstPath);
        Assertions.assertTrue(gvfs.exists(dstPath));
        Assertions.assertFalse(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(dstPath).isDirectory());
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));

        // test rename when the dstPath exists.
        gvfs.mkdirs(srcPath);
        Assertions.assertTrue(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.exists(dstPath));
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> gvfs.rename(srcPath, dstPath));

        // delete the dstPath.
        gvfs.delete(dstPath, true);
        Assertions.assertFalse(gvfs.exists(dstPath));
        Assertions.assertTrue(gvfs.exists(srcPath));
      } else {
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));
        Assertions.assertTrue(gvfs.rename(srcPath, dstPath));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));

        // The src path both exist at the primary and backup storage location, throw
        // IllegalArgumentException.
        primaryFs.mkdirs(new Path(storageLocation + "/" + srcName));
        backupFs.mkdirs(new Path(backupStorageLocation + "/" + srcName));
        backupFs.delete(new Path(backupStorageLocation + "/" + dstName), true);
        Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
        Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
        Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
        Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));
        Assertions.assertTrue(gvfs.exists(srcPath));
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> gvfs.rename(dstPath, srcPath));
      }
    }
  }

  @Test
  public void testRenameWithDoubleWrite() throws IOException {
    // create fileset
    String filesetName = "test_fileset_rename_with_double_write";
    NameIdentifier filesetIdent =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    String storageLocation;
    String backupStorageLocation;
    Map<String, String> props;
    storageLocation = genStorageLocation(filesetName);
    backupStorageLocation = genBackupStorageLocation(filesetName);
    props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupStorageLocation);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent, "fileset comment", Fileset.Type.MANAGED, storageLocation, props);
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    // test gvfs rename
    Path primaryPath = new Path(storageLocation);
    Path backupPath = new Path(backupStorageLocation);
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf);
        FileSystem gvfs = gvfsPath.getFileSystem(newConf)) {
      Assertions.assertTrue(primaryFs.exists(primaryPath));
      Assertions.assertTrue(backupFs.exists(backupPath));

      String srcName = "test_src";
      Path srcPath = new Path(gvfsPath + "/" + srcName);
      Assertions.assertTrue(gvfs.exists(gvfsPath));
      gvfs.mkdirs(srcPath);
      Assertions.assertTrue(gvfs.exists(srcPath));
      Assertions.assertTrue(gvfs.getFileStatus(srcPath).isDirectory());
      Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));

      String dstName = "test_dst";
      Path dstPath = new Path(gvfsPath + "/" + dstName);
      Assertions.assertTrue(gvfs.rename(srcPath, dstPath));
      Assertions.assertTrue(gvfs.exists(dstPath));
      Assertions.assertFalse(gvfs.exists(srcPath));
      Assertions.assertTrue(gvfs.getFileStatus(dstPath).isDirectory());
      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
      Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));

      // delete the dstPath.
      primaryFs.delete(new Path(storageLocation + "/" + srcName), true);
      primaryFs.delete(new Path(storageLocation + "/" + dstName), true);
      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));

      // only support rename the path in primary location when enable
      // fs.gravitino.fileset.write.primaryOnly.
      backupFs.mkdirs(new Path(backupStorageLocation + "/" + srcName));
      Assertions.assertFalse(gvfs.rename(srcPath, dstPath));
      primaryFs.mkdirs(new Path(storageLocation + "/" + srcName));
      Assertions.assertTrue(gvfs.rename(srcPath, dstPath));
      Assertions.assertFalse(primaryFs.exists(new Path(storageLocation + "/" + srcName)));
      Assertions.assertTrue(primaryFs.exists(new Path(storageLocation + "/" + dstName)));
      Assertions.assertTrue(backupFs.exists(new Path(backupStorageLocation + "/" + srcName)));
      Assertions.assertFalse(backupFs.exists(new Path(backupStorageLocation + "/" + dstName)));
    }
  }

  private String genStorageLocation(String fileset) {
    return String.format("%s/%s", baseHdfsPath(), fileset);
  }

  private String genBackupStorageLocation(String fileset) {
    return String.format("%s/%s", baseHdfsPath(), fileset + "_bak");
  }

  private static String baseHdfsPath() {
    return String.format(
        "hdfs://%s:%d/%s/%s",
        containerSuite.getHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT,
        catalogName,
        schemaName);
  }

  private Path genGvfsPath(String fileset) {
    return new Path(String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, fileset));
  }
}
