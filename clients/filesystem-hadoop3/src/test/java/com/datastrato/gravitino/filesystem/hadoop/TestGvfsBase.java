/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import com.datastrato.gravitino.shaded.com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGvfsBase extends GravitinoMockServerBase {
  protected static final String GVFS_IMPL_CLASS = GravitinoVirtualFileSystem.class.getName();
  protected static final String GVFS_ABSTRACT_IMPL_CLASS = Gvfs.class.getName();
  protected static Configuration conf = new Configuration();
  protected Path localDirPath = null;
  protected Path localFilePath = null;
  protected Path managedFilesetPath = null;
  protected Path externalFilesetPath = null;

  @BeforeAll
  public static void setup() {
    GravitinoMockServerBase.setup();
    conf.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    conf.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        GravitinoMockServerBase.serverUri());
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    // close the cache
    conf.set(
        String.format(
            "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
        "true");
  }

  @AfterAll
  public static void tearDown() {
    GravitinoMockServerBase.tearDown();
  }

  @BeforeEach
  public void init() {
    mockMetalakeDTO(metalakeName, "comment");
    mockCatalogDTO(catalogName, provider, "comment");

    localDirPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, managedFilesetName);
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        managedFilesetName,
        Fileset.Type.MANAGED,
        localDirPath.toString(),
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            String.valueOf(20)));
    managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, managedFilesetName, true);

    localFilePath =
        new Path(
            FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, externalFilesetName)
                + "/test.txt");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        externalFilesetName,
        Fileset.Type.EXTERNAL,
        localFilePath.toString(),
        ImmutableMap.of(
            FilesetProperties.PREFIX_PATTERN_KEY,
            FilesetPrefixPattern.ANY.name(),
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            String.valueOf(20)));
    externalFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, externalFilesetName, true);
  }

  @AfterEach
  public void destroy() throws IOException {
    Path localRootPath = FileSystemTestUtils.createLocalRootDir(catalogName);
    try (FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      if (localFileSystem.exists(localRootPath)) {
        localFileSystem.delete(localRootPath, true);
      }
    }
  }

  @Test
  public void testFSCache() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      Configuration conf1 = localFileSystem.getConf();
      assertEquals(
          "true",
          conf1.get(
              String.format(
                  "fs.%s.impl.disable.cache",
                  GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));

      Configuration conf2 = gravitinoFileSystem.getConf();
      assertEquals(
          "true",
          conf2.get(
              String.format(
                  "fs.%s.impl.disable.cache",
                  GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));

      // test gvfs, should not get the same fs
      try (FileSystem externalFs = externalFilesetPath.getFileSystem(conf)) {
        assertNotEquals(externalFs, gravitinoFileSystem);
      }

      // test proxyed local fs, should not get the same fs
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      FileSystem proxyLocalFs =
          Objects.requireNonNull(
                  ((GravitinoVirtualFileSystem) gravitinoFileSystem)
                      .getFilesetCache()
                      .getIfPresent(
                          NameIdentifier.of(
                              metalakeName, catalogName, schemaName, managedFilesetName)))
              .getRight();

      String anotherFilesetName = "test_new_fs";
      Path diffLocalPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, anotherFilesetName);
      try (FileSystem localFs = diffLocalPath.getFileSystem(conf)) {
        assertNotEquals(localFs, proxyLocalFs);
        localFs.delete(diffLocalPath, true);
      }
    }
  }

  @Test
  public void testInternalCache() throws IOException {
    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY, "1");
    configuration.set(
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
        "1000");

    Path filesetPath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "fileset1", true);
    try (FileSystem fs = filesetPath1.getFileSystem(configuration)) {
      Path localPath1 =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "fileset1");
      mockFilesetDTO(
          metalakeName,
          catalogName,
          schemaName,
          "fileset1",
          Fileset.Type.MANAGED,
          localPath1.toString(),
          ImmutableMap.of(
              FilesetProperties.PREFIX_PATTERN_KEY,
              FilesetPrefixPattern.ANY.name(),
              FilesetProperties.DIR_MAX_LEVEL_KEY,
              String.valueOf(20)));
      FileSystemTestUtils.mkdirs(filesetPath1, fs);

      // expired by size
      Path filesetPath2 =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "fileset2", true);
      Path localPath2 =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "fileset2");
      mockFilesetDTO(
          metalakeName,
          catalogName,
          schemaName,
          "fileset2",
          Fileset.Type.MANAGED,
          localPath2.toString(),
          ImmutableMap.of(
              FilesetProperties.PREFIX_PATTERN_KEY,
              FilesetPrefixPattern.ANY.name(),
              FilesetProperties.DIR_MAX_LEVEL_KEY,
              String.valueOf(20)));
      FileSystemTestUtils.mkdirs(filesetPath2, fs);

      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertNull(
                      ((GravitinoVirtualFileSystem) fs)
                          .getFilesetCache()
                          .getIfPresent(
                              NameIdentifier.of(
                                  metalakeName, catalogName, schemaName, "fileset1"))));

      // expired by time
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertEquals(
                      0, ((GravitinoVirtualFileSystem) fs).getFilesetCache().asMap().size()));

      assertNull(
          ((GravitinoVirtualFileSystem) fs)
              .getFilesetCache()
              .getIfPresent(NameIdentifier.of(metalakeName, catalogName, schemaName, "fileset2")));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreate(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset create
      Path filePath = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(filePath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(filePath));
      gravitinoFileSystem.delete(filePath, true);

      // mock the invalid fileset not in the server
      String invalidFilesetName = "invalid_fileset";
      Path invalidFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidFilesetName, withScheme);
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.create(invalidFilesetPath, gravitinoFileSystem));

      // mock the not correct protocol prefix path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.create(localPrefixPath, gravitinoFileSystem));

      // test external fileset mounts a single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(externalFilesetPath).isFile());
      gravitinoFileSystem.delete(externalFilesetPath, true);
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @Test
  public void testCreateWithPathLimit() throws IOException {
    // test invalid properties
    String filesetInvalidProperties = "fileset_invalid_properties";
    Path localFilesetInvalidPropertiesPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetInvalidProperties);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetInvalidProperties,
        Fileset.Type.MANAGED,
        localFilesetInvalidPropertiesPath.toString(),
        properties);
    Path filesetInvalidPropertiesPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, filesetInvalidProperties, true);
    try (FileSystem gravitinoFileSystem = filesetInvalidPropertiesPath.getFileSystem(conf)) {
      // test
      Path invalidPrefixPath = new Path(filesetInvalidPropertiesPath + "/xxx/test.txt");
      assertThrows(
          IllegalArgumentException.class,
          () -> FileSystemTestUtils.create(invalidPrefixPath, gravitinoFileSystem));
    }

    // test prefix = DATE_WITH_STRING and max level = 3
    String filesetWithDateString = "fileset_date_with_string";
    Path localFilesetWithDateStringPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetWithDateString);
    properties.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetWithDateString,
        Fileset.Type.MANAGED,
        localFilesetWithDateStringPath.toString(),
        properties);
    Path filesetWithDateStringPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetWithDateString, true);
    try (FileSystem gravitinoFileSystem = filesetWithDateStringPath.getFileSystem(conf)) {
      // test invalid prefix
      Path invalidPrefixPath = new Path(filesetWithDateStringPath + "/xxx/test.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.create(invalidPrefixPath, gravitinoFileSystem));

      // test invalid level
      Path invalidLevelPath =
          new Path(filesetWithDateStringPath + "/date=20240408/xxx/zzz/qqq/test.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.create(invalidLevelPath, gravitinoFileSystem));

      // test valid path 1
      Path validPath1 = new Path(filesetWithDateStringPath + "/date=20240408/xxx/zzz/test.txt");
      FileSystemTestUtils.create(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test valid path 2
      Path validPath2 = new Path(filesetWithDateStringPath + "/date=20240408/xxx/test.txt");
      FileSystemTestUtils.create(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);

      // test valid path 3
      Path validPath3 = new Path(filesetWithDateStringPath + "/date=20240408/test.txt");
      FileSystemTestUtils.create(validPath3, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath3));
      gravitinoFileSystem.delete(validPath3, true);
    }

    // test prefix = YEAR_MONTH_DAY and max level = 5
    String filesetYMD = "fileset_year_month_day";
    Path localFilesetYMDPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetYMD);
    properties.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.YEAR_MONTH_DAY.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(5));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetYMD,
        Fileset.Type.MANAGED,
        localFilesetYMDPath.toString(),
        properties);
    Path filesetYMDPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetYMD, true);
    try (FileSystem gravitinoFileSystem = filesetYMDPath.getFileSystem(conf)) {
      // test invalid prefix
      Path invalidPrefixPath = new Path(filesetYMDPath + "/xxx/test.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.create(invalidPrefixPath, gravitinoFileSystem));

      // test invalid level
      Path invalidLevelPath =
          new Path(filesetYMDPath + "/year=2024/month=04/day=08/xxx/zzz/qqq/test.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.create(invalidLevelPath, gravitinoFileSystem));

      // test valid path 1
      Path validPath1 = new Path(filesetYMDPath + "/year=2024/month=04/day=08/xxx/zzz/test.txt");
      FileSystemTestUtils.create(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test valid path 2
      Path validPath2 = new Path(filesetYMDPath + "/year=2024/month=04/day=08/xxx/test.txt");
      FileSystemTestUtils.create(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);

      // test valid path 3
      Path validPath3 = new Path(filesetYMDPath + "/year=2024/month=04/day=08/test.txt");
      FileSystemTestUtils.create(validPath3, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath3));
      gravitinoFileSystem.delete(validPath3, true);
    }

    // test prefix = ANY and max level = 3
    String filesetAny = "fileset_any";
    Path localFilesetAnyPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetAny);
    properties.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.ANY.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetAny,
        Fileset.Type.MANAGED,
        localFilesetAnyPath.toString(),
        properties);
    Path filesetAnyPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetAny, true);
    try (FileSystem gravitinoFileSystem = filesetAnyPath.getFileSystem(conf)) {
      // test invalid level
      Path invalidLevelPath = new Path(filesetAnyPath + "/year=2024/month=04/day=08/xxx/qqq.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.create(invalidLevelPath, gravitinoFileSystem));

      // test valid path 1
      Path validPath1 = new Path(filesetAnyPath + "/year=2024/month=04/day=08/xxx.txt");
      FileSystemTestUtils.create(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test valid path 2
      Path validPath2 = new Path(filesetAnyPath + "/xxx.txt");
      FileSystemTestUtils.create(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);
    }

    // test prefix = DATE_WITH_STRING and max level = 3
    String filesetD1 = "fileset_date_1";
    Path localFilesetD1Path =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetD1);
    properties.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetD1,
        Fileset.Type.MANAGED,
        localFilesetD1Path.toString(),
        properties);
    Path filesetD1Path =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetD1, true);
    try (FileSystem gravitinoFileSystem = filesetD1Path.getFileSystem(conf)) {
      // test temporary dir 1
      Path validPath1 = new Path(filesetD1Path + "/date=20240408/_temporary/xxx/ddd/zzz.parquet");
      FileSystemTestUtils.create(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test temporary dir 2
      Path validPath2 = new Path(filesetD1Path + "/date=20240408/.temporary/xxx/ddd/zzz.parquet");
      FileSystemTestUtils.create(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @Disabled("Append operation is not supported in LocalFileSystem. We can't test it now.")
  public void testAppend(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset append
      Path appendFile = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(appendFile, gravitinoFileSystem);
      FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(appendFile));
      assertTrue(gravitinoFileSystem.getFileStatus(appendFile).isFile());
      assertEquals(
          "Hello, World!",
          new String(
              FileSystemTestUtils.read(appendFile, gravitinoFileSystem), StandardCharsets.UTF_8));
      gravitinoFileSystem.delete(appendFile, true);

      // mock the invalid fileset not in server
      String invalidAppendFilesetName = "invalid_fileset";
      Path invalidAppendFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidAppendFilesetName, withScheme);
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.append(invalidAppendFilesetPath, gravitinoFileSystem));

      // mock the not correct protocol path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.append(localPrefixPath, gravitinoFileSystem));

      // test external fileset mounts the single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      FileSystemTestUtils.append(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(externalFilesetPath).isFile());
      gravitinoFileSystem.delete(externalFilesetPath, true);
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRename(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset rename
      Path srcRenamePath = new Path(managedFilesetPath + "/rename_src");
      gravitinoFileSystem.mkdirs(srcRenamePath);
      assertTrue(gravitinoFileSystem.getFileStatus(srcRenamePath).isDirectory());
      assertTrue(gravitinoFileSystem.exists(srcRenamePath));

      // cannot rename the identifier
      Path dstRenamePath1 =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "rename_dst1", withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(srcRenamePath, dstRenamePath1));

      Path dstRenamePath2 = new Path(managedFilesetPath + "/rename_dst2");
      gravitinoFileSystem.rename(srcRenamePath, dstRenamePath2);
      assertFalse(gravitinoFileSystem.exists(srcRenamePath));
      assertTrue(gravitinoFileSystem.exists(dstRenamePath2));
      gravitinoFileSystem.delete(dstRenamePath2, true);

      // test invalid src path
      Path invalidSrcPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_src_name", withScheme);
      Path validDstPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, managedFilesetName, withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

      // test invalid dst path
      Path invalidDstPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_dst_name", withScheme);
      assertThrows(
          RuntimeException.class,
          () -> gravitinoFileSystem.rename(managedFilesetPath, invalidDstPath));

      // test external fileset mount the single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(externalFilesetPath).isFile());

      Path dstPath =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "rename_dst", withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(externalFilesetPath, dstPath));
      localFileSystem.delete(localFilePath, true);
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @Test
  public void testRenameFileWithPathLimit() throws IOException {
    // test invalid properties
    String filesetInvalidProperties = "fileset_file_invalid_properties";
    Path localFilesetInvalidPropertiesPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetInvalidProperties);
    Map<String, String> properties1 = Maps.newHashMap();
    properties1.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetInvalidProperties,
        Fileset.Type.MANAGED,
        localFilesetInvalidPropertiesPath.toString(),
        properties1);
    Path filesetInvalidPropertiesPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, filesetInvalidProperties, true);
    try (FileSystem gravitinoFileSystem = filesetInvalidPropertiesPath.getFileSystem(conf)) {
      Path invalidSrcFilePath = new Path(filesetInvalidPropertiesPath + "/xxx/test.txt");
      Path invalidDstFilePath = new Path(filesetInvalidPropertiesPath + "/xxx/test1.txt");
      assertThrows(
          IllegalArgumentException.class,
          () -> gravitinoFileSystem.rename(invalidSrcFilePath, invalidDstFilePath));
    }

    // test valid properties but src is not existing
    String filesetValidProperties = "fileset_file_valid_properties";
    Path localFilesetValidPropertiesPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetValidProperties);
    Map<String, String> properties2 = Maps.newHashMap();
    properties2.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    properties2.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.ANY.name());
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetValidProperties,
        Fileset.Type.MANAGED,
        localFilesetValidPropertiesPath.toString(),
        properties2);
    Path filesetValidPropertiesPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, filesetValidProperties, true);
    try (FileSystem gravitinoFileSystem = filesetValidPropertiesPath.getFileSystem(conf)) {
      Path validSrcFilePath = new Path(filesetValidPropertiesPath + "/xxx/test.txt");
      Path validDstFilePath = new Path(filesetValidPropertiesPath + "/xxx/test1.txt");
      assertThrows(
          FileNotFoundException.class,
          () -> gravitinoFileSystem.rename(validSrcFilePath, validDstFilePath));
    }

    // test file rename with YMD
    String fileRename1 = "fileset_file_rename1";
    Map<String, String> properties3 = Maps.newHashMap();
    Path localRenamePath1 =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, fileRename1);
    properties3.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.YEAR_MONTH_DAY.name());
    properties3.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(5));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        fileRename1,
        Fileset.Type.MANAGED,
        localRenamePath1.toString(),
        properties3);
    Path filesetRenamePath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileRename1, true);
    try (FileSystem gravitinoFileSystem = filesetRenamePath1.getFileSystem(conf);
        FileSystem localFileSystem = localRenamePath1.getFileSystem(conf)) {
      // test invalid src level
      String subInvalidSrcLevelPath = "/year=2024/month=04/day=08/xxx/yyy/zzz/test.parquet";
      Path localInvalidSrcLevelPath = new Path(localRenamePath1 + subInvalidSrcLevelPath);
      Path gvfsInvalidSrcLevelPath = new Path(filesetRenamePath1 + subInvalidSrcLevelPath);
      localFileSystem.create(localInvalidSrcLevelPath);
      assertTrue(localFileSystem.exists(localInvalidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsInvalidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsInvalidSrcLevelPath, new Path(gvfsInvalidSrcLevelPath + ".zzz")));
      localFileSystem.delete(localInvalidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localInvalidSrcLevelPath));

      // test invalid dst level
      String subValidSrcLevelPath = "/year=2024/month=04/day=08/xxx/yyy/test.parquet";
      Path localValidSrcLevelPath = new Path(localRenamePath1 + subValidSrcLevelPath);
      Path gvfsValidSrcLevelPath = new Path(filesetRenamePath1 + subValidSrcLevelPath);
      localFileSystem.create(localValidSrcLevelPath);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsValidSrcLevelPath, new Path(gvfsValidSrcLevelPath + "/qqq.parquet")));
      localFileSystem.delete(localValidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localValidSrcLevelPath));

      // test valid src and dst
      String subValidSrcLevelPath1 = "/year=2024/month=04/day=08/xxx/test.parquet";
      String subValidDstLevelPath1 = "/year=2024/month=04/day=08/xxx/yyy/test.parquet";
      Path localValidSrcLevelPath1 = new Path(localRenamePath1 + subValidSrcLevelPath1);
      Path gvfsValidSrcLevelPath1 = new Path(filesetRenamePath1 + subValidSrcLevelPath1);
      Path gvfsValidDstLevelPath1 = new Path(filesetRenamePath1 + subValidDstLevelPath1);
      localFileSystem.create(localValidSrcLevelPath1);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsValidSrcLevelPath1, gvfsValidDstLevelPath1);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath1, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));

      // test valid src, invalid dst level
      String subValidSrcLevelPath2 = "/year=2024/month=04/day=08/xxx/test.parquet";
      String subInvalidDstLevelPath2 = "/year=2024/month=04/day=08/xxx/yyy/zzz/test.parquet";
      Path localValidSrcLevelPath2 = new Path(localRenamePath1 + subValidSrcLevelPath2);
      Path gvfsValidSrcLevelPath2 = new Path(filesetRenamePath1 + subValidSrcLevelPath2);
      Path gvfsInvalidDstLevelPath2 = new Path(filesetRenamePath1 + subInvalidDstLevelPath2);
      localFileSystem.create(localValidSrcLevelPath2);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath2));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
      assertThrows(
          InvalidPathException.class,
          () -> gravitinoFileSystem.rename(gvfsValidSrcLevelPath2, gvfsInvalidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidSrcLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
    }

    // test file rename with date string
    String fileRename2 = "fileset_file_rename2";
    Map<String, String> properties4 = Maps.newHashMap();
    Path localRenamePath2 =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, fileRename2);
    properties4.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
    properties4.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        fileRename2,
        Fileset.Type.MANAGED,
        localRenamePath2.toString(),
        properties4);
    Path filesetRenamePath2 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileRename2, true);
    try (FileSystem gravitinoFileSystem = filesetRenamePath2.getFileSystem(conf);
        FileSystem localFileSystem = localRenamePath2.getFileSystem(conf)) {
      // test invalid src level
      String subInvalidSrcLevelPath = "/date=20240408/xxx/yyy/zzz/test.parquet";
      Path localInvalidSrcLevelPath = new Path(localRenamePath2 + subInvalidSrcLevelPath);
      Path gvfsInvalidSrcLevelPath = new Path(filesetRenamePath2 + subInvalidSrcLevelPath);
      localFileSystem.create(localInvalidSrcLevelPath);
      assertTrue(localFileSystem.exists(localInvalidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsInvalidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsInvalidSrcLevelPath, new Path(gvfsInvalidSrcLevelPath + ".zzz")));
      localFileSystem.delete(localInvalidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localInvalidSrcLevelPath));

      // test invalid dst level
      String subValidSrcLevelPath = "/date=20240408/xxx/yyy/test.parquet";
      Path localValidSrcLevelPath = new Path(localRenamePath2 + subValidSrcLevelPath);
      Path gvfsValidSrcLevelPath = new Path(filesetRenamePath2 + subValidSrcLevelPath);
      localFileSystem.create(localValidSrcLevelPath);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsValidSrcLevelPath, new Path(gvfsValidSrcLevelPath + "/qqq.parquet")));
      localFileSystem.delete(localValidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localValidSrcLevelPath));

      // test valid src and dst
      String subValidSrcLevelPath1 = "/date=20240408/xxx/test.parquet";
      String subValidDstLevelPath1 = "/date=20240408/xxx/yyy/test.parquet";
      Path localValidSrcLevelPath1 = new Path(localRenamePath2 + subValidSrcLevelPath1);
      Path gvfsValidSrcLevelPath1 = new Path(filesetRenamePath2 + subValidSrcLevelPath1);
      Path gvfsValidDstLevelPath1 = new Path(filesetRenamePath2 + subValidDstLevelPath1);
      localFileSystem.create(localValidSrcLevelPath1);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsValidSrcLevelPath1, gvfsValidDstLevelPath1);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath1, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));

      // test valid src, invalid dst level
      String subValidSrcLevelPath2 = "/date=20240408/xxx/test.parquet";
      String subInvalidDstLevelPath2 = "/date=20240408/xxx/yyy/zzz/test.parquet";
      Path localValidSrcLevelPath2 = new Path(localRenamePath2 + subValidSrcLevelPath2);
      Path gvfsValidSrcLevelPath2 = new Path(filesetRenamePath2 + subValidSrcLevelPath2);
      Path gvfsInvalidDstLevelPath2 = new Path(filesetRenamePath2 + subInvalidDstLevelPath2);
      localFileSystem.create(localValidSrcLevelPath2);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath2));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
      assertThrows(
          InvalidPathException.class,
          () -> gravitinoFileSystem.rename(gvfsValidSrcLevelPath2, gvfsInvalidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidSrcLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));

      // test src temporary directory
      String tmpSrcLevelPath1 = "/date=20240408/_temporary/zzz/ddd/www.parquet";
      String subValidDstLevelPath2 = "/date=20240408/zz.parquet";
      Path localTmpSrcLevelPath1 = new Path(localRenamePath2 + tmpSrcLevelPath1);
      Path gvfsTmpSrcLevelPath1 = new Path(filesetRenamePath2 + tmpSrcLevelPath1);
      Path gvfsValidDstLevelPath2 = new Path(filesetRenamePath2 + subValidDstLevelPath2);
      localFileSystem.create(localTmpSrcLevelPath1);
      assertTrue(localFileSystem.exists(localTmpSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsTmpSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsTmpSrcLevelPath1, gvfsValidDstLevelPath2);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath2));

      // test dst temporary directory
      String subValidSrcLevelPath3 = "/date=20240408/www.parquet";
      String tmpDstLevelPath1 = "/date=20240408/_temp/ddd/xxx/zzz.parquet";
      Path localSrcLevelPath1 = new Path(localRenamePath2 + subValidSrcLevelPath3);
      Path gvfsSrcLevelPath1 = new Path(filesetRenamePath2 + subValidSrcLevelPath3);
      Path gvfsTmpDstLevelPath2 = new Path(filesetRenamePath2 + tmpDstLevelPath1);
      localFileSystem.create(localSrcLevelPath1);
      assertTrue(localFileSystem.exists(localSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsSrcLevelPath1, gvfsTmpDstLevelPath2);
      assertTrue(gravitinoFileSystem.exists(gvfsTmpDstLevelPath2));
      gravitinoFileSystem.delete(gvfsTmpDstLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsTmpDstLevelPath2));
    }

    // test file rename with any
    String fileRename3 = "fileset_file_rename3";
    Map<String, String> properties5 = Maps.newHashMap();
    Path localRenamePath3 =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, fileRename3);
    properties5.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.ANY.name());
    properties5.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        fileRename3,
        Fileset.Type.MANAGED,
        localRenamePath3.toString(),
        properties5);
    Path filesetRenamePath3 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileRename3, true);
    try (FileSystem gravitinoFileSystem = filesetRenamePath3.getFileSystem(conf);
        FileSystem localFileSystem = localRenamePath3.getFileSystem(conf)) {
      // test invalid src level
      String subInvalidSrcLevelPath = "/date=20240408/xxx/yyy/zzz/test.parquet";
      Path localInvalidSrcLevelPath = new Path(localRenamePath3 + subInvalidSrcLevelPath);
      Path gvfsInvalidSrcLevelPath = new Path(filesetRenamePath3 + subInvalidSrcLevelPath);
      localFileSystem.create(localInvalidSrcLevelPath);
      assertTrue(localFileSystem.exists(localInvalidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsInvalidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsInvalidSrcLevelPath, new Path(gvfsInvalidSrcLevelPath + ".zzz")));
      localFileSystem.delete(localInvalidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localInvalidSrcLevelPath));

      // test invalid dst level
      String subValidSrcLevelPath = "/date=20240408/xxx/yyy/test.parquet";
      Path localValidSrcLevelPath = new Path(localRenamePath3 + subValidSrcLevelPath);
      Path gvfsValidSrcLevelPath = new Path(filesetRenamePath3 + subValidSrcLevelPath);
      localFileSystem.create(localValidSrcLevelPath);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsValidSrcLevelPath, new Path(gvfsValidSrcLevelPath + "/qqq.parquet")));
      localFileSystem.delete(localValidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localValidSrcLevelPath));

      // test valid src and dst
      String subValidSrcLevelPath1 = "/xxx/test.parquet";
      String subValidDstLevelPath1 = "/test.parquet";
      Path localValidSrcLevelPath1 = new Path(localRenamePath3 + subValidSrcLevelPath1);
      Path gvfsValidSrcLevelPath1 = new Path(filesetRenamePath3 + subValidSrcLevelPath1);
      Path gvfsValidDstLevelPath1 = new Path(filesetRenamePath3 + subValidDstLevelPath1);
      localFileSystem.create(localValidSrcLevelPath1);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsValidSrcLevelPath1, gvfsValidDstLevelPath1);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath1, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));

      // test valid src, invalid dst level
      String subValidSrcLevelPath2 = "/xxx/test.parquet";
      String subInvalidDstLevelPath2 = "/qqq/zzz/rrr/aaa/test.parquet";
      Path localValidSrcLevelPath2 = new Path(localRenamePath3 + subValidSrcLevelPath2);
      Path gvfsValidSrcLevelPath2 = new Path(filesetRenamePath3 + subValidSrcLevelPath2);
      Path gvfsInvalidDstLevelPath2 = new Path(filesetRenamePath3 + subInvalidDstLevelPath2);
      localFileSystem.create(localValidSrcLevelPath2);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath2));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
      assertThrows(
          InvalidPathException.class,
          () -> gravitinoFileSystem.rename(gvfsValidSrcLevelPath2, gvfsInvalidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidSrcLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
    }
  }

  @Test
  public void testRenameDirWithPathLimit() throws IOException {
    // test invalid properties
    String filesetInvalidProperties = "fileset_dir_invalid_properties";
    Path localFilesetInvalidPropertiesPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetInvalidProperties);
    Map<String, String> properties1 = Maps.newHashMap();
    properties1.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetInvalidProperties,
        Fileset.Type.MANAGED,
        localFilesetInvalidPropertiesPath.toString(),
        properties1);
    Path filesetInvalidPropertiesPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, filesetInvalidProperties, true);
    try (FileSystem gravitinoFileSystem = filesetInvalidPropertiesPath.getFileSystem(conf)) {
      Path invalidSrcFilePath = new Path(filesetInvalidPropertiesPath + "/xxx");
      Path invalidDstFilePath = new Path(filesetInvalidPropertiesPath + "/xxx/yyy");
      assertThrows(
          IllegalArgumentException.class,
          () -> gravitinoFileSystem.rename(invalidSrcFilePath, invalidDstFilePath));
    }

    // test valid properties but src is not existing
    String filesetValidProperties = "fileset_dir_valid_properties";
    Path localFilesetValidPropertiesPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetValidProperties);
    Map<String, String> properties2 = Maps.newHashMap();
    properties2.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    properties2.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.ANY.name());
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetValidProperties,
        Fileset.Type.MANAGED,
        localFilesetValidPropertiesPath.toString(),
        properties2);
    Path filesetValidPropertiesPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, filesetValidProperties, true);
    try (FileSystem gravitinoFileSystem = filesetValidPropertiesPath.getFileSystem(conf)) {
      Path validSrcFilePath = new Path(filesetValidPropertiesPath + "/xxx/yyy");
      Path validDstFilePath = new Path(filesetValidPropertiesPath + "/xxx/zzz");
      assertThrows(
          FileNotFoundException.class,
          () -> gravitinoFileSystem.rename(validSrcFilePath, validDstFilePath));
    }

    // test dir rename with YMD
    String fileRename1 = "fileset_dir_rename1";
    Map<String, String> properties3 = Maps.newHashMap();
    Path localRenamePath1 =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, fileRename1);
    properties3.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.YEAR_MONTH_DAY.name());
    properties3.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(5));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        fileRename1,
        Fileset.Type.MANAGED,
        localRenamePath1.toString(),
        properties3);
    Path filesetRenamePath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileRename1, true);
    try (FileSystem gravitinoFileSystem = filesetRenamePath1.getFileSystem(conf);
        FileSystem localFileSystem = localRenamePath1.getFileSystem(conf)) {
      // test invalid src level
      String subInvalidSrcLevelPath = "/year=2024/month=04/day=08/xxx/yyy/zzz";
      Path localInvalidSrcLevelPath = new Path(localRenamePath1 + subInvalidSrcLevelPath);
      Path gvfsInvalidSrcLevelPath = new Path(filesetRenamePath1 + subInvalidSrcLevelPath);
      localFileSystem.mkdirs(localInvalidSrcLevelPath);
      assertTrue(localFileSystem.exists(localInvalidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsInvalidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsInvalidSrcLevelPath, new Path(gvfsInvalidSrcLevelPath + ".zzz")));
      localFileSystem.delete(localInvalidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localInvalidSrcLevelPath));

      // test invalid dst level
      String subValidSrcLevelPath = "/year=2024/month=04/day=08/xxx/yyy";
      Path localValidSrcLevelPath = new Path(localRenamePath1 + subValidSrcLevelPath);
      Path gvfsValidSrcLevelPath = new Path(filesetRenamePath1 + subValidSrcLevelPath);
      localFileSystem.mkdirs(localValidSrcLevelPath);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsValidSrcLevelPath, new Path(gvfsValidSrcLevelPath + "/zzz")));
      localFileSystem.delete(localValidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localValidSrcLevelPath));

      // test valid src and dst
      String subValidSrcLevelPath1 = "/year=2024/month=04/day=08/xxx";
      String subValidDstLevelPath1 = "/year=2024/month=04/day=08/aaa/yyy";
      Path localValidSrcLevelPath1 = new Path(localRenamePath1 + subValidSrcLevelPath1);
      Path gvfsValidSrcLevelPath1 = new Path(filesetRenamePath1 + subValidSrcLevelPath1);
      Path gvfsValidDstLevelPath1 = new Path(filesetRenamePath1 + subValidDstLevelPath1);
      localFileSystem.mkdirs(localValidSrcLevelPath1);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsValidSrcLevelPath1, gvfsValidDstLevelPath1);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath1, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));

      // test valid src, invalid dst level
      String subValidSrcLevelPath2 = "/year=2024/month=04/day=08/xxx";
      String subInvalidDstLevelPath2 = "/year=2024/month=04/day=08/qqq/yyy/zzz";
      Path localValidSrcLevelPath2 = new Path(localRenamePath1 + subValidSrcLevelPath2);
      Path gvfsValidSrcLevelPath2 = new Path(filesetRenamePath1 + subValidSrcLevelPath2);
      Path gvfsInvalidDstLevelPath2 = new Path(filesetRenamePath1 + subInvalidDstLevelPath2);
      localFileSystem.mkdirs(localValidSrcLevelPath2);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath2));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
      assertThrows(
          InvalidPathException.class,
          () -> gravitinoFileSystem.rename(gvfsValidSrcLevelPath2, gvfsInvalidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidSrcLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
    }

    // test file rename with date string
    String fileRename2 = "fileset_dir_rename2";
    Map<String, String> properties4 = Maps.newHashMap();
    Path localRenamePath2 =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, fileRename2);
    properties4.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
    properties4.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        fileRename2,
        Fileset.Type.MANAGED,
        localRenamePath2.toString(),
        properties4);
    Path filesetRenamePath2 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileRename2, true);
    try (FileSystem gravitinoFileSystem = filesetRenamePath2.getFileSystem(conf);
        FileSystem localFileSystem = localRenamePath2.getFileSystem(conf)) {
      // test invalid src level
      String subInvalidSrcLevelPath = "/date=20240408/xxx/yyy/zzz";
      Path localInvalidSrcLevelPath = new Path(localRenamePath2 + subInvalidSrcLevelPath);
      Path gvfsInvalidSrcLevelPath = new Path(filesetRenamePath2 + subInvalidSrcLevelPath);
      localFileSystem.mkdirs(localInvalidSrcLevelPath);
      assertTrue(localFileSystem.exists(localInvalidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsInvalidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsInvalidSrcLevelPath, new Path(gvfsInvalidSrcLevelPath + "/zzz")));
      localFileSystem.delete(localInvalidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localInvalidSrcLevelPath));

      // test invalid dst level
      String subValidSrcLevelPath = "/date=20240408/xxx/yyy";
      Path localValidSrcLevelPath = new Path(localRenamePath2 + subValidSrcLevelPath);
      Path gvfsValidSrcLevelPath = new Path(filesetRenamePath2 + subValidSrcLevelPath);
      localFileSystem.mkdirs(localValidSrcLevelPath);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsValidSrcLevelPath, new Path(gvfsValidSrcLevelPath + "/qqq")));
      localFileSystem.delete(localValidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localValidSrcLevelPath));

      // test valid src and dst
      String subValidSrcLevelPath1 = "/date=20240408/xxx";
      String subValidDstLevelPath1 = "/date=20240408/qqq/yyy";
      Path localValidSrcLevelPath1 = new Path(localRenamePath2 + subValidSrcLevelPath1);
      Path gvfsValidSrcLevelPath1 = new Path(filesetRenamePath2 + subValidSrcLevelPath1);
      Path gvfsValidDstLevelPath1 = new Path(filesetRenamePath2 + subValidDstLevelPath1);
      localFileSystem.mkdirs(localValidSrcLevelPath1);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsValidSrcLevelPath1, gvfsValidDstLevelPath1);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath1, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));

      // test valid src, invalid dst level
      String subValidSrcLevelPath2 = "/date=20240408/xxx";
      String subInvalidDstLevelPath2 = "/date=20240408/qqq/yyy/zzz";
      Path localValidSrcLevelPath2 = new Path(localRenamePath2 + subValidSrcLevelPath2);
      Path gvfsValidSrcLevelPath2 = new Path(filesetRenamePath2 + subValidSrcLevelPath2);
      Path gvfsInvalidDstLevelPath2 = new Path(filesetRenamePath2 + subInvalidDstLevelPath2);
      localFileSystem.mkdirs(localValidSrcLevelPath2);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath2));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
      assertThrows(
          InvalidPathException.class,
          () -> gravitinoFileSystem.rename(gvfsValidSrcLevelPath2, gvfsInvalidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidSrcLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));

      // test src temporary directory
      String tmpSrcLevelPath1 = "/date=20240408/_temporary/zzz/ddd/www";
      String subValidDstLevelPath2 = "/date=20240408/";
      Path localTmpSrcLevelPath1 = new Path(localRenamePath2 + tmpSrcLevelPath1);
      Path gvfsTmpSrcLevelPath1 = new Path(filesetRenamePath2 + tmpSrcLevelPath1);
      Path gvfsValidDstLevelPath2 = new Path(filesetRenamePath2 + subValidDstLevelPath2);
      localFileSystem.mkdirs(localTmpSrcLevelPath1);
      assertTrue(localFileSystem.exists(localTmpSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsTmpSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsTmpSrcLevelPath1, gvfsValidDstLevelPath2);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath2));

      // test dst temporary directory
      String subValidSrcLevelPath3 = "/date=20240408/www";
      String tmpDstLevelPath1 = "/date=20240408/_temp/ddd/xxx/zzz";
      Path localSrcLevelPath1 = new Path(localRenamePath2 + subValidSrcLevelPath3);
      Path gvfsSrcLevelPath1 = new Path(filesetRenamePath2 + subValidSrcLevelPath3);
      Path gvfsTmpDstLevelPath2 = new Path(filesetRenamePath2 + tmpDstLevelPath1);
      localFileSystem.mkdirs(localSrcLevelPath1);
      assertTrue(localFileSystem.exists(localSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsSrcLevelPath1, gvfsTmpDstLevelPath2);
      assertTrue(gravitinoFileSystem.exists(gvfsTmpDstLevelPath2));
      gravitinoFileSystem.delete(gvfsTmpDstLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsTmpDstLevelPath2));
    }

    // test file rename with any
    String fileRename3 = "fileset_dir_rename3";
    Map<String, String> properties5 = Maps.newHashMap();
    Path localRenamePath3 =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, fileRename3);
    properties5.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.ANY.name());
    properties5.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        fileRename3,
        Fileset.Type.MANAGED,
        localRenamePath3.toString(),
        properties5);
    Path filesetRenamePath3 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileRename3, true);
    try (FileSystem gravitinoFileSystem = filesetRenamePath3.getFileSystem(conf);
        FileSystem localFileSystem = localRenamePath3.getFileSystem(conf)) {
      // test invalid src level
      String subInvalidSrcLevelPath = "/date=20240408/xxx/yyy/zzz";
      Path localInvalidSrcLevelPath = new Path(localRenamePath3 + subInvalidSrcLevelPath);
      Path gvfsInvalidSrcLevelPath = new Path(filesetRenamePath3 + subInvalidSrcLevelPath);
      localFileSystem.mkdirs(localInvalidSrcLevelPath);
      assertTrue(localFileSystem.exists(localInvalidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsInvalidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsInvalidSrcLevelPath, new Path(gvfsInvalidSrcLevelPath + "/zzz")));
      localFileSystem.delete(localInvalidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localInvalidSrcLevelPath));

      // test invalid dst level
      String subValidSrcLevelPath = "/date=20240408/xxx/yyy";
      Path localValidSrcLevelPath = new Path(localRenamePath3 + subValidSrcLevelPath);
      Path gvfsValidSrcLevelPath = new Path(filesetRenamePath3 + subValidSrcLevelPath);
      localFileSystem.mkdirs(localValidSrcLevelPath);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath));
      assertThrows(
          InvalidPathException.class,
          () ->
              gravitinoFileSystem.rename(
                  gvfsValidSrcLevelPath, new Path(gvfsValidSrcLevelPath + "/qqq")));
      localFileSystem.delete(localValidSrcLevelPath, true);
      assertFalse(localFileSystem.exists(localValidSrcLevelPath));

      // test valid src and dst
      String subValidSrcLevelPath1 = "/xxx";
      String subValidDstLevelPath1 = "/test/zzz/qqq";
      Path localValidSrcLevelPath1 = new Path(localRenamePath3 + subValidSrcLevelPath1);
      Path gvfsValidSrcLevelPath1 = new Path(filesetRenamePath3 + subValidSrcLevelPath1);
      Path gvfsValidDstLevelPath1 = new Path(filesetRenamePath3 + subValidDstLevelPath1);
      localFileSystem.mkdirs(localValidSrcLevelPath1);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath1));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath1));
      gravitinoFileSystem.rename(gvfsValidSrcLevelPath1, gvfsValidDstLevelPath1);
      assertTrue(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));
      gravitinoFileSystem.delete(gvfsValidDstLevelPath1, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidDstLevelPath1));

      // test valid src, invalid dst level
      String subValidSrcLevelPath2 = "/xxx";
      String subInvalidDstLevelPath2 = "/qqq/zzz/rrr/aaa";
      Path localValidSrcLevelPath2 = new Path(localRenamePath3 + subValidSrcLevelPath2);
      Path gvfsValidSrcLevelPath2 = new Path(filesetRenamePath3 + subValidSrcLevelPath2);
      Path gvfsInvalidDstLevelPath2 = new Path(filesetRenamePath3 + subInvalidDstLevelPath2);
      localFileSystem.mkdirs(localValidSrcLevelPath2);
      assertTrue(localFileSystem.exists(localValidSrcLevelPath2));
      assertTrue(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
      assertThrows(
          InvalidPathException.class,
          () -> gravitinoFileSystem.rename(gvfsValidSrcLevelPath2, gvfsInvalidDstLevelPath2));
      gravitinoFileSystem.delete(gvfsValidSrcLevelPath2, true);
      assertFalse(gravitinoFileSystem.exists(gvfsValidSrcLevelPath2));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDelete(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset delete
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      gravitinoFileSystem.delete(managedFilesetPath, true);
      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));

      // mock the invalid fileset not in server
      String invalidFilesetName = "invalid_fileset";
      Path invalidFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidFilesetName, withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.delete(invalidFilesetPath, true));

      // mock the not correct protocol path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(RuntimeException.class, () -> gravitinoFileSystem.delete(localPrefixPath, true));

      // test external fileset mounts the single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      gravitinoFileSystem.delete(externalFilesetPath, true);
      assertFalse(gravitinoFileSystem.exists(externalFilesetPath));
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @Test
  public void testGetStatus() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
      assertTrue(localFileSystem.exists(localDirPath));

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus localStatus = localFileSystem.getFileStatus(localDirPath);
      gravitinoFileSystem.delete(managedFilesetPath, true);

      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));
      assertEquals(
          localStatus.getPath().toString(),
          gravitinoStatus
              .getPath()
              .toString()
              .replaceFirst(
                  GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                  FileSystemTestUtils.localRootPrefix()));
    }
  }

  @Test
  public void testListStatus() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
      assertTrue(localFileSystem.exists(localDirPath));

      for (int i = 0; i < 5; i++) {
        Path subPath = new Path(managedFilesetPath + "/sub" + i);
        FileSystemTestUtils.mkdirs(subPath, gravitinoFileSystem);
        assertTrue(gravitinoFileSystem.exists(subPath));
        assertTrue(gravitinoFileSystem.getFileStatus(subPath).isDirectory());
      }

      List<FileStatus> gravitinoStatuses =
          new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(managedFilesetPath)));
      gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, gravitinoStatuses.size());

      List<FileStatus> localStatuses =
          new ArrayList<>(Arrays.asList(localFileSystem.listStatus(localDirPath)));
      localStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, localStatuses.size());

      for (int i = 0; i < 5; i++) {
        assertEquals(
            localStatuses.get(i).getPath().toString(),
            gravitinoStatuses
                .get(i)
                .getPath()
                .toString()
                .replaceFirst(
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                    FileSystemTestUtils.localRootPrefix()));
        gravitinoFileSystem.delete(gravitinoStatuses.get(i).getPath(), true);
      }
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus localStatus = localFileSystem.getFileStatus(localDirPath);
      gravitinoFileSystem.delete(managedFilesetPath, true);
      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));

      assertEquals(
          localStatus.getPath().toString(),
          gravitinoStatus
              .getPath()
              .toString()
              .replaceFirst(
                  GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                  FileSystemTestUtils.localRootPrefix()));
    }
  }

  @Test
  public void testMkdirsWithPathLimit() throws IOException {
    // test invalid properties
    String filesetInvalidProperties = "fileset_invalid_properties";
    Path localFilesetInvalidPropertiesPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetInvalidProperties);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetInvalidProperties,
        Fileset.Type.MANAGED,
        localFilesetInvalidPropertiesPath.toString(),
        properties);
    Path filesetInvalidPropertiesPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, filesetInvalidProperties, true);
    try (FileSystem gravitinoFileSystem = filesetInvalidPropertiesPath.getFileSystem(conf)) {
      // test
      Path invalidPrefixPath = new Path(filesetInvalidPropertiesPath + "/xxx");
      assertThrows(
          IllegalArgumentException.class,
          () -> FileSystemTestUtils.mkdirs(invalidPrefixPath, gravitinoFileSystem));
    }

    // test prefix = DATE_WITH_STRING and max level = 3
    String filesetWithDateString = "fileset_date_with_string";
    Path localFilesetWithDateStringPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetWithDateString);
    properties.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetWithDateString,
        Fileset.Type.MANAGED,
        localFilesetWithDateStringPath.toString(),
        properties);
    Path filesetWithDateStringPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetWithDateString, true);
    try (FileSystem gravitinoFileSystem = filesetWithDateStringPath.getFileSystem(conf)) {
      // test invalid prefix
      Path invalidPrefixPath = new Path(filesetWithDateStringPath + "/xxx/test.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.mkdirs(invalidPrefixPath, gravitinoFileSystem));

      // test invalid level
      Path invalidLevelPath = new Path(filesetWithDateStringPath + "/date=20240408/xxx/zzz/qqq");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.mkdirs(invalidLevelPath, gravitinoFileSystem));

      // test valid path 1
      Path validPath1 = new Path(filesetWithDateStringPath + "/date=20240408/xxx/zzz");
      FileSystemTestUtils.mkdirs(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test valid path 2
      Path validPath2 = new Path(filesetWithDateStringPath + "/date=20240408/xxx");
      FileSystemTestUtils.mkdirs(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);

      // test valid path 3
      Path validPath3 = new Path(filesetWithDateStringPath + "/date=20240408");
      FileSystemTestUtils.mkdirs(validPath3, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath3));
      gravitinoFileSystem.delete(validPath3, true);
    }

    // test prefix = YEAR_MONTH_DAY and max level = 5
    String filesetYMD = "fileset_year_month_day";
    Path localFilesetYMDPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetYMD);
    properties.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.YEAR_MONTH_DAY.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(5));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetYMD,
        Fileset.Type.MANAGED,
        localFilesetYMDPath.toString(),
        properties);
    Path filesetYMDPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetYMD, true);
    try (FileSystem gravitinoFileSystem = filesetYMDPath.getFileSystem(conf)) {
      // test invalid prefix
      Path invalidPrefixPath = new Path(filesetYMDPath + "/xxx/test.txt");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.mkdirs(invalidPrefixPath, gravitinoFileSystem));

      // test invalid level
      Path invalidLevelPath = new Path(filesetYMDPath + "/year=2024/month=04/day=08/xxx/zzz/qqq");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.mkdirs(invalidLevelPath, gravitinoFileSystem));

      // test valid path 1
      Path validPath1 = new Path(filesetYMDPath + "/year=2024/month=04/day=08/xxx/zzz");
      FileSystemTestUtils.mkdirs(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test valid path 2
      Path validPath2 = new Path(filesetYMDPath + "/year=2024/month=04/day=08/xxx");
      FileSystemTestUtils.mkdirs(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);

      // test valid path 3
      Path validPath3 = new Path(filesetYMDPath + "/year=2024/month=04/day=08");
      FileSystemTestUtils.mkdirs(validPath3, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath3));
      gravitinoFileSystem.delete(validPath3, true);
    }

    // test prefix = ANY and max level = 3
    String filesetAny = "fileset_any";
    Path localFilesetAnyPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetAny);
    properties.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.ANY.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetAny,
        Fileset.Type.MANAGED,
        localFilesetAnyPath.toString(),
        properties);
    Path filesetAnyPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetAny, true);
    try (FileSystem gravitinoFileSystem = filesetAnyPath.getFileSystem(conf)) {
      // test invalid level
      Path invalidLevelPath = new Path(filesetAnyPath + "/year=2024/month=04/day=08/xxx");
      assertThrows(
          InvalidPathException.class,
          () -> FileSystemTestUtils.mkdirs(invalidLevelPath, gravitinoFileSystem));

      // test valid path 1
      Path validPath1 = new Path(filesetAnyPath + "/year=2024/month=04/day=08");
      FileSystemTestUtils.mkdirs(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test valid path 2
      Path validPath2 = new Path(filesetAnyPath + "/xxx");
      FileSystemTestUtils.mkdirs(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);
    }

    // test prefix = DATE_WITH_STRING and max level = 3
    String filesetD1 = "fileset_date_1";
    Path localFilesetD1Path =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetD1);
    properties.put(
        FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
    properties.put(FilesetProperties.DIR_MAX_LEVEL_KEY, String.valueOf(3));
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        filesetD1,
        Fileset.Type.MANAGED,
        localFilesetD1Path.toString(),
        properties);
    Path filesetD1Path =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetD1, true);
    try (FileSystem gravitinoFileSystem = filesetD1Path.getFileSystem(conf)) {
      // test temporary dir 1
      Path validPath1 = new Path(filesetD1Path + "/date=20240408/_temporary/xxx/ddd/zzz");
      FileSystemTestUtils.mkdirs(validPath1, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath1));
      gravitinoFileSystem.delete(validPath1, true);

      // test temporary dir 2
      Path validPath2 = new Path(filesetD1Path + "/date=20240408/.temporary/xxx/ddd/zzz");
      FileSystemTestUtils.mkdirs(validPath2, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(validPath2));
      gravitinoFileSystem.delete(validPath2, true);
    }
  }

  @Test
  public void testExtractIdentifier() throws IOException, URISyntaxException {
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {
      NameIdentifier identifier =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier);

      NameIdentifier identifier2 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier2);

      NameIdentifier identifier3 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/files"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier3);

      NameIdentifier identifier4 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/dir/dir"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier4);

      NameIdentifier identifier5 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/dir/dir/"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier5);

      NameIdentifier identifier6 = fs.extractIdentifier(new URI("/catalog1/schema1/fileset1"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier6);

      NameIdentifier identifier7 = fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier7);

      NameIdentifier identifier8 = fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier8);

      NameIdentifier identifier9 =
          fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir/dir/"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier9);

      NameIdentifier identifier10 =
          fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir/dir"));
      assertEquals(
          NameIdentifier.ofFileset(metalakeName, "catalog1", "schema1", "fileset1"), identifier10);

      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("gvfs://fileset/catalog1/")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("hdfs://fileset/catalog1/schema1/fileset1")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("/catalog1/schema1/")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1//")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir//")));
    }
  }
}
