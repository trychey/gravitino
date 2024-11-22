/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockserver.model.HttpRequest.request;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.file.FilesetContextDTO;
import com.datastrato.gravitino.dto.file.FilesetDTO;
import com.datastrato.gravitino.dto.responses.FilesetContextResponse;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.Method;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestGvfsBase extends GravitinoMockServerBase {
  protected static final String GVFS_IMPL_CLASS = GravitinoVirtualFileSystem.class.getName();
  protected static final String GVFS_ABSTRACT_IMPL_CLASS = Gvfs.class.getName();
  protected static Configuration conf = new Configuration();
  protected static final Path localCatalogPath =
      FileSystemTestUtils.createLocalRootDir(catalogName);

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
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_SUPER_USER_KEY, "test");
  }

  @AfterAll
  public static void tearDown() {
    GravitinoMockServerBase.tearDown();
    try (FileSystem localFileSystem = localCatalogPath.getFileSystem(conf)) {
      if (localFileSystem.exists(localCatalogPath)) {
        localFileSystem.delete(localCatalogPath, true);
      }
    } catch (IOException e) {
      // ignore
    }
  }

  @BeforeEach
  public void init() {
    mockMetalakeDTO(metalakeName, "comment");
    mockCatalogDTO(catalogName, provider, "comment");
  }

  @Test
  public void testFSCache() throws IOException {
    String filesetName = "testFSCache";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localStorageLocation =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localStorageLocation.getFileSystem(conf)) {

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
      Path newGvfsPath =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "new_fileset", true);
      try (FileSystem anotherFS = newGvfsPath.getFileSystem(conf)) {
        assertNotEquals(anotherFS, gravitinoFileSystem);
      }

      // test proxyed local fs, should not get the same fs
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localStorageLocation.toString(),
              ImmutableMap.of());

      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localStorageLocation + "/dir"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.MKDIRS, "/dir"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      FileSystemTestUtils.mkdirs(new Path(managedFilesetPath + "/dir"), gravitinoFileSystem);
      FileSystem proxyLocalFs =
          Objects.requireNonNull(
                  ((GravitinoVirtualFileSystem) gravitinoFileSystem)
                      .getFileSystemManager()
                      .getInternalContextCache()
                      .getIfPresent(localStorageLocation.toString()))
              .getFileSystem();

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
    String authType =
        conf.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
            GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE);
    if (authType.equals(GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE)) {
      GravitinoMockServerBase.mockServer()
          .clear(request().withPath("/api/metalakes/" + metalakeName + "/secrets"));
      GravitinoMockServerBase.mockSecretDTO("testInternalCache", System.currentTimeMillis() + 1000);
    }
    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_INTERNAL_FILESYSTEM_DELAY_CLOSE_MILLS_KEY,
        "0");
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY, "1");
    configuration.set(
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
        "1000");

    Path localPath1 = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "fileset1");
    Path filesetPath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "fileset1", true);
    String contextPath1 =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, "fileset1");
    try (FileSystem fs = filesetPath1.getFileSystem(configuration)) {
      FilesetDTO fileset1 =
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
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(fileset1)
              .actualPaths(new String[] {localPath1.toString()})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath1,
            mockGetContextRequest(FilesetDataOperation.MKDIRS, ""),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      FileSystemTestUtils.mkdirs(filesetPath1, fs);

      // expired by time
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertEquals(
                      0,
                      ((GravitinoVirtualFileSystem) fs)
                          .getFileSystemManager()
                          .getInternalContextCache()
                          .asMap()
                          .size()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreate(boolean withScheme) throws IOException {
    String filesetName = "testCreate";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test gvfs normal create
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/test.txt"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.CREATE, "/test.txt"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path localFilePath = new Path(localPath + "/test.txt");
      assertFalse(localFileSystem.exists(localFilePath));
      Path filePath = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(filePath, gravitinoFileSystem, true);
      assertTrue(localFileSystem.exists(localFilePath));
      localFileSystem.delete(localFilePath, true);

      // mock the invalid fileset not in the server
      String invalidFilesetName = "invalid_fileset";
      Path invalidFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidFilesetName, withScheme);
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.create(invalidFilesetPath, gravitinoFileSystem, true));

      // mock the not correct protocol prefix path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.create(localPrefixPath, gravitinoFileSystem, true));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @Disabled("Append operation is not supported in LocalFileSystem. We can't test it now.")
  public void testAppend(boolean withScheme) throws IOException {
    String filesetName = "testAppend";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset append
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/test.txt"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.APPEND, "/test.txt"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path appendFile = new Path(managedFilesetPath + "/test.txt");
      Path localAppendFile = new Path(localPath + "/test.txt");
      FileSystemTestUtils.create(localAppendFile, localFileSystem, true);
      assertTrue(localFileSystem.exists(localAppendFile));
      FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
      assertEquals(
          "Hello, World!",
          new String(
              FileSystemTestUtils.read(localAppendFile, localFileSystem), StandardCharsets.UTF_8));
      localFileSystem.delete(localAppendFile, true);

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
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRename(boolean withScheme) throws IOException {
    String filesetName = "testRename";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset rename
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO1 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/rename_src"})
              .build();
      FilesetContextResponse contextResponse1 = new FilesetContextResponse(mockContextDTO1);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.RENAME, "/rename_src"),
            contextResponse1,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      FilesetContextDTO mockContextDTO2 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/rename_dst2"})
              .build();
      FilesetContextResponse contextResponse2 = new FilesetContextResponse(mockContextDTO2);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.RENAME, "/rename_dst2"),
            contextResponse2,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path srcLocalRenamePath = new Path(localPath + "/rename_src");
      localFileSystem.mkdirs(srcLocalRenamePath);
      assertTrue(localFileSystem.getFileStatus(srcLocalRenamePath).isDirectory());
      assertTrue(localFileSystem.exists(srcLocalRenamePath));
      // cannot rename the identifier
      Path srcFilesetRenamePath = new Path(managedFilesetPath + "/rename_src");
      Path dstRenamePath1 =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "rename_dst1", withScheme);
      assertThrows(
          RuntimeException.class,
          () -> gravitinoFileSystem.rename(srcFilesetRenamePath, dstRenamePath1));

      Path dstFilesetRenamePath2 = new Path(managedFilesetPath + "/rename_dst2");
      Path dstLocalRenamePath2 = new Path(localPath + "/rename_dst2");
      gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath2);
      assertFalse(localFileSystem.exists(srcLocalRenamePath));
      assertTrue(localFileSystem.exists(dstLocalRenamePath2));
      localFileSystem.delete(dstLocalRenamePath2, true);

      // test invalid src path
      Path invalidSrcPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_src_name", withScheme);
      Path validDstPath =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

      // test invalid dst path
      Path invalidDstPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_dst_name", withScheme);
      assertThrows(
          RuntimeException.class,
          () -> gravitinoFileSystem.rename(managedFilesetPath, invalidDstPath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDelete(boolean withScheme) throws IOException {
    String filesetName = "testDelete";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset delete
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/test_delete"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.DELETE, "/test_delete"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path dirPath = new Path(managedFilesetPath + "/test_delete");
      Path localDirPath = new Path(localPath + "/test_delete");
      localFileSystem.mkdirs(localDirPath);
      assertTrue(localFileSystem.exists(localDirPath));
      gravitinoFileSystem.delete(dirPath, true);
      assertFalse(localFileSystem.exists(localDirPath));

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
    }
  }

  @Test
  public void testGetStatus() throws IOException {
    String filesetName = "testGetStatus";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath.toString()})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.GET_FILE_STATUS, ""),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus localStatus = localFileSystem.getFileStatus(localPath);

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
    String filesetName = "testListStatus";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      for (int i = 0; i < 5; i++) {
        Path subLocalPath = new Path(localPath + "/sub" + i);
        FileSystemTestUtils.mkdirs(subLocalPath, localFileSystem);
        assertTrue(localFileSystem.exists(subLocalPath));
        assertTrue(localFileSystem.getFileStatus(subLocalPath).isDirectory());
      }

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath.toString()})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.LIST_STATUS, ""),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      List<FileStatus> gravitinoStatuses =
          new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(managedFilesetPath)));
      gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, gravitinoStatuses.size());

      List<FileStatus> localStatuses =
          new ArrayList<>(Arrays.asList(localFileSystem.listStatus(localPath)));
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
      }
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    String filesetName = "testMkdirs";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));
      assertTrue(localFileSystem.getFileStatus(localPath).isDirectory());

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              localPath.toString(),
              ImmutableMap.of());
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/test_mkdirs"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.MKDIRS, "/test_mkdirs"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path subDirPath = new Path(managedFilesetPath + "/test_mkdirs");
      Path localDirPath = new Path(localPath + "/test_mkdirs");
      FileSystemTestUtils.mkdirs(subDirPath, gravitinoFileSystem);
      assertTrue(localFileSystem.exists(localDirPath));
      assertTrue(localFileSystem.getFileStatus(localDirPath).isDirectory());

      FileStatus localStatus = localFileSystem.getFileStatus(localDirPath);

      FilesetContextDTO mockContextDTO1 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {localPath + "/test_mkdirs"})
              .build();
      FilesetContextResponse contextResponse1 = new FilesetContextResponse(mockContextDTO1);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.GET_FILE_STATUS, "/test_mkdirs"),
            contextResponse1,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(subDirPath);

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
  public void testExtractIdentifier() throws IOException, URISyntaxException {
    String filesetName = "testExtractIdentifier";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    String filesetName = "testCreate_with_multi_locs_" + isTestPrimaryLocation;
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    if (isTestPrimaryLocation) {
      primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
      backupPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    } else {
      primaryPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
      backupPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    }
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      // test gvfs create
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/test.txt", backupPath + "/test.txt"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.CREATE, "/test.txt"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      if (isTestPrimaryLocation) {
        Path primaryFilePath = new Path(primaryPath + "/test.txt");
        Path backupFilePath = new Path(backupPath + "/test.txt");
        assertFalse(primaryFs.exists(primaryFilePath));
        assertFalse(backupFs.exists(backupFilePath));

        Path filePath = new Path(managedFilesetPath + "/test.txt");
        FileSystemTestUtils.create(filePath, gravitinoFileSystem, true);
        assertTrue(primaryFs.exists(primaryFilePath));
        assertFalse(backupFs.exists(backupFilePath));
      } else {
        Path primaryFilePath = new Path(primaryPath + "/test.txt");
        Path backupFilePath = new Path(backupPath + "/test.txt");
        assertFalse(primaryFs.exists(primaryFilePath));
        assertFalse(backupFs.exists(backupFilePath));
        backupFs.create(backupFilePath);
        assertTrue(backupFs.exists(backupFilePath));

        // Create the file in backup location as the file only exist in the backup location.
        Path filePath = new Path(managedFilesetPath + "/test.txt");
        FileSystemTestUtils.create(filePath, gravitinoFileSystem, true);
        assertFalse(primaryFs.exists(primaryFilePath));
        assertTrue(backupFs.exists(backupFilePath));

        // Only support no more than one actual path exists.
        primaryFs.create(primaryFilePath, true);
        assertTrue(primaryFs.exists(primaryFilePath));
        assertTrue(backupFs.exists(backupFilePath));
        assertThrows(
            IllegalArgumentException.class,
            () -> FileSystemTestUtils.create(filePath, gravitinoFileSystem, true));
      }
    }
  }

  @Test
  public void testCreateWithDoubleWrite() throws IOException {
    String filesetName = "testCreate_with_multi_locs";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    backupPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(newConf);
        FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      // test gvfs create
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/test.txt", backupPath + "/test.txt"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.CREATE, "/test.txt"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path primaryFilePath = new Path(primaryPath + "/test.txt");
      Path backupFilePath = new Path(backupPath + "/test.txt");
      assertFalse(primaryFs.exists(primaryFilePath));
      assertFalse(backupFs.exists(backupFilePath));
      backupFs.create(backupFilePath);
      assertTrue(backupFs.exists(backupFilePath));

      // Create the file in primary location when enable double write.
      Path filePath = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(filePath, gravitinoFileSystem, true);
      assertTrue(primaryFs.exists(primaryFilePath));
      assertTrue(backupFs.exists(backupFilePath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeleteWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    String filesetName = "testDelete_with_multi_locs_" + isTestPrimaryLocation;
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    if (isTestPrimaryLocation) {
      primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
      backupPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    } else {
      primaryPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
      backupPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    }
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      // test gvfs delete
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/test_delete", backupPath + "/test_delete"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.DELETE, "/test_delete"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      if (isTestPrimaryLocation) {
        Path primaryDirPath = new Path(primaryPath + "/test_delete");
        Path backupDirPath = new Path(backupPath + "/test_delete");
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));
        primaryFs.mkdirs(primaryDirPath);
        assertTrue(primaryFs.exists(primaryDirPath));

        Path dirPath = new Path(managedFilesetPath + "/test_delete");
        gravitinoFileSystem.delete(dirPath, true);
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));
      } else {
        Path primaryDirPath = new Path(primaryPath + "/test_delete");
        Path backupDirPath = new Path(backupPath + "/test_delete");
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));
        backupFs.mkdirs(backupDirPath);
        assertTrue(backupFs.exists(backupDirPath));

        // delete the directory in backup location.
        Path dirPath = new Path(managedFilesetPath + "/test_delete");
        gravitinoFileSystem.delete(dirPath, true);
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));

        // Only support no more than one actual path exists.
        primaryFs.mkdirs(primaryDirPath);
        backupFs.mkdirs(backupDirPath);
        assertTrue(primaryFs.exists(primaryDirPath));
        assertTrue(backupFs.exists(backupDirPath));
        assertThrowsExactly(
            IllegalArgumentException.class, () -> gravitinoFileSystem.delete(dirPath, true));

        // File not exists.
        primaryFs.delete(primaryDirPath, true);
        backupFs.delete(backupDirPath, true);
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));
        assertFalse(gravitinoFileSystem.delete(dirPath, true));
      }
    }
  }

  @Test
  public void testDeleteWithDoubleWrite() throws IOException {
    String filesetName = "testDelete_with_double_write";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    backupPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(newConf);
        FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      // test gvfs delete
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/test_delete", backupPath + "/test_delete"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.DELETE, "/test_delete"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path primaryDirPath = new Path(primaryPath + "/test_delete");
      Path backupDirPath = new Path(backupPath + "/test_delete");
      assertFalse(primaryFs.exists(primaryDirPath));
      assertFalse(backupFs.exists(backupDirPath));
      backupFs.mkdirs(backupDirPath);
      assertTrue(backupFs.exists(backupDirPath));

      // can not delete the directory in backup location when enable double write.
      Path dirPath = new Path(managedFilesetPath + "/test_delete");
      gravitinoFileSystem.delete(dirPath, true);
      assertFalse(primaryFs.exists(primaryDirPath));
      assertTrue(backupFs.exists(backupDirPath));

      // delete the directory in primary location when enable double write.
      primaryFs.mkdirs(primaryDirPath);
      assertTrue(primaryFs.exists(primaryDirPath));
      assertTrue(backupFs.exists(backupDirPath));
      gravitinoFileSystem.delete(dirPath, true);
      assertFalse(primaryFs.exists(primaryDirPath));
      assertTrue(backupFs.exists(backupDirPath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetStatusWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    String filesetName = "testGetStatus_with_multiple_locs_" + isTestPrimaryLocation;
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    if (isTestPrimaryLocation) {
      primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
      backupPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    } else {
      primaryPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
      backupPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    }
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath.toString(), backupPath.toString()})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.GET_FILE_STATUS, ""),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      if (isTestPrimaryLocation) {

        assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());

        FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
        FileStatus primaryFileStatus = primaryFs.getFileStatus(primaryPath);

        assertEquals(
            primaryFileStatus.getPath().toString(),
            gravitinoStatus
                .getPath()
                .toString()
                .replaceFirst(
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                    FileSystemTestUtils.localRootPrefix()));
      } else {
        primaryFs.delete(primaryPath, true);
        assertFalse(primaryFs.exists(primaryPath));

        assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
        FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
        FileStatus backupFileStatus = primaryFs.getFileStatus(backupPath);
        assertEquals(
            backupFileStatus.getPath().toString(),
            gravitinoStatus
                .getPath()
                .toString()
                .replaceFirst(
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                    FileSystemTestUtils.localRootPrefix()));

        backupFs.delete(backupPath, true);
        assertFalse(backupFs.exists(backupPath));

        assertThrowsExactly(
            FileNotFoundException.class,
            () -> gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
      }
    }
  }

  @Test
  public void testGetStatusWithDoubleWrite() throws IOException {
    String filesetName = "testGetStatus_with_double_write";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    backupPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(newConf);
        FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath.toString(), backupPath.toString()})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.GET_FILE_STATUS, ""),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus primaryFileStatus = primaryFs.getFileStatus(primaryPath);
      assertEquals(
          primaryFileStatus.getPath().toString(),
          gravitinoStatus
              .getPath()
              .toString()
              .replaceFirst(
                  GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                  FileSystemTestUtils.localRootPrefix()));

      // only support getting file status in the primary location when enable double write.
      primaryFs.delete(primaryPath, true);
      assertFalse(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));
      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));
      assertThrowsExactly(
          FileNotFoundException.class,
          () -> gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  public void testListStatusWithMultipleLocs(
      boolean isTestPrimaryLocation, boolean isWritePrimaryOnly) throws IOException {
    String filesetName = "testListStatus_with_multi_locs_" + isWritePrimaryOnly;
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    if (isTestPrimaryLocation) {
      primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
      backupPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    } else {
      primaryPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
      backupPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    }
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY,
        isWritePrimaryOnly);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(newConf);
        FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath.toString(), backupPath.toString()})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.LIST_STATUS, ""),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      for (int i = 0; i < 5; i++) {
        if (isTestPrimaryLocation) {
          Path subLocalPath = new Path(primaryPath + "/sub" + i);
          FileSystemTestUtils.mkdirs(subLocalPath, primaryFs);
          assertTrue(primaryFs.exists(subLocalPath));
          assertTrue(primaryFs.getFileStatus(subLocalPath).isDirectory());
        } else {
          Path subLocalPath = new Path(primaryPath + "/sub" + i);
          FileSystemTestUtils.mkdirs(subLocalPath, primaryFs);
          assertTrue(primaryFs.exists(subLocalPath));
          assertTrue(primaryFs.getFileStatus(subLocalPath).isDirectory());

          Path subLocalPath1 = new Path(backupPath + "/sub" + i);
          FileSystemTestUtils.mkdirs(subLocalPath1, backupFs);
          assertTrue(backupFs.exists(subLocalPath1));
          assertTrue(backupFs.getFileStatus(subLocalPath1).isDirectory());
        }
      }

      List<FileStatus> gravitinoStatuses =
          new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(managedFilesetPath)));
      gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, gravitinoStatuses.size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMkdirsWithMultiLocs(boolean isTestPrimaryLocation) throws IOException {
    String filesetName = "testMkdirs_with_multi_locs_" + isTestPrimaryLocation;
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    if (isTestPrimaryLocation) {
      primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
      backupPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    } else {
      primaryPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
      backupPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    }
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/test_mkdirs", backupPath + "/test_mkdirs"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.MKDIRS, "/test_mkdirs"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      if (isTestPrimaryLocation) {
        Path primaryDirPath = new Path(primaryPath + "/test_mkdirs");
        Path backupDirPath = new Path(backupPath + "/test_mkdirs");
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));

        Path subDirPath = new Path(managedFilesetPath + "/test_mkdirs");
        FileSystemTestUtils.mkdirs(subDirPath, gravitinoFileSystem);
        assertTrue(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));
      } else {
        Path primaryDirPath = new Path(primaryPath + "/test_mkdirs");
        Path backupDirPath = new Path(backupPath + "/test_mkdirs");
        assertFalse(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));

        Path subDirPath = new Path(managedFilesetPath + "/test_mkdirs");
        FileSystemTestUtils.mkdirs(subDirPath, gravitinoFileSystem);
        assertTrue(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));

        assertTrue(gravitinoFileSystem.mkdirs(subDirPath));
        assertTrue(primaryFs.exists(primaryDirPath));
        assertFalse(backupFs.exists(backupDirPath));

        primaryFs.delete(primaryDirPath, true);
        assertFalse(primaryFs.exists(primaryDirPath));
        backupFs.mkdirs(backupDirPath);
        assertTrue(backupFs.exists(backupDirPath));
        assertTrue(gravitinoFileSystem.mkdirs(subDirPath));
        assertFalse(primaryFs.exists(primaryDirPath));
        assertTrue(backupFs.exists(backupDirPath));
      }
    }
  }

  @Test
  public void testMkdirsWithDoubleWrite() throws IOException {
    String filesetName = "testMkdirs_with_double_write";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    backupPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(newConf);
        FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/test_mkdirs", backupPath + "/test_mkdirs"})
              .build();
      FilesetContextResponse contextResponse = new FilesetContextResponse(mockContextDTO);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.MKDIRS, "/test_mkdirs"),
            contextResponse,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path primaryDirPath = new Path(primaryPath + "/test_mkdirs");
      Path backupDirPath = new Path(backupPath + "/test_mkdirs");
      assertFalse(primaryFs.exists(primaryDirPath));
      assertFalse(backupFs.exists(backupDirPath));

      Path subDirPath = new Path(managedFilesetPath + "/test_mkdirs");
      FileSystemTestUtils.mkdirs(subDirPath, gravitinoFileSystem);
      assertTrue(primaryFs.exists(primaryDirPath));
      assertFalse(backupFs.exists(backupDirPath));

      // support make a new dir in primary location when enable
      // fs.gravitino.fileset.write.primaryOnly.
      primaryFs.delete(primaryDirPath, true);
      assertFalse(primaryFs.exists(primaryDirPath));
      backupFs.mkdirs(backupDirPath);
      assertTrue(backupFs.exists(backupDirPath));
      assertTrue(gravitinoFileSystem.mkdirs(subDirPath));
      assertTrue(primaryFs.exists(primaryDirPath));
      assertTrue(backupFs.exists(backupDirPath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRenameWithMultipleLocs(boolean isTestPrimaryLocation) throws IOException {
    String filesetName = "testRename_with_multi_locs_" + isTestPrimaryLocation;
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    if (isTestPrimaryLocation) {
      primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
      backupPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    } else {
      primaryPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
      backupPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    }
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem primaryFs = primaryPath.getFileSystem(conf);
        FileSystem backupFs = backupPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      // test managed fileset rename
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO1 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/rename_src", backupPath + "/rename_src"})
              .build();
      FilesetContextResponse contextResponse1 = new FilesetContextResponse(mockContextDTO1);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.RENAME, "/rename_src"),
            contextResponse1,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      FilesetContextDTO mockContextDTO2 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/rename_dst1", backupPath + "/rename_dst1"})
              .build();
      FilesetContextResponse contextResponse2 = new FilesetContextResponse(mockContextDTO2);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.RENAME, "/rename_dst1"),
            contextResponse2,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path primarySrcPath = new Path(primaryPath + "/rename_src");
      Path primaryDstPath = new Path(primaryPath + "/rename_dst1");
      Path backupSrcPath = new Path(backupPath + "/rename_src");
      Path backupDstPath = new Path(backupPath + "/rename_dst1");
      Path srcFilesetRenamePath = new Path(managedFilesetPath + "/rename_src");
      Path dstFilesetRenamePath = new Path(managedFilesetPath + "/rename_dst1");

      if (isTestPrimaryLocation) {
        primaryFs.mkdirs(primarySrcPath);
        assertTrue(primaryFs.getFileStatus(primarySrcPath).isDirectory());
        assertFalse(primaryFs.exists(primaryDstPath));
        assertFalse(backupFs.exists(backupSrcPath));
        assertFalse(backupFs.exists(backupDstPath));

        assertTrue(gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
        assertFalse(primaryFs.exists(primarySrcPath));
        assertTrue(primaryFs.exists(primaryDstPath));

        // rename again when the dst path exists.
        primaryFs.mkdirs(primarySrcPath);
        assertTrue(primaryFs.exists(primarySrcPath));
        assertTrue(primaryFs.exists(primaryDstPath));
        assertFalse(backupFs.exists(backupSrcPath));
        assertFalse(backupFs.exists(backupDstPath));
        assertThrowsExactly(
            IllegalArgumentException.class,
            () -> gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
      } else {
        backupFs.mkdirs(backupSrcPath);
        assertFalse(primaryFs.exists(primarySrcPath));
        assertFalse(primaryFs.exists(primaryDstPath));
        assertTrue(backupFs.getFileStatus(backupSrcPath).isDirectory());
        assertFalse(backupFs.exists(backupDstPath));
        assertTrue(gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
        assertFalse(primaryFs.exists(primarySrcPath));
        assertFalse(primaryFs.exists(primaryDstPath));
        assertFalse(backupFs.exists(backupSrcPath));
        assertTrue(backupFs.exists(backupDstPath));

        backupFs.delete(backupDstPath, true);
        primaryFs.mkdirs(primarySrcPath);
        backupFs.mkdirs(backupSrcPath);
        assertTrue(primaryFs.exists(primarySrcPath));
        assertFalse(primaryFs.exists(primaryDstPath));
        assertTrue(backupFs.exists(backupSrcPath));
        assertFalse(backupFs.exists(backupDstPath));
        assertThrowsExactly(
            IllegalArgumentException.class,
            () -> gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
      }
    }
  }

  @Test
  public void testRenameWithDoubleWrite() throws IOException {
    String filesetName = "testRename_with_double_write";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);

    Path primaryPath;
    Path backupPath;
    primaryPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    backupPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName + "_bak");
    Map<String, String> props =
        ImmutableMap.of(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, backupPath.toString());

    String contextPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/context",
            metalakeName, catalogName, schemaName, filesetName);

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY, true);

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(newConf);
        FileSystem primaryFs = primaryPath.getFileSystem(newConf);
        FileSystem backupFs = backupPath.getFileSystem(newConf)) {

      FileSystemTestUtils.mkdirs(primaryPath, primaryFs);
      FileSystemTestUtils.mkdirs(backupPath, backupFs);
      assertTrue(primaryFs.exists(primaryPath));
      assertTrue(backupFs.exists(backupPath));

      // test managed fileset rename
      FilesetDTO managedFileset =
          mockFilesetDTO(
              metalakeName,
              catalogName,
              schemaName,
              filesetName,
              Fileset.Type.MANAGED,
              primaryPath.toString(),
              props);
      FilesetContextDTO mockContextDTO1 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/rename_src", backupPath + "/rename_src"})
              .build();
      FilesetContextResponse contextResponse1 = new FilesetContextResponse(mockContextDTO1);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.RENAME, "/rename_src"),
            contextResponse1,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      FilesetContextDTO mockContextDTO2 =
          FilesetContextDTO.builder()
              .fileset(managedFileset)
              .actualPaths(new String[] {primaryPath + "/rename_dst1", backupPath + "/rename_dst1"})
              .build();
      FilesetContextResponse contextResponse2 = new FilesetContextResponse(mockContextDTO2);
      try {
        buildMockResource(
            Method.POST,
            contextPath,
            mockGetContextRequest(FilesetDataOperation.RENAME, "/rename_dst1"),
            contextResponse2,
            SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path primarySrcPath = new Path(primaryPath + "/rename_src");
      Path primaryDstPath = new Path(primaryPath + "/rename_dst1");
      Path backupSrcPath = new Path(backupPath + "/rename_src");
      Path backupDstPath = new Path(backupPath + "/rename_dst1");
      Path srcFilesetRenamePath = new Path(managedFilesetPath + "/rename_src");
      Path dstFilesetRenamePath = new Path(managedFilesetPath + "/rename_dst1");

      primaryFs.mkdirs(primarySrcPath);
      assertTrue(primaryFs.getFileStatus(primarySrcPath).isDirectory());
      assertFalse(primaryFs.exists(primaryDstPath));
      assertFalse(backupFs.exists(backupSrcPath));
      assertFalse(backupFs.exists(backupDstPath));
      assertTrue(gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
      assertFalse(primaryFs.exists(primarySrcPath));
      assertTrue(primaryFs.exists(primaryDstPath));

      // only support rename the path in primary location when enable
      // fs.gravitino.fileset.write.primaryOnly.
      primaryFs.delete(primarySrcPath, true);
      primaryFs.delete(primaryDstPath, true);
      backupFs.mkdirs(backupSrcPath);
      assertFalse(primaryFs.exists(primarySrcPath));
      assertFalse(primaryFs.exists(primaryDstPath));
      assertTrue(backupFs.getFileStatus(backupSrcPath).isDirectory());
      assertFalse(backupFs.exists(backupDstPath));
      assertThrowsExactly(
          FileNotFoundException.class,
          () -> gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
      primaryFs.mkdirs(primarySrcPath);
      assertTrue(primaryFs.exists(primarySrcPath));
      assertFalse(primaryFs.exists(primaryDstPath));
      assertTrue(gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath));
      assertFalse(primaryFs.exists(primarySrcPath));
      assertTrue(primaryFs.exists(primaryDstPath));
      assertTrue(backupFs.exists(backupSrcPath));
      assertFalse(backupFs.exists(backupDstPath));
    }
  }

  @Test
  public void testLoadConfig() throws IOException {
    Configuration configuration = new Configuration(conf);
    configuration.set("key1", "value1");
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            "gravitino.bypass.key1", "value2",
            "gravitino.bypass.key2", "value2",
            "key4", "value4");
    Map<String, String> filesetProperties =
        ImmutableMap.of(
            "gravitino.bypass.key2", "value3",
            "gravitino.bypass.key3", "value3",
            "key5", "value5");
    try (GravitinoVirtualFileSystem gvfs = new GravitinoVirtualFileSystem()) {
      gvfs.initialize(URI.create("gvfs://fileset/test"), configuration);
      Configuration res = gvfs.loadConfig(catalogProperties, filesetProperties);
      Assertions.assertEquals("value2", res.get("key1"));
      Assertions.assertEquals("value3", res.get("key2"));
      Assertions.assertEquals("value3", res.get("key3"));
    }
  }

  @Test
  public void testGetStorageLocation() {
    Fileset fileset1 = Mockito.mock(Fileset.class);
    when(fileset1.storageLocation()).thenReturn("hdfs://cluster/test-with-slash/");
    Map<String, String> mockProperties1 = new HashMap<>();
    mockProperties1.put(
        FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + 1, "lavafs://cluster/test-without-slash");
    when(fileset1.properties()).thenReturn(mockProperties1);

    String location1 = GravitinoVirtualFileSystem.getStorageLocation(fileset1, 0);
    Assertions.assertEquals("hdfs://cluster/test-with-slash", location1);

    String location2 = GravitinoVirtualFileSystem.getStorageLocation(fileset1, 1);
    Assertions.assertEquals("lavafs://cluster/test-without-slash", location2);
  }
}
