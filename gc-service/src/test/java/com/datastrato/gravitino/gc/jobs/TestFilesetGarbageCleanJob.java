/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.gc.jobs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.gc.utils.CliParser;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.tuple.Pair;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestFilesetGarbageCleanJob {
  private static final String LOCAL_FS_PREFIX =
      "file:/tmp/gravitino_test_clean_job_" + UUID.randomUUID().toString().replace("-", "");
  @TempDir private static File tempDir;

  private static MiniDFSCluster hdfsCluster;

  @BeforeAll
  public static void setUp() {
    CliParser cliParser = Mockito.mock(CliParser.class);
    FilesetGarbageCleanJob.cliParser = cliParser;
    Mockito.when(cliParser.getDate()).thenReturn("20240401");
    Mockito.when(cliParser.getGravitinoMetalake()).thenReturn("metalake_1");
    Mockito.when(cliParser.getFilesetCatalog()).thenReturn("catalog_1");
    try {
      if (!tempDir.exists()) {
        tempDir.mkdirs();
      }
      Configuration conf = new Configuration();
      conf.set("fs.trash.interval", "1440");
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
      hdfsCluster = new MiniDFSCluster.Builder(conf).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void tearDown() {
    try (FileSystem fs = hdfsCluster.getFileSystem()) {
      fs.delete(fs.getHomeDirectory(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    hdfsCluster.shutdown();
    File localDir = new File(LOCAL_FS_PREFIX);
    if (localDir.exists()) {
      localDir.delete();
    }
    if (tempDir.exists()) {
      tempDir.delete();
    }
  }

  @Test
  public void testDateWithStringCleanFiles() throws IOException {
    Fileset mockFileset = Mockito.mock(Fileset.class);
    String metalakeName = "metalake_1";
    String catalogName = "catalog_1";
    String schemaName = "schema_1";
    String name = "fileset_1";
    String mockedFilePath = LOCAL_FS_PREFIX + "/" + catalogName + "/" + schemaName + "/" + name;
    Mockito.when(mockFileset.storageLocation()).thenReturn(mockedFilePath);
    NameIdentifier identifier =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, name);
    Path datePath1 = new Path(mockedFilePath + "/date=20240101/xxx");
    Path datePath2 = new Path(mockedFilePath + "/date=20240329/xxx");
    Configuration conf = new Configuration();
    try (FileSystem fs = FileSystem.newInstance(conf)) {
      fs.mkdirs(datePath1);
      assertTrue(fs.exists(datePath1));
      fs.mkdirs(datePath2);
      assertTrue(fs.exists(datePath2));
      // test properties not valid
      Map<String, String> props1 = Maps.newHashMap();
      props1.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props1.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props1);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));

      Map<String, String> props2 = Maps.newHashMap();
      props2.put(
          FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
      props2.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props2);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));

      Map<String, String> props3 = Maps.newHashMap();
      props3.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props3.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props3);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));

      Map<String, String> props4 = Maps.newHashMap();
      props4.put(
          FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_WITH_STRING.name());
      props4.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props4.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props4);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertFalse(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));
      fs.delete(datePath2, true);
      assertFalse(fs.exists(datePath2));
    }
  }

  @Test
  public void testDateHourCleanFiles() throws IOException {
    Fileset mockFileset = Mockito.mock(Fileset.class);
    String metalakeName = "metalake_1";
    String catalogName = "catalog_1";
    String schemaName = "schema_1";
    String name = "fileset_2";
    String mockedFilePath = LOCAL_FS_PREFIX + "/" + catalogName + "/" + schemaName + "/" + name;
    Mockito.when(mockFileset.storageLocation()).thenReturn(mockedFilePath);
    NameIdentifier identifier =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, name);
    Path datePath1 = new Path(mockedFilePath + "/2024010112/xxx");
    Path datePath2 = new Path(mockedFilePath + "/2024032912/xxx");
    Configuration conf = new Configuration();
    try (FileSystem fs = FileSystem.newInstance(conf)) {
      fs.mkdirs(datePath1);
      assertTrue(fs.exists(datePath1));
      fs.mkdirs(datePath2);
      assertTrue(fs.exists(datePath2));

      // test properties not valid
      Map<String, String> props1 = Maps.newHashMap();
      props1.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props1.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props1);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));

      Map<String, String> props2 = Maps.newHashMap();
      props2.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_HOUR.name());
      props2.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props2);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));

      Map<String, String> props3 = Maps.newHashMap();
      props3.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props3.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props3);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));

      Map<String, String> props4 = Maps.newHashMap();
      props4.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.DATE_HOUR.name());
      props4.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props4.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props4);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));

      assertFalse(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));
      fs.delete(datePath2, true);
      assertFalse(fs.exists(datePath2));
    }
  }

  @Test
  public void testYearMonthDayCleanFiles() throws IOException {
    Fileset mockFileset = Mockito.mock(Fileset.class);
    String metalakeName = "metalake_1";
    String catalogName = "catalog_1";
    String schemaName = "schema_1";
    String name = "fileset_3";
    String mockedFilePath = LOCAL_FS_PREFIX + "/" + catalogName + "/" + schemaName + "/" + name;
    Mockito.when(mockFileset.storageLocation()).thenReturn(mockedFilePath);
    NameIdentifier identifier =
        NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, name);
    Path datePath1 = new Path(mockedFilePath + "/year=2024/month=01/day=01/xxx");
    Path datePath2 = new Path(mockedFilePath + "/year=2024/month=03/day=29/xxx");
    Path datePath3 = new Path(mockedFilePath + "/year=2023");
    Path datePath4 = new Path(mockedFilePath + "/year=2024/month=02");
    Configuration conf = new Configuration();
    try (FileSystem fs = FileSystem.newInstance(conf)) {
      fs.mkdirs(datePath1);
      assertTrue(fs.exists(datePath1));
      fs.mkdirs(datePath2);
      assertTrue(fs.exists(datePath2));
      fs.mkdirs(datePath3);
      assertTrue(fs.exists(datePath3));
      fs.mkdirs(datePath4);
      assertTrue(fs.exists(datePath4));

      // test properties not valid
      Map<String, String> props1 = Maps.newHashMap();
      props1.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props1.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props1);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));
      assertTrue(fs.exists(datePath3));
      assertTrue(fs.exists(datePath4));

      Map<String, String> props2 = Maps.newHashMap();
      props2.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.YEAR_MONTH_DAY.name());
      props2.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props2);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));
      assertTrue(fs.exists(datePath3));
      assertTrue(fs.exists(datePath4));

      Map<String, String> props3 = Maps.newHashMap();
      props3.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props3.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props3);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertTrue(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));
      assertTrue(fs.exists(datePath3));
      assertTrue(fs.exists(datePath4));

      Map<String, String> props4 = Maps.newHashMap();
      props4.put(FilesetProperties.PREFIX_PATTERN_KEY, FilesetPrefixPattern.YEAR_MONTH_DAY.name());
      props4.put(FilesetProperties.LIFECYCLE_TIME_NUM_KEY, "30");
      props4.put(
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY, FilesetLifecycleUnit.RETENTION_DAY.name());
      Mockito.when(mockFileset.properties()).thenReturn(props4);
      FilesetGarbageCleanJob.cleanFiles(Pair.of(identifier, mockFileset));
      assertFalse(fs.exists(datePath1));
      assertTrue(fs.exists(datePath2));
      assertFalse(fs.exists(datePath3));
      assertFalse(fs.exists(datePath4));

      fs.delete(datePath2, true);
      assertFalse(fs.exists(datePath2));
    }
  }

  @Test
  public void testProcessMonthDir() throws IOException {
    NameIdentifier identifier =
        NameIdentifier.ofFileset("test_metalake", "test_catalog", "test_schema", "test_fileset");
    String extractedYear = "2024";
    String extractedMonth = "01";
    Integer ttlDate = 20240401;
    Pattern dayDirNamePattern = Pattern.compile("^day=(\\d{2})");

    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    Path dirPath = new Path(LOCAL_FS_PREFIX + "/catalog/db/fileset/year=2024/month=01");
    Mockito.when(fileStatus.getPath()).thenReturn(dirPath);
    Mockito.when(fileStatus.isDirectory()).thenReturn(true);

    FileSystem fs = Mockito.mock(FileSystem.class);
    FileStatus[] dayDirs = new FileStatus[5];

    for (int i = 0; i < 5; i++) {
      FileStatus dayStatus = Mockito.mock(FileStatus.class);
      Path dayPath = new Path(LOCAL_FS_PREFIX + "/catalog/db/fileset/year=2024/month=01/day=0" + i);
      Mockito.when(dayStatus.getPath()).thenReturn(dayPath);
      Mockito.when(dayStatus.isDirectory()).thenReturn(true);
      dayDirs[i] = dayStatus;
      Mockito.when(
              FilesetGarbageCleanJob.processDayDir(
                  identifier,
                  dayStatus,
                  dayDirNamePattern,
                  extractedYear,
                  extractedMonth,
                  ttlDate,
                  fs))
          .thenReturn(true);
    }

    Mockito.when(fs.listStatus(fileStatus.getPath())).thenReturn(dayDirs);
    assertTrue(
        FilesetGarbageCleanJob.processMonthDir(identifier, fileStatus, extractedYear, ttlDate, fs));

    Mockito.when(fs.listStatus(fileStatus.getPath())).thenReturn(null);
    Mockito.when(fs.delete(fileStatus.getPath(), true)).thenReturn(false);
    assertFalse(
        FilesetGarbageCleanJob.processMonthDir(identifier, fileStatus, extractedYear, ttlDate, fs));

    Mockito.when(fs.delete(fileStatus.getPath(), true)).thenThrow(IOException.class);
    assertFalse(
        FilesetGarbageCleanJob.processMonthDir(identifier, fileStatus, extractedYear, ttlDate, fs));
  }

  @Test
  public void testProcessDayDir() throws IOException {
    NameIdentifier identifier =
        NameIdentifier.ofFileset("test_metalake", "test_catalog", "test_schema", "test_fileset");
    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    Path dirPath = new Path(LOCAL_FS_PREFIX + "/catalog/db/fileset/year=2024/month=01/day=01");
    Pattern dayDirNamePattern = Pattern.compile("^day=(\\d{2})");
    Mockito.when(fileStatus.getPath()).thenReturn(dirPath);
    Mockito.when(fileStatus.isDirectory()).thenReturn(true);
    String extractedYear = "2024";
    String extractedMonth = "01";
    Integer ttlDate = 20240401;
    FileSystem fs = Mockito.mock(FileSystem.class);
    Mockito.when(fs.delete(dirPath, true)).thenReturn(true);
    assertTrue(
        FilesetGarbageCleanJob.processDayDir(
            identifier, fileStatus, dayDirNamePattern, extractedYear, extractedMonth, ttlDate, fs));

    FileSystem fs1 = Mockito.mock(FileSystem.class);
    Mockito.when(fs.delete(dirPath, true)).thenThrow(IOException.class);
    assertFalse(
        FilesetGarbageCleanJob.processDayDir(
            identifier,
            fileStatus,
            dayDirNamePattern,
            extractedYear,
            extractedMonth,
            ttlDate,
            fs1));

    FileStatus fileStatus1 = Mockito.mock(FileStatus.class);
    Mockito.when(fileStatus1.getPath()).thenReturn(dirPath);
    Mockito.when(fileStatus1.isDirectory()).thenReturn(false);
    assertTrue(
        FilesetGarbageCleanJob.processDayDir(
            identifier,
            fileStatus1,
            dayDirNamePattern,
            extractedYear,
            extractedMonth,
            ttlDate,
            fs));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMoveToTrash() throws IOException {
    try (FileSystem fs = hdfsCluster.getFileSystem()) {
      Path hdfsPath = new Path(fs.getHomeDirectory() + "/tmp/test");
      fs.mkdirs(hdfsPath);
      FileStatus pathStatus = fs.getFileStatus(hdfsPath);
      Mockito.when(FilesetGarbageCleanJob.cliParser.skipTrash()).thenReturn(false);
      FilesetGarbageCleanJob.tryToDelete(fs, pathStatus);
      TrashPolicy trashPolicy = TrashPolicy.getInstance(fs.getConf(), fs, fs.getHomeDirectory());
      Path trashPath = Path.mergePaths(trashPolicy.getCurrentTrashDir(), hdfsPath);
      assertTrue(fs.exists(trashPath));
    }
  }
}
