/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.gc.jobs;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.gc.constants.DefaultConstants;
import com.datastrato.gravitino.gc.utils.CliParser;
import com.datastrato.gravitino.gc.utils.DateUtils;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableList;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a spark job responsible for deleting expired data in Filesets. */
public class FilesetGarbageCleanJob {
  private static final Logger LOG = LoggerFactory.getLogger(FilesetGarbageCleanJob.class);
  @VisibleForTesting static CliParser cliParser;
  private static final Pattern HDFS_LOCATION_PATTERN = Pattern.compile("^hdfs://[^/]+/.+");
  // just for testing
  private static final Pattern LOCAL_LOCATION_PATTERN = Pattern.compile("^file:/[^/]+/.+");
  private static final List<Pattern> VALID_LOCATIONS =
      ImmutableList.of(HDFS_LOCATION_PATTERN, LOCAL_LOCATION_PATTERN);
  private static final String DO_NOT_HAVE_PROPERTY_LOG_MESSAGE =
      "Fileset: `{}` does not have the `{}` property, skip garbage cleaning.";
  private static final String TRY_TO_DELETE_DIR_MESSAGE =
      "Sub directory: `{}` is older than the ttl date: `{}` for fileset: `{}`, try to delete it.";
  private static final String DELETE_DIR_ERROR_MESSAGE =
      "While deleting sub directory: `{}` happened exception: ";

  public static void main(String[] args) throws IOException {
    try (SparkSession sparkSession =
        SparkSession.builder()
            .appName("FilesetGarbageCleanJob")
            .enableHiveSupport()
            .getOrCreate()) {
      cliParser = new CliParser(args);

      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
              DefaultConstants.PRINCIPAL_NAME, cliParser.getPrincipalFilePath());
      ugi.doAs(
          (PrivilegedAction<Object>)
              () -> {
                executeJob();
                return null;
              });
    }
  }

  private static void executeJob() {
    try (GravitinoClient client =
        GravitinoClient.builder(cliParser.getGravitinoServerUri())
            .withMetalake(cliParser.getGravitinoMetalake())
            .withSimpleAuth(cliParser.getSuperUser())
            .build()) {
      Catalog filesetCatalog =
          client.loadCatalog(
              NameIdentifier.ofCatalog(
                  cliParser.getGravitinoMetalake(), cliParser.getFilesetCatalog()));
      // List all fileset schemas
      NameIdentifier[] filesetSchemas =
          filesetCatalog
              .asSchemas()
              .listSchemas(
                  Namespace.ofSchema(
                      cliParser.getGravitinoMetalake(), cliParser.getFilesetCatalog()));
      for (NameIdentifier schemaIdentifier : filesetSchemas) {
        // List all filesets in the schema
        NameIdentifier[] filesetIdentifiers =
            filesetCatalog
                .asFilesetCatalog()
                .listFilesets(
                    Namespace.ofFileset(
                        cliParser.getGravitinoMetalake(),
                        cliParser.getFilesetCatalog(),
                        schemaIdentifier.name()));
        for (NameIdentifier identifier : filesetIdentifiers) {
          try {
            Fileset fileset = filesetCatalog.asFilesetCatalog().loadFileset(identifier);
            cleanFiles(Pair.of(identifier, fileset));
          } catch (Exception e) {
            LOG.error("While processing fileset: `{}` happened exception: ", identifier, e);
          }
        }
      }
    }
  }

  @VisibleForTesting
  static void cleanFiles(Pair<NameIdentifier, Fileset> filesetPair) throws IOException {
    Map<String, String> properties = filesetPair.getRight().properties();
    // Filter filesets that not have ttl properties
    if (!properties.containsKey(FilesetProperties.PREFIX_PATTERN_KEY)) {
      LOG.warn(
          DO_NOT_HAVE_PROPERTY_LOG_MESSAGE,
          filesetPair.getLeft(),
          FilesetProperties.PREFIX_PATTERN_KEY);
      return;
    }

    FilesetPrefixPattern prefixPattern =
        FilesetPrefixPattern.valueOf(properties.get(FilesetProperties.PREFIX_PATTERN_KEY));
    if (prefixPattern == FilesetPrefixPattern.ANY) {
      LOG.warn(
          "Fileset: `{}`'s `{}` property is `{}` , which is not supported garbage cleaning now.",
          filesetPair.getLeft(),
          FilesetProperties.PREFIX_PATTERN_KEY,
          prefixPattern);
      return;
    }

    if (!properties.containsKey(FilesetProperties.LIFECYCLE_TIME_NUM_KEY)) {
      LOG.warn(
          DO_NOT_HAVE_PROPERTY_LOG_MESSAGE,
          filesetPair.getLeft(),
          FilesetProperties.LIFECYCLE_TIME_NUM_KEY);
      return;
    }

    int lifecycleTime = Integer.parseInt(properties.get(FilesetProperties.LIFECYCLE_TIME_NUM_KEY));
    if (lifecycleTime <= 0) {
      LOG.warn(
          "Fileset: `{}` lifecycle time: `{}` is not valid, skip garbage cleaning.",
          filesetPair.getLeft(),
          lifecycleTime);
      return;
    }

    if (!properties.containsKey(FilesetProperties.LIFECYCLE_TIME_UNIT_KEY)) {
      LOG.warn(
          DO_NOT_HAVE_PROPERTY_LOG_MESSAGE,
          filesetPair.getLeft(),
          FilesetProperties.LIFECYCLE_TIME_UNIT_KEY);
      return;
    }

    FilesetLifecycleUnit lifecycleUnit =
        FilesetLifecycleUnit.valueOf(properties.get(FilesetProperties.LIFECYCLE_TIME_UNIT_KEY));
    if (lifecycleUnit != FilesetLifecycleUnit.RETENTION_DAY) {
      LOG.warn(
          "Fileset: `{}` lifecycle time unit: `{}` is not valid, skip garbage cleaning.",
          filesetPair.getLeft(),
          lifecycleTime);
      return;
    }

    String storageLocation = filesetPair.getRight().storageLocation();
    boolean validLocation =
        VALID_LOCATIONS.stream()
            .anyMatch(
                locationPattern -> {
                  Matcher validLocationMatcher = locationPattern.matcher(storageLocation);
                  return validLocationMatcher.matches();
                });
    if (!validLocation) {
      LOG.warn(
          "Unsupported storage location: `{}` in the fileset: `{}` for garbage cleaning.",
          storageLocation,
          filesetPair.getLeft());
      return;
    }

    Integer ttlDate = DateUtils.computeTTLDateByDateString(cliParser.getDate(), lifecycleTime);
    try (FileSystem fs = FileSystem.newInstance(new Configuration())) {
      List<FileStatus> subDirs = Arrays.asList(fs.listStatus(new Path(storageLocation)));

      if (subDirs.isEmpty()) {
        LOG.info(
            "Fileset: `{}`'s subdirectory is empty, skip garbage cleaning.", filesetPair.getLeft());
        return;
      }

      Pattern dirNamePattern;
      switch (prefixPattern) {
        case DATE:
          // Match all the /xxxxxxxx dirs
          dirNamePattern = Pattern.compile("^(\\d{8})$");
          break;
        case DATE_HOUR:
          // Match all the /xxxxxxxxxx dirs
          dirNamePattern = Pattern.compile("^(\\d{10})$");
          break;
        case DATE_WITH_STRING:
          // Match all the /date=xxxxxxxx dirs
          dirNamePattern = Pattern.compile("^date=(\\d{8})$");
          break;
        case DATE_US_HOUR:
          // Match all the /xxxxxxxx_xx dirs
          dirNamePattern = Pattern.compile("^(\\d{8})_(\\d{2})$");
          break;
        case DATE_US_HOUR_US_MINUTE:
          // Match all the /xxxxxxxx_xx_xx dirs
          dirNamePattern = Pattern.compile("^(\\d{8})_(\\d{2})_(\\d{2})$");
          break;
        case YEAR_MONTH_DAY:
          // Match all the /year=xxxx dirs
          dirNamePattern = Pattern.compile("^year=(\\d{4})");
          break;
        default:
          LOG.warn(
              "Unsupported dir prefix pattern: `{}` to clean files for fileset: `{}`.",
              prefixPattern,
              filesetPair.getLeft());
          return;
      }

      subDirs.forEach(
          subDir -> doClean(prefixPattern, subDir, dirNamePattern, fs, filesetPair, ttlDate));
    }
  }

  private static void doClean(
      FilesetPrefixPattern prefixPattern,
      FileStatus subDir,
      Pattern dirNamePattern,
      FileSystem fs,
      Pair<NameIdentifier, Fileset> filesetPair,
      Integer ttlDate) {
    try {
      switch (prefixPattern) {
        case YEAR_MONTH_DAY:
          Matcher yearNameMatcher = dirNamePattern.matcher(subDir.getPath().getName());
          if (yearNameMatcher.find() && subDir.isDirectory()) {
            String extractedYear = yearNameMatcher.group(1);
            List<FileStatus> yearSubDirs = Arrays.asList(fs.listStatus(subDir.getPath()));
            if (!yearSubDirs.isEmpty()) {
              yearSubDirs.forEach(
                  yearSubDir ->
                      processMonthDir(
                          filesetPair.getLeft(), yearSubDir, extractedYear, ttlDate, fs));
            } else {
              // Construct the last date of the year, for example: 20231231
              int dirDate = Integer.parseInt(String.format("%s1231", extractedYear));
              if (dirDate < ttlDate) {
                LOG.info(
                    "Sub directory: `{}` does not have any subdirectories,"
                        + " and the last day of the year is older than the ttl date: `{}` for fileset: `{}`,"
                        + " try to delete it.",
                    subDir.getPath(),
                    ttlDate,
                    filesetPair.getLeft());
                tryToDelete(fs, subDir);
              }
            }
          }
          break;
        case DATE:
        case DATE_HOUR:
        case DATE_WITH_STRING:
        case DATE_US_HOUR:
        case DATE_US_HOUR_US_MINUTE:
          Matcher nameMatcher = dirNamePattern.matcher(subDir.getPath().getName());
          if (nameMatcher.find() && subDir.isDirectory()) {
            int extractedDate =
                prefixPattern == FilesetPrefixPattern.DATE_HOUR
                    ? Integer.parseInt(String.valueOf(nameMatcher.group(1)).substring(0, 8))
                    : Integer.parseInt(nameMatcher.group(1));
            if (extractedDate < ttlDate) {
              LOG.info(TRY_TO_DELETE_DIR_MESSAGE, subDir.getPath(), ttlDate, filesetPair.getLeft());
              tryToDelete(fs, subDir);
            }
          }
          break;
        case ANY:
        default:
          throw new UnsupportedOperationException(
              "Unsupported prefix type: "
                  + prefixPattern.name()
                  + " to delete sub dir for fileset: "
                  + filesetPair.getLeft().toString());
      }
    } catch (Exception e) {
      LOG.error(DELETE_DIR_ERROR_MESSAGE, subDir.getPath(), e);
    }
  }

  @VisibleForTesting
  static boolean processMonthDir(
      NameIdentifier identifier,
      FileStatus subDir,
      String extractedYear,
      Integer ttlDate,
      FileSystem fs) {
    try {
      // Match all the /month=xx dirs
      Pattern monthDirNamePattern = Pattern.compile("^month=(\\d{2})");
      Matcher monthNameMatcher = monthDirNamePattern.matcher(subDir.getPath().getName());
      if (monthNameMatcher.find() && subDir.isDirectory()) {
        String extractedMonth = monthNameMatcher.group(1);
        List<FileStatus> monthSubDirs = Arrays.asList(fs.listStatus(subDir.getPath()));
        Pattern dayDirNamePattern = Pattern.compile("^day=(\\d{2})");
        if (!monthSubDirs.isEmpty()) {
          monthSubDirs.forEach(
              monthSubDir ->
                  processDayDir(
                      identifier,
                      monthSubDir,
                      dayDirNamePattern,
                      extractedYear,
                      extractedMonth,
                      ttlDate,
                      fs));
        } else {
          YearMonth yearMonth =
              YearMonth.of(Integer.parseInt(extractedYear), Integer.parseInt(extractedMonth));
          int dirDate =
              Integer.parseInt(
                  String.format(
                      "%s%s%s", extractedYear, extractedMonth, yearMonth.lengthOfMonth()));
          if (dirDate < ttlDate) {
            LOG.info(
                "Sub directory: `{}` does not have any subdirectories,"
                    + " and the last day of the month is older than the ttl date: `{}` for fileset: `{}`, try to delete it.",
                subDir.getPath(),
                ttlDate,
                identifier);
            return tryToDelete(fs, subDir);
          }
        }
      }
    } catch (Exception e) {
      LOG.error(DELETE_DIR_ERROR_MESSAGE, subDir.getPath(), e);
      return false;
    }
    return true;
  }

  @VisibleForTesting
  static boolean processDayDir(
      NameIdentifier identifier,
      FileStatus subDir,
      Pattern dayDirNamePattern,
      String extractedYear,
      String extractedMonth,
      Integer ttlDate,
      FileSystem fs) {
    try {
      // Match all the /day=xx dirs
      Matcher dayNameMatcher = dayDirNamePattern.matcher(subDir.getPath().getName());
      if (dayNameMatcher.find() && subDir.isDirectory()) {
        String extractedDay = dayNameMatcher.group(1);
        int dirDate = Integer.parseInt(extractedYear + extractedMonth + extractedDay);
        if (dirDate < ttlDate) {
          LOG.info(TRY_TO_DELETE_DIR_MESSAGE, subDir.getPath(), ttlDate, identifier);
          return tryToDelete(fs, subDir);
        }
      }
    } catch (Exception e) {
      LOG.error(DELETE_DIR_ERROR_MESSAGE, subDir.getPath(), e);
      return false;
    }
    return true;
  }

  @VisibleForTesting
  static boolean tryToDelete(FileSystem fs, FileStatus subDir) throws IOException {
    if (!cliParser.skipTrash() && subDir.getPath().toString().toLowerCase().startsWith("hdfs://")) {
      return Trash.moveToAppropriateTrash(fs, subDir.getPath(), fs.getConf());
    } else {
      return fs.delete(subDir.getPath(), true);
    }
  }
}
