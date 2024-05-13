/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilesetPrefixPatternUtils {
  private FilesetPrefixPatternUtils() {}

  public static boolean checkLevelValid(String subDir, int maxLevel, boolean isFile) {
    int actualSubDirLevel =
        subDir.startsWith("/") ? subDir.substring(1).split("/").length : subDir.split("/").length;
    if (isFile) {
      return actualSubDirLevel <= maxLevel + 1;
    } else {
      return actualSubDirLevel <= maxLevel;
    }
  }

  public static boolean checkPrefixValid(String subDir, FilesetPrefixPattern prefixPattern) {
    Pattern pattern;
    Matcher matcher;
    switch (prefixPattern) {
      case ANY:
        pattern = Pattern.compile(FilesetPrefixPattern.ANY.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      case DATE:
        pattern = Pattern.compile(FilesetPrefixPattern.DATE.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      case DATE_HOUR:
        pattern = Pattern.compile(FilesetPrefixPattern.DATE_HOUR.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      case DATE_WITH_STRING:
        pattern = Pattern.compile(FilesetPrefixPattern.DATE_WITH_STRING.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      case DATE_US_HOUR:
        pattern = Pattern.compile(FilesetPrefixPattern.DATE_US_HOUR.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      case DATE_US_HOUR_US_MINUTE:
        pattern = Pattern.compile(FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      case YEAR_MONTH_DAY:
        pattern = Pattern.compile(FilesetPrefixPattern.YEAR_MONTH_DAY.getPrefixRegex());
        matcher = pattern.matcher(subDir);
        return matcher.matches();
      default:
        throw new UnsupportedOperationException(
            "Unsupported prefix pattern type: " + prefixPattern);
    }
  }
}
