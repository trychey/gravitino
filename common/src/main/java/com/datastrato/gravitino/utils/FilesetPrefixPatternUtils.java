/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import java.util.regex.Pattern;

public class FilesetPrefixPatternUtils {
  private FilesetPrefixPatternUtils() {}

  public static Pattern combinePrefixPattern(
      FilesetPrefixPattern prefixPattern, int maxLevel, boolean isFile) {
    switch (prefixPattern) {
      case ANY:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.ANY.getPrefixRegex(),
                isFile
                    // For files, we can allow `/xxx` to appear one more time, which is like
                    // `/xxx/yyy/zzz.parquet`
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    // For dirs, we can't allow `/xxx` to appear one more time, which is only like
                    // `/xxx/yyy`
                    : maxLevel - prefixPattern.getDirLevel()));
      case DATE:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.DATE.getPrefixRegex(),
                isFile
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    : maxLevel - prefixPattern.getDirLevel()));
      case DATE_HOUR:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.DATE_HOUR.getPrefixRegex(),
                isFile
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    : maxLevel - prefixPattern.getDirLevel()));
      case DATE_WITH_STRING:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.DATE_WITH_STRING.getPrefixRegex(),
                isFile
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    : maxLevel - prefixPattern.getDirLevel()));
      case DATE_US_HOUR:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.DATE_US_HOUR.getPrefixRegex(),
                isFile
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    : maxLevel - prefixPattern.getDirLevel()));
      case DATE_US_HOUR_US_MINUTE:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE.getPrefixRegex(),
                isFile
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    : maxLevel - prefixPattern.getDirLevel()));
      case YEAR_MONTH_DAY:
        return Pattern.compile(
            String.format(
                FilesetPrefixPattern.YEAR_MONTH_DAY.getPrefixRegex(),
                isFile
                    ? maxLevel - prefixPattern.getDirLevel() + 1
                    : maxLevel - prefixPattern.getDirLevel()));
      default:
        throw new UnsupportedOperationException(
            "Unsupported prefix pattern type: " + prefixPattern);
    }
  }
}
