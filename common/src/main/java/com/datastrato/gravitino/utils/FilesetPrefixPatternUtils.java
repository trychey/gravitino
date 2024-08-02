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
    Pattern pattern = Pattern.compile(prefixPattern.getPrefixRegex());
    Matcher matcher = pattern.matcher(subDir);
    return matcher.matches();
  }
}
