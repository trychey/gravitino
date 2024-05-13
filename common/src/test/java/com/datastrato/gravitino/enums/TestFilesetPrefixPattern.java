/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.enums;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.utils.FilesetPrefixPatternUtils;
import org.junit.jupiter.api.Test;

public class TestFilesetPrefixPattern {

  @Test
  public void testAny() {
    int maxLevel = 3;

    String dirPath1 = "/test";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath1, FilesetPrefixPattern.ANY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/20240409/test";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath2, FilesetPrefixPattern.ANY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/date=20240409/test/xxx";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath3, FilesetPrefixPattern.ANY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/year=2024/month=04/day=09/test";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath4, FilesetPrefixPattern.ANY));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String dirPath5 = "/";
    assertFalse(FilesetPrefixPatternUtils.checkPrefixValid(dirPath5, FilesetPrefixPattern.ANY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel, false));

    String filePath1 = "/year=2024/month=04/day=09/test.parquet";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(filePath1, FilesetPrefixPattern.ANY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/year=2024/month=04/day=09/zzzz/test.parquet";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(filePath2, FilesetPrefixPattern.ANY));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));
  }

  @Test
  public void testDate() {
    int maxLevel = 3;

    String dirPath1 = "/20240409";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath1, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/20240409/xxx";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath2, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/date=20240409/test";
    assertFalse(FilesetPrefixPatternUtils.checkPrefixValid(dirPath3, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/20240409/test/zzz/ddd";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath4, FilesetPrefixPattern.DATE));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String filePath1 = "/date=20240409/xxx.parquet";
    assertFalse(FilesetPrefixPatternUtils.checkPrefixValid(filePath1, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/20240409/20240409/zzz/xxx.parquet";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(filePath2, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));

    String filePath3 = "/20240409/xxx.parquet";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(filePath3, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath3, maxLevel, true));

    String filePath4 = "/20240409/lll/zzz/qqq/xxx.parquet";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(filePath4, FilesetPrefixPattern.DATE));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath4, maxLevel, true));

    int maxLevel1 = 1;

    String dirPath5 = "/20240408";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath5, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel1, false));

    String dirPath6 = "/20240408/ddd";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(dirPath6, FilesetPrefixPattern.DATE));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath6, maxLevel1, false));

    String filePath5 = "/20240408/xxx.parquet";
    assertTrue(FilesetPrefixPatternUtils.checkPrefixValid(filePath5, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath5, maxLevel1, true));

    String filePath6 = "/xxx.parquet";
    assertFalse(FilesetPrefixPatternUtils.checkPrefixValid(filePath6, FilesetPrefixPattern.DATE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath6, maxLevel1, true));
  }

  @Test
  public void testDateHour() {
    int maxLevel = 3;

    String dirPath1 = "/2024040912";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath1, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/date=2024040912/test";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath2, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/2024040912/test/zzz/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath3, FilesetPrefixPattern.DATE_HOUR));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/2024040912/test/zzz";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath4, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String filePath1 = "/2024040912/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath1, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/2024040912/zzz/aqqq/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath2, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));

    String filePath3 = "/2024040912/zzz/aqqq/lll/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath3, FilesetPrefixPattern.DATE_HOUR));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath3, maxLevel, true));

    int maxLevel1 = 1;

    String dirPath5 = "/2024040812";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath5, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel1, false));

    String dirPath6 = "/2024040812/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath6, FilesetPrefixPattern.DATE_HOUR));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath6, maxLevel1, false));

    String filePath5 = "/2024040812/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath5, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath5, maxLevel1, true));

    String filePath6 = "/xxx.parquet";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath6, FilesetPrefixPattern.DATE_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath6, maxLevel1, true));
  }

  @Test
  public void testDateUSHour() {
    int maxLevel = 3;

    String dirPath1 = "/20240409_12";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath1, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/date=20240409_12/test";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath2, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/20240409_12/test/zzz/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath3, FilesetPrefixPattern.DATE_US_HOUR));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/20240409_12/test/zzz";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath4, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String filePath1 = "/20240409_12/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath1, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/20240409_12/zzz/aqqq/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath2, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));

    String filePath3 = "/20240409_12/zzz/aqqq/lll/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath3, FilesetPrefixPattern.DATE_US_HOUR));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath3, maxLevel, true));

    int maxLevel1 = 1;

    String dirPath5 = "/20240408_12";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath5, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel1, false));

    String dirPath6 = "/20240408_12/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath6, FilesetPrefixPattern.DATE_US_HOUR));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath6, maxLevel1, false));

    String filePath5 = "/20240408_12/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath5, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath5, maxLevel1, true));

    String filePath6 = "/xxx.parquet";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath6, FilesetPrefixPattern.DATE_US_HOUR));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath6, maxLevel1, true));
  }

  @Test
  public void testDateUSHourUSMinute() {
    int maxLevel = 3;

    String dirPath1 = "/20240409_12_00";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath1, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/date=20240409_12_00/test";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath2, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/20240409_12_00/test/zzz/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath3, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/20240409_12_00/test/zzz";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath4, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String filePath1 = "/20240409_12_00/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath1, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/20240409_12_00/zzz/aqqq/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath2, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));

    String filePath3 = "/20240409_12_00/zzz/aqqq/lll/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath3, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath3, maxLevel, true));

    int maxLevel1 = 1;

    String dirPath5 = "/20240408_12_00";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath5, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel1, false));

    String dirPath6 = "/20240408_12_00/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath6, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath6, maxLevel1, false));

    String filePath5 = "/20240408_12_00/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath5, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath5, maxLevel1, true));

    String filePath6 = "/xxx.parquet";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath6, FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath6, maxLevel1, true));
  }

  @Test
  public void testDateWithString() {
    int maxLevel = 3;

    String dirPath1 = "/date=20240409";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath1, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/date=2024040912";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath2, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/date=20240409/xxx/ddd/qqq";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath3, FilesetPrefixPattern.DATE_WITH_STRING));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/date=20240409/xxx/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath4, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String filePath1 = "/date=20240409/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath1, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/20240409/date=20240409/xxx.parquet";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath2, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));

    String filePath3 = "/date=20240409/zzz/qqq/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath3, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath3, maxLevel, true));

    String filePath4 = "/date=20240409/zzz/qqq/ppp/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath4, FilesetPrefixPattern.DATE_WITH_STRING));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath4, maxLevel, true));

    int maxLevel1 = 1;

    String dirPath5 = "/date=20240408";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath5, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel1, false));

    String dirPath6 = "/date=20240408/ddd";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            dirPath6, FilesetPrefixPattern.DATE_WITH_STRING));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath6, maxLevel1, false));

    String filePath5 = "/date=20240408/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath5, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath5, maxLevel1, true));

    String filePath6 = "/xxx.parquet";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(
            filePath6, FilesetPrefixPattern.DATE_WITH_STRING));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath6, maxLevel1, true));
  }

  @Test
  public void testYearMonthDay() {
    int maxLevel = 5;

    String dirPath1 = "/year=2024/month=04/day=09";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath1, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath1, maxLevel, false));

    String dirPath2 = "/year=2024/month=04/day=09/test/zzz";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath2, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath2, maxLevel, false));

    String dirPath3 = "/year=2024/month=04/day=09/test/zzz/wwww";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath3, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath3, maxLevel, false));

    String dirPath4 = "/date=20240401/month=04/day=09/test/zzz";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath4, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath4, maxLevel, false));

    String filePath1 = "/year=2024/month=04/day=09/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath1, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath1, maxLevel, true));

    String filePath2 = "/year=202412/month=04/day=09/date=20240409/xxx.parquet";
    assertFalse(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath2, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath2, maxLevel, true));

    String filePath3 = "/year=2024/month=04/day=09/zzz/www/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath3, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath3, maxLevel, true));

    String filePath4 = "/year=2024/month=04/day=09/zzz/www/qqq/xxx.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath4, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath4, maxLevel, true));

    int maxLevel1 = 3;

    String dirPath5 = "/year=2024/month=04/day=09/zzz";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath5, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(dirPath5, maxLevel1, false));

    String dirPath6 = "/year=2024/month=04/day=09";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(dirPath6, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(dirPath6, maxLevel1, false));

    String filePath5 = "/year=2024/month=04/day=09/zzz.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath5, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertTrue(FilesetPrefixPatternUtils.checkLevelValid(filePath5, maxLevel1, true));

    String filePath6 = "/year=2024/month=04/day=09/qqq/zzz.parquet";
    assertTrue(
        FilesetPrefixPatternUtils.checkPrefixValid(filePath6, FilesetPrefixPattern.YEAR_MONTH_DAY));
    assertFalse(FilesetPrefixPatternUtils.checkLevelValid(filePath6, maxLevel1, true));
  }
}
