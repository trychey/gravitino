/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.enums;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.utils.FilesetPrefixPatternUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class TestFilesetPrefixPattern {

  @Test
  public void testAny() {
    int maxLevel = 3;
    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(FilesetPrefixPattern.ANY, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(FilesetPrefixPattern.ANY, maxLevel, true);

    String dirPath1 = "/test";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/20240409/test";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertTrue(matcher2.matches());

    String dirPath3 = "/date=20240409/test/xxx";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertTrue(matcher3.matches());

    String dirPath4 = "/year=2024/month=04/day=09/test";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertFalse(matcher4.matches());

    String dirPath5 = "/";
    Matcher matcher5 = dirPattern.matcher(dirPath5);
    assertFalse(matcher5.matches());

    String filePath1 = "/year=2024/month=04/day=09/test.parquet";
    Matcher matcher6 = filePattern.matcher(filePath1);
    assertTrue(matcher6.matches());

    String filePath2 = "/year=2024/month=04/day=09/zzzz/test.parquet";
    Matcher matcher7 = filePattern.matcher(filePath2);
    assertFalse(matcher7.matches());
  }

  @Test
  public void testDate() {
    int maxLevel = 3;
    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(FilesetPrefixPattern.DATE, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(FilesetPrefixPattern.DATE, maxLevel, true);

    String dirPath1 = "/20240409";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/20240409/xxx";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertTrue(matcher2.matches());

    String dirPath3 = "/date=20240409/test";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertFalse(matcher3.matches());

    String dirPath4 = "/20240409/test/zzz/ddd";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertFalse(matcher4.matches());

    String filePath1 = "/date=20240409/xxx.parquet";
    Matcher matcher5 = filePattern.matcher(filePath1);
    assertFalse(matcher5.matches());

    String filePath2 = "/20240409/20240409/zzz/xxx.parquet";
    Matcher matcher6 = filePattern.matcher(filePath2);
    assertTrue(matcher6.matches());

    String filePath3 = "/20240409/xxx.parquet";
    Matcher matcher7 = filePattern.matcher(filePath3);
    assertTrue(matcher7.matches());

    String filePath4 = "/20240409/lll/zzz/qqq/xxx.parquet";
    Matcher matcher8 = filePattern.matcher(filePath4);
    assertFalse(matcher8.matches());

    int maxLevel1 = 1;

    Pattern dirPattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(FilesetPrefixPattern.DATE, maxLevel1, false);

    Pattern filePattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(FilesetPrefixPattern.DATE, maxLevel1, true);

    String dirPath5 = "/20240408";
    Matcher matcher9 = dirPattern1.matcher(dirPath5);
    assertTrue(matcher9.matches());

    String dirPath6 = "/20240408/ddd";
    Matcher matcher10 = dirPattern1.matcher(dirPath6);
    assertFalse(matcher10.matches());

    String filePath5 = "/20240408/xxx.parquet";
    Matcher matcher11 = filePattern1.matcher(filePath5);
    assertTrue(matcher11.matches());

    String filePath6 = "/xxx.parquet";
    Matcher matcher12 = filePattern1.matcher(filePath6);
    assertFalse(matcher12.matches());
  }

  @Test
  public void testDateHour() {
    int maxLevel = 3;

    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_HOUR, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_HOUR, maxLevel, true);

    String dirPath1 = "/2024040912";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/date=2024040912/test";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertFalse(matcher2.matches());

    String dirPath3 = "/2024040912/test/zzz/ddd";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertFalse(matcher3.matches());

    String dirPath4 = "/2024040912/test/zzz";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertTrue(matcher4.matches());

    String filePath1 = "/2024040912/xxx.parquet";
    Matcher matcher5 = filePattern.matcher(filePath1);
    assertTrue(matcher5.matches());

    String filePath2 = "/2024040912/zzz/aqqq/xxx.parquet";
    Matcher matcher6 = filePattern.matcher(filePath2);
    assertTrue(matcher6.matches());

    String filePath3 = "/2024040912/zzz/aqqq/lll/xxx.parquet";
    Matcher matcher7 = filePattern.matcher(filePath3);
    assertFalse(matcher7.matches());

    int maxLevel1 = 1;

    Pattern dirPattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_HOUR, maxLevel1, false);

    Pattern filePattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_HOUR, maxLevel1, true);

    String dirPath5 = "/2024040812";
    Matcher matcher9 = dirPattern1.matcher(dirPath5);
    assertTrue(matcher9.matches());

    String dirPath6 = "/2024040812/ddd";
    Matcher matcher10 = dirPattern1.matcher(dirPath6);
    assertFalse(matcher10.matches());

    String filePath5 = "/2024040812/xxx.parquet";
    Matcher matcher11 = filePattern1.matcher(filePath5);
    assertTrue(matcher11.matches());

    String filePath6 = "/xxx.parquet";
    Matcher matcher12 = filePattern1.matcher(filePath6);
    assertFalse(matcher12.matches());
  }

  @Test
  public void testDateUSHour() {
    int maxLevel = 3;

    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR, maxLevel, true);

    String dirPath1 = "/20240409_12";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/date=20240409_12/test";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertFalse(matcher2.matches());

    String dirPath3 = "/20240409_12/test/zzz/ddd";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertFalse(matcher3.matches());

    String dirPath4 = "/20240409_12/test/zzz";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertTrue(matcher4.matches());

    String filePath1 = "/20240409_12/xxx.parquet";
    Matcher matcher5 = filePattern.matcher(filePath1);
    assertTrue(matcher5.matches());

    String filePath2 = "/20240409_12/zzz/aqqq/xxx.parquet";
    Matcher matcher6 = filePattern.matcher(filePath2);
    assertTrue(matcher6.matches());

    String filePath3 = "/20240409_12/zzz/aqqq/lll/xxx.parquet";
    Matcher matcher7 = filePattern.matcher(filePath3);
    assertFalse(matcher7.matches());

    int maxLevel1 = 1;

    Pattern dirPattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR, maxLevel1, false);

    Pattern filePattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR, maxLevel1, true);

    String dirPath5 = "/20240408_12";
    Matcher matcher9 = dirPattern1.matcher(dirPath5);
    assertTrue(matcher9.matches());

    String dirPath6 = "/20240408_12/ddd";
    Matcher matcher10 = dirPattern1.matcher(dirPath6);
    assertFalse(matcher10.matches());

    String filePath5 = "/20240408_12/xxx.parquet";
    Matcher matcher11 = filePattern1.matcher(filePath5);
    assertTrue(matcher11.matches());

    String filePath6 = "/xxx.parquet";
    Matcher matcher12 = filePattern1.matcher(filePath6);
    assertFalse(matcher12.matches());
  }

  @Test
  public void testDateUSHourUSMinute() {
    int maxLevel = 3;

    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE, maxLevel, true);

    String dirPath1 = "/20240409_12_00";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/date=20240409_12_00/test";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertFalse(matcher2.matches());

    String dirPath3 = "/20240409_12_00/test/zzz/ddd";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertFalse(matcher3.matches());

    String dirPath4 = "/20240409_12_00/test/zzz";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertTrue(matcher4.matches());

    String filePath1 = "/20240409_12_00/xxx.parquet";
    Matcher matcher5 = filePattern.matcher(filePath1);
    assertTrue(matcher5.matches());

    String filePath2 = "/20240409_12_00/zzz/aqqq/xxx.parquet";
    Matcher matcher6 = filePattern.matcher(filePath2);
    assertTrue(matcher6.matches());

    String filePath3 = "/20240409_12_00/zzz/aqqq/lll/xxx.parquet";
    Matcher matcher7 = filePattern.matcher(filePath3);
    assertFalse(matcher7.matches());

    int maxLevel1 = 1;

    Pattern dirPattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE, maxLevel1, false);

    Pattern filePattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_US_HOUR_US_MINUTE, maxLevel1, true);

    String dirPath5 = "/20240408_12_00";
    Matcher matcher9 = dirPattern1.matcher(dirPath5);
    assertTrue(matcher9.matches());

    String dirPath6 = "/20240408_12_00/ddd";
    Matcher matcher10 = dirPattern1.matcher(dirPath6);
    assertFalse(matcher10.matches());

    String filePath5 = "/20240408_12_00/xxx.parquet";
    Matcher matcher11 = filePattern1.matcher(filePath5);
    assertTrue(matcher11.matches());

    String filePath6 = "/xxx.parquet";
    Matcher matcher12 = filePattern1.matcher(filePath6);
    assertFalse(matcher12.matches());
  }

  @Test
  public void testDateWithString() {
    int maxLevel = 3;

    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_WITH_STRING, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_WITH_STRING, maxLevel, true);

    String dirPath1 = "/date=20240409";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/date=2024040912";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertFalse(matcher2.matches());

    String dirPath3 = "/date=20240409/xxx/ddd/qqq";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertFalse(matcher3.matches());

    String dirPath4 = "/date=20240409/xxx/ddd";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertTrue(matcher4.matches());

    String filePath1 = "/date=20240409/xxx.parquet";
    Matcher matcher5 = filePattern.matcher(filePath1);
    assertTrue(matcher5.matches());

    String filePath2 = "/20240409/date=20240409/xxx.parquet";
    Matcher matcher6 = filePattern.matcher(filePath2);
    assertFalse(matcher6.matches());

    String filePath3 = "/date=20240409/zzz/qqq/xxx.parquet";
    Matcher matcher7 = filePattern.matcher(filePath3);
    assertTrue(matcher7.matches());

    String filePath4 = "/date=20240409/zzz/qqq/ppp/xxx.parquet";
    Matcher matcher8 = filePattern.matcher(filePath4);
    assertFalse(matcher8.matches());

    int maxLevel1 = 1;

    Pattern dirPattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_WITH_STRING, maxLevel1, false);

    Pattern filePattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.DATE_WITH_STRING, maxLevel1, true);

    String dirPath5 = "/date=20240408";
    Matcher matcher9 = dirPattern1.matcher(dirPath5);
    assertTrue(matcher9.matches());

    String dirPath6 = "/date=20240408/ddd";
    Matcher matcher10 = dirPattern1.matcher(dirPath6);
    assertFalse(matcher10.matches());

    String filePath5 = "/date=20240408/xxx.parquet";
    Matcher matcher11 = filePattern1.matcher(filePath5);
    assertTrue(matcher11.matches());

    String filePath6 = "/xxx.parquet";
    Matcher matcher12 = filePattern1.matcher(filePath6);
    assertFalse(matcher12.matches());
  }

  @Test
  public void testYearMonthDay() {
    int maxLevel = 5;

    Pattern dirPattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.YEAR_MONTH_DAY, maxLevel, false);

    Pattern filePattern =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.YEAR_MONTH_DAY, maxLevel, true);

    String dirPath1 = "/year=2024/month=04/day=09";
    Matcher matcher1 = dirPattern.matcher(dirPath1);
    assertTrue(matcher1.matches());

    String dirPath2 = "/year=2024/month=04/day=09/test/zzz";
    Matcher matcher2 = dirPattern.matcher(dirPath2);
    assertTrue(matcher2.matches());

    String dirPath3 = "/year=2024/month=04/day=09/test/zzz/wwww";
    Matcher matcher3 = dirPattern.matcher(dirPath3);
    assertFalse(matcher3.matches());

    String dirPath4 = "/date=20240401/month=04/day=09/test/zzz";
    Matcher matcher4 = dirPattern.matcher(dirPath4);
    assertFalse(matcher4.matches());

    String filePath1 = "/year=2024/month=04/day=09/xxx.parquet";
    Matcher matcher5 = filePattern.matcher(filePath1);
    assertTrue(matcher5.matches());

    String filePath2 = "/year=202412/month=04/day=09/date=20240409/xxx.parquet";
    Matcher matcher6 = filePattern.matcher(filePath2);
    assertFalse(matcher6.matches());

    String filePath3 = "/year=2024/month=04/day=09/zzz/www/xxx.parquet";
    Matcher matcher7 = filePattern.matcher(filePath3);
    assertTrue(matcher7.matches());

    String filePath4 = "/year=2024/month=04/day=09/zzz/www/qqq/xxx.parquet";
    Matcher matcher8 = filePattern.matcher(filePath4);
    assertFalse(matcher8.matches());

    int maxLevel1 = 3;

    Pattern dirPattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.YEAR_MONTH_DAY, maxLevel1, false);

    Pattern filePattern1 =
        FilesetPrefixPatternUtils.combinePrefixPattern(
            FilesetPrefixPattern.YEAR_MONTH_DAY, maxLevel1, true);

    String dirPath5 = "/year=2024/month=04/day=09/zzz";
    Matcher matcher9 = dirPattern1.matcher(dirPath5);
    assertFalse(matcher9.matches());

    String dirPath6 = "/year=2024/month=04/day=09";
    Matcher matcher10 = dirPattern1.matcher(dirPath6);
    assertTrue(matcher10.matches());

    String filePath5 = "/year=2024/month=04/day=09/zzz.parquet";
    Matcher matcher11 = filePattern1.matcher(filePath5);
    assertTrue(matcher11.matches());

    String filePath6 = "/year=2024/month=04/day=09/qqq/zzz.parquet";
    Matcher matcher12 = filePattern1.matcher(filePath6);
    assertFalse(matcher12.matches());
  }
}
