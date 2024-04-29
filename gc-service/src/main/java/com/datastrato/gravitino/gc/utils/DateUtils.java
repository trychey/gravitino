/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.gc.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateUtils {
  public static Integer computeTTLDateByDateString(String baseDate, Integer lifecycleTime) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    LocalDate ttlDate = LocalDate.parse(baseDate, formatter).minusDays(lifecycleTime);
    return Integer.parseInt(formatter.format(ttlDate));
  }

  private DateUtils() {}
}
