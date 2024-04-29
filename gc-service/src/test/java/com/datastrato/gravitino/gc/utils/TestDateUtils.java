/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.gc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestDateUtils {

  @Test
  public void testComputeTTLDateByDateString() {
    String baseDate = "20240401";
    Integer lifecycleTime = 30;
    Integer actualTTLDate = DateUtils.computeTTLDateByDateString(baseDate, lifecycleTime);
    assertEquals(20240302, actualTTLDate);
  }
}
