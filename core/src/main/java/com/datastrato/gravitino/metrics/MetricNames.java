/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics;

public class MetricNames {
  public static final String HTTP_PROCESS_DURATION = "http-request-duration-seconds";
  public static final String SERVER_IDLE_THREAD_NUM = "http-server.idle-thread.num";
  public static final String SERVER_QUEUE_JOBS = "http-server.queue-jobs.num";
  public static final String SERVER_MAX_AVAILABLE_THREAD_NUM =
      "http-server.max-available-thread.num";
  public static final String SERVER_UTILIZED_THREAD_NUM = "http-server.utilized-thread.num";
  public static final String SERVER_THREAD_UTILIZED_RATE = "http-server.thread-utilized.rate";
  public static final String SQL_SESSION_ACTIVE_NUM = "sql-session-active-num";
  public static final String SQL_SESSION_IDLE_NUM = "sql-session-idle-num";
  public static final String SQL_SESSION_MAX_NUM = "sql-session-max-num";
  public static final String SQL_SESSION_UTILIZED_RATE = "sql-session-utilized-rate";

  private MetricNames() {}
}
