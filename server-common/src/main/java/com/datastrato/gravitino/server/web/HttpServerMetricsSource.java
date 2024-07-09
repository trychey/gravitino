/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web;

import com.codahale.metrics.Clock;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerMetricsSource extends MetricsSource {
  private static final Logger LOG = LoggerFactory.getLogger(HttpServerMetricsSource.class);

  public HttpServerMetricsSource(String name, ResourceConfig resourceConfig, JettyServer server) {
    super(name);
    resourceConfig.register(
        new InstrumentedResourceMethodApplicationListener(
            getMetricRegistry(),
            Clock.defaultClock(),
            false,
            () ->
                new SlidingTimeWindowArrayReservoir(
                    getTimeSlidingWindowSeconds(), TimeUnit.SECONDS)));
    registerGauge(
        MetricNames.SERVER_IDLE_THREAD_NUM, () -> server.getThreadPool().getIdleThreads());

    // Jetty server use QueuedThreadpool now
    // if change thread type, follow metrics should be changed
    try {
      QueuedThreadPool serverQueuedThreadPool = (QueuedThreadPool) server.getThreadPool();
      registerGauge(MetricNames.SERVER_QUEUE_JOBS, serverQueuedThreadPool::getQueueSize);
      registerGauge(
          MetricNames.SERVER_MAX_AVAILABLE_THREAD_NUM,
          serverQueuedThreadPool::getMaxAvailableThreads);
      registerGauge(
          MetricNames.SERVER_UTILIZED_THREAD_NUM, serverQueuedThreadPool::getUtilizedThreads);
      registerGauge(
          MetricNames.SERVER_THREAD_UTILIZED_RATE, serverQueuedThreadPool::getUtilizationRate);
    } catch (ClassCastException classCastException) {
      LOG.error("Register http server metrics failed, exception: ", classCastException);
    }
  }
}
