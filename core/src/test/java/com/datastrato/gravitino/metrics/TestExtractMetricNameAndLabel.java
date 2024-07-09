/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics;

import com.datastrato.gravitino.metrics.source.MetricsSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExtractMetricNameAndLabel {

  CustomMappingSampleBuilder sampleBuilder =
      new CustomMappingSampleBuilder(MetricsSystem.getMetricNameAndLabelRules());

  private static final ImmutableList<String> httpMetrics =
      ImmutableList.of(
          MetricNames.SERVER_QUEUE_JOBS,
          MetricNames.SERVER_THREAD_UTILIZED_RATE,
          MetricNames.SERVER_IDLE_THREAD_NUM,
          MetricNames.SERVER_MAX_AVAILABLE_THREAD_NUM,
          MetricNames.SERVER_UTILIZED_THREAD_NUM,
          ".update-table." + MetricNames.HTTP_PROCESS_DURATION);

  private static final ImmutableList<String> httpMetricSources =
      ImmutableList.of(
          MetricsSource.GRAVITINO_SERVER_METRIC_NAME,
          MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME);

  private static final ImmutableList<String> sqlMetrics =
      ImmutableList.of(
          MetricNames.SQL_SESSION_UTILIZED_RATE,
          MetricNames.SQL_SESSION_ACTIVE_NUM,
          MetricNames.SQL_SESSION_IDLE_NUM,
          MetricNames.SQL_SESSION_MAX_NUM);

  private static final ImmutableList<String> sqlMetricSources =
      ImmutableList.of(MetricsSource.RELATIONAL_STORAGE_METRIC_NAME);

  private void checkResult(String dropwizardName, String metricName, Map<String, String> labels) {
    Sample sample =
        sampleBuilder.createSample(dropwizardName, "", Arrays.asList(), Arrays.asList(), 0);

    Assertions.assertEquals(metricName, sample.name);
    Assertions.assertEquals(labels.keySet(), new HashSet(sample.labelNames));
    Assertions.assertEquals(new HashSet(labels.values()), new HashSet(sample.labelValues));
  }

  @Test
  void testMapperConfig() {
    checkResult("jvm.total.used", "jvm_total_used", ImmutableMap.of());

    for (String metricSource : httpMetricSources) {
      for (String metricName : httpMetrics) {
        checkResult(
            metricSource + "." + metricName,
            Collector.sanitizeMetricName(metricSource)
                + "_"
                + Collector.sanitizeMetricName(metricName),
            ImmutableMap.of());
      }
    }

    for (String metricSource : sqlMetricSources) {
      for (String metricName : sqlMetrics) {
        checkResult(
            metricSource + "." + metricName,
            Collector.sanitizeMetricName(metricSource)
                + "_"
                + Collector.sanitizeMetricName(metricName),
            ImmutableMap.of());
      }
    }
  }
}
