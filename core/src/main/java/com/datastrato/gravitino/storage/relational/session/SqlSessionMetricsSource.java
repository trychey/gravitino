package com.datastrato.gravitino.storage.relational.session;

import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import org.apache.commons.dbcp2.BasicDataSource;

public class SqlSessionMetricsSource extends MetricsSource {
  public SqlSessionMetricsSource(String name, BasicDataSource dataSource) {
    super(name);
    registerGauge(MetricNames.SQL_SESSION_ACTIVE_NUM, dataSource::getNumActive);
    registerGauge(MetricNames.SQL_SESSION_IDLE_NUM, dataSource::getNumIdle);
    registerGauge(MetricNames.SQL_SESSION_MAX_NUM, dataSource::getMaxTotal);
    registerGauge(
        MetricNames.SQL_SESSION_UTILIZED_RATE,
        () -> (double) dataSource.getNumActive() / dataSource.getMaxTotal());
  }
}
