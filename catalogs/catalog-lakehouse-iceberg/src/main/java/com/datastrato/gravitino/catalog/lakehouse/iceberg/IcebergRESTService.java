/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.aux.GravitinoAuxiliaryService;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergExceptionMapper;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import com.datastrato.gravitino.metrics.MetricsSystem;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import com.datastrato.gravitino.server.auth.AuthenticationFilter;
import com.datastrato.gravitino.server.web.HttpServerMetricsSource;
import com.datastrato.gravitino.server.web.JettyServer;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import java.util.Map;
import javax.servlet.Servlet;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTService implements GravitinoAuxiliaryService {

  private static Logger LOG = LoggerFactory.getLogger(IcebergRESTService.class);

  private JettyServer server;

  public static final String SERVICE_NAME = "iceberg-rest";

  private void initServer(IcebergConfig icebergConfig) {
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(icebergConfig);
    server = new JettyServer();
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().getMetricsSystem();
    server.initialize(serverConfig, SERVICE_NAME, false, metricsSystem);

    ResourceConfig config = new ResourceConfig();
    config.packages("com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest");

    config.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    config.register(IcebergExceptionMapper.class);
    HttpServerMetricsSource httpServerMetricsSource =
        new HttpServerMetricsSource(MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME, config, server);
    metricsSystem.register(httpServerMetricsSource);

    IcebergTableOps icebergTableOps = new IcebergTableOps(icebergConfig);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(icebergTableOps).to(IcebergTableOps.class).ranked(1);
          }
        });

    Servlet servlet = new ServletContainer(config);
    server.addServlet(servlet, "/iceberg/*");
    server.addFilter(new AuthenticationFilter(), "/iceberg/*");
  }

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig();
    icebergConfig.loadFromMap(properties, k -> true);
    initServer(icebergConfig);
    LOG.info("Iceberg REST service inited");
  }

  @Override
  public void serviceStart() {
    if (server != null) {
      try {
        server.start();
        LOG.info("Iceberg REST service started");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Iceberg REST service stopped");
    }
  }
}
