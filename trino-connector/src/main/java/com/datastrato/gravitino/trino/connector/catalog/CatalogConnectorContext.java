/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.trino.connector.GravitinoConnector;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

/**
 * The CatalogConnector serves as a communication bridge between the Gravitino connector and its
 * internal connectors. It manages the lifecycle, configuration, and runtime environment of internal
 * connectors.
 */
public class CatalogConnectorContext {

  private final GravitinoCatalog catalog;
  private final GravitinoMetaLake metalake;

  // Connector communicates with trino
  private final GravitinoConnector connector;

  // Internal connector communicates with data storage
  private final Connector internalConnector;

  private final CatalogConnectorAdapter adapter;

  public CatalogConnectorContext(
      GravitinoCatalog catalog,
      GravitinoMetaLake metalake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    this.catalog = catalog;
    this.metalake = metalake;
    this.internalConnector = internalConnector;
    this.adapter = adapter;

    this.connector = new GravitinoConnector(catalog.geNameIdentifier(), this);
  }

  public GravitinoMetaLake getMetalake() {
    return metalake;
  }

  public GravitinoCatalog getCatalog() {
    return catalog;
  }

  public GravitinoConnector getConnector() {
    return connector;
  }

  public Connector getInternalConnector() {
    return internalConnector;
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return adapter.getTableProperties();
  }

  public List<PropertyMetadata<?>> getSchemaProperties() {
    return adapter.getSchemaProperties();
  }

  public List<PropertyMetadata<?>> getColumnProperties() {
    return adapter.getColumnProperties();
  }

  public void close() {
    this.internalConnector.shutdown();
  }

  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return adapter.getMetadataAdapter();
  }

  static class Builder {
    private final CatalogConnectorAdapter connectorAdapter;
    private GravitinoMetaLake metalake;
    private Connector internalConnector;
    private GravitinoCatalog catalog;

    Builder(CatalogConnectorAdapter connectorAdapter) {
      this.connectorAdapter = connectorAdapter;
    }

    public Builder clone() {
      return new Builder(connectorAdapter);
    }

    public Map<String, Object> buildConfig(GravitinoCatalog catalog) throws Exception {
      return connectorAdapter.buildInternalConnectorConfig(catalog);
    }

    Builder withMetalake(GravitinoMetaLake metalake) {
      this.metalake = metalake;
      return this;
    }

    Builder withInternalConnector(Connector internalConnector) {
      this.internalConnector = internalConnector;
      return this;
    }

    Builder withCatalog(GravitinoCatalog catalog) {
      this.catalog = catalog;
      return this;
    }

    CatalogConnectorContext build() {
      Preconditions.checkArgument(metalake != null, "metalake is not null");
      Preconditions.checkArgument(internalConnector != null, "internalConnector is not null");
      Preconditions.checkArgument(catalog != null, "catalog  ss not null");
      return new CatalogConnectorContext(catalog, metalake, internalConnector, connectorAdapter);
    }
  }
}
