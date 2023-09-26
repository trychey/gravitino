/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.trino.connector.GravitonConnector;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/**
 * The CatalogConnector serves as a communication bridge between the Graviton connector and its
 * internal connectors. It manages the lifecycle, configuration, and runtime environment of internal
 * connectors.
 */
public class CatalogConnectorContext {

  private final NameIdentifier catalogName;
  private final GravitonMetaLake metaLake;

  // connector communicate with trino
  protected GravitonConnector connector;
  // internal connector communicate with data storage
  protected Connector internalConnector;

  private CatalogConnectorAdapter adapter;

  public CatalogConnectorContext(
      NameIdentifier catalogName,
      GravitonMetaLake metaLake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    Preconditions.checkArgument(catalogName != null, "catalogName is not null");
    Preconditions.checkArgument(metaLake != null, "metaLake is not null");
    Preconditions.checkArgument(internalConnector != null, "internalConnector is not null");
    Preconditions.checkArgument(adapter != null, "adapter is not null");

    this.catalogName = catalogName;
    this.metaLake = metaLake;
    this.internalConnector = internalConnector;
    this.adapter = adapter;

    this.connector = new GravitonConnector(catalogName, this);
  }

  public GravitonMetaLake getMetaLake() {
    return metaLake;
  }

  public GravitonConnector getConnector() {
    return connector;
  }

  public Connector getInternalConnector() {
    return internalConnector;
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return adapter.getTableProperties();
  }

  public void close() {
    this.internalConnector.shutdown();
  }
}
