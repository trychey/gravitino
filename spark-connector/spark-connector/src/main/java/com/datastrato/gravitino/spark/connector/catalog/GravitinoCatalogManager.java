/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to retrieve catalogs from Gravitino server. */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static volatile GravitinoCatalogManager gravitinoCatalogManager;

  private final AtomicBoolean closed = new AtomicBoolean();
  private final Cache<String, Catalog> gravitinoCatalogs;
  private final String metalakeName;
  private final GravitinoMetalake metalake;
  private final GravitinoAdminClient gravitinoClient;

  private GravitinoCatalogManager(String gravitinoUri, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoAdminClient.builder(gravitinoUri).build();
    // Will not evict catalog by default
    this.gravitinoCatalogs = CacheBuilder.newBuilder().build();
    this.metalake = gravitinoClient.loadMetalake(metalakeName);
  }

  public static GravitinoCatalogManager create(String gravitinoUrl, String metalakeName) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    synchronized (GravitinoCatalogManager.class) {
      if (gravitinoCatalogManager == null) {
        gravitinoCatalogManager = new GravitinoCatalogManager(gravitinoUrl, metalakeName);
      }
    }
    return gravitinoCatalogManager;
  }

  public static GravitinoCatalogManager get() {
    Preconditions.checkState(
        gravitinoCatalogManager != null, "GravitinoCatalogManager has not created yet");
    Preconditions.checkState(
        !gravitinoCatalogManager.isClosed(), "GravitinoCatalogManager is already closed");
    return gravitinoCatalogManager;
  }

  public boolean isClosed() {
    return closed.get();
  }

  public void close() {
    if (!closed.compareAndSet(false, true)) {
      throw new IllegalStateException("Gravitino Catalog is already closed");
    }
    gravitinoClient.close();
    gravitinoCatalogManager = null;
  }

  public Catalog getGravitinoCatalogInfo(String name) {
    try {
      return gravitinoCatalogs.get(name, () -> loadCatalog(name));
    } catch (ExecutionException e) {
      LOG.error(String.format("Load catalog %s failed", name), e);
      throw new RuntimeException(e);
    }
  }

  public String getMetalakeName() {
    return metalakeName;
  }

  public void loadRelationalCatalogs() {
    Catalog[] catalogs = metalake.listCatalogsInfo();
    Arrays.stream(catalogs)
        .filter(catalog -> Catalog.Type.RELATIONAL.equals(catalog.type()))
        .forEach(catalog -> gravitinoCatalogs.put(catalog.name(), catalog));
  }

  public Map<String, Catalog> getCatalogs() {
    return gravitinoCatalogs.asMap();
  }

  private Catalog loadCatalog(String catalogName) {
    Catalog catalog = metalake.loadCatalog(catalogName);
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return catalog;
  }
}
