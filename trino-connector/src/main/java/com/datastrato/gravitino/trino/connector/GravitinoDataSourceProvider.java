/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** This class provides a ConnectorPageSource for trino read data from internal connector. */
public class GravitinoDataSourceProvider implements ConnectorPageSourceProvider {

  ConnectorPageSourceProvider internalPageSourceProvider;

  public GravitinoDataSourceProvider(ConnectorPageSourceProvider pageSourceProvider) {
    this.internalPageSourceProvider = pageSourceProvider;
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {
    return internalPageSourceProvider.createPageSource(
        GravitinoHandle.unWrap(transaction),
        session,
        GravitinoHandle.unWrap(split),
        GravitinoHandle.unWrap(table),
        GravitinoHandle.unWrap(columns),
        new GravitinoDynamicFilter(dynamicFilter));
  }

  class GravitinoDynamicFilter implements DynamicFilter {
    private final DynamicFilter delegate;

    GravitinoDynamicFilter(DynamicFilter dynamicFilter) {
      delegate = dynamicFilter;
    }

    @Override
    public Set<ColumnHandle> getColumnsCovered() {
      return delegate.getColumnsCovered();
    }

    @Override
    public CompletableFuture<?> isBlocked() {
      return delegate.isBlocked();
    }

    @Override
    public boolean isComplete() {
      return delegate.isComplete();
    }

    @Override
    public boolean isAwaitable() {
      return delegate.isAwaitable();
    }

    @Override
    public TupleDomain<ColumnHandle> getCurrentPredicate() {
      return delegate.getCurrentPredicate().transformKeys(GravitinoHandle::unWrap);
    }
  }
}
