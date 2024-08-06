/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import com.datastrato.gravitino.listener.api.event.Event;
import com.google.common.annotations.VisibleForTesting;

public abstract class AbstractAuditLogWriter implements AuditLogWriter {

  private Formatter formatter;

  public AbstractAuditLogWriter(Formatter formatter) {
    this.formatter = formatter;
  }

  public void write(Event event) {
    Object formatted = this.formatter.format(event);
    doWrite(formatted);
  }

  public abstract void doWrite(Object event);

  public abstract void close();

  @VisibleForTesting
  Formatter getFormatter() {
    return formatter;
  }
}
