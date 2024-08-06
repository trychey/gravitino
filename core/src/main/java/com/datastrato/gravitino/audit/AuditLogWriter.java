/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import com.datastrato.gravitino.listener.api.event.Event;
import java.util.Map;

/** Interface for writing the audit log. */
public interface AuditLogWriter {

  /**
   * Initialize the writer with the given configuration.
   *
   * @param config
   */
  void init(Map<String, String> config);

  /**
   * Write the audit event to storage.
   *
   * @param auditLog
   */
  void write(Event auditLog);

  /** Close the writer. */
  void close();
}
