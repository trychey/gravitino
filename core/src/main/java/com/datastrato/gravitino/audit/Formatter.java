/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import com.datastrato.gravitino.listener.api.event.Event;

/**
 * Interface for formatting the event.
 *
 * @param <R>
 */
public interface Formatter<R> {

  /**
   * Format the event.
   *
   * @param event The event to format.
   * @return The formatted event.
   */
  R format(Event event);
}
