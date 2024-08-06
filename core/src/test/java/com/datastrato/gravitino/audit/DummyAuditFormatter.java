/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import com.beust.jcommander.internal.Maps;
import com.datastrato.gravitino.listener.api.event.Event;
import java.util.Map;

public class DummyAuditFormatter implements Formatter {
  @Override
  public Map<String, String> format(Event event) {
    Map<String, String> formatted = Maps.newHashMap();
    formatted.put("eventName", event.getClass().getSimpleName());
    formatted.put("user", event.user());
    formatted.put("timestamp", String.valueOf(event.eventTime()));
    return formatted;
  }
}
