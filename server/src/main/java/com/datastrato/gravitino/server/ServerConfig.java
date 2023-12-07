/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;

public class ServerConfig extends Config {

  public static final ConfigEntry<Integer> SERVER_SHUTDOWN_TIMEOUT =
      new ConfigBuilder("gravitino.server.shutdown.timeout")
          .doc("The stop idle timeout(millis) of the Gravitino Server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(3 * 1000);

  public ServerConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  public ServerConfig() {
    this(true);
  }
}
