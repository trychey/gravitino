/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;

public interface SimpleConfig {

  String SIMPLE_CONFIG_PREFIX = "gravitino.authenticator.simple.";

  ConfigEntry<String> SUPER_USERS =
      new ConfigBuilder(SIMPLE_CONFIG_PREFIX + "superUsers")
          .doc(
              "The super users when Gravitino uses Simple as the authenticator, "
                  + "multiple audience need to be separated by commas.")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault("GravitinoServer");

  ConfigEntry<Boolean> LOCAL_ENV =
      new ConfigBuilder(SIMPLE_CONFIG_PREFIX + "localEnv")
          .doc(
              "Whether the simple authenticator is running is local environment."
                  + "Enable this to maintain compatibility for testing. Remember to disable this"
                  + "when using simple authenticator in production environment.")
          .version(ConfigConstants.VERSION_0_5_0)
          .booleanConf()
          .createWithDefault(true);
}
