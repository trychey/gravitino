/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import org.apache.commons.lang3.StringUtils;

public interface TokenAuthConfig extends Configs {
  String TOKEN_AUTH_CONFIG_PREFIX = "gravitino.authenticator.token.";

  ConfigEntry<String> SERVER_URI =
      new ConfigBuilder(TOKEN_AUTH_CONFIG_PREFIX + "serverUri")
          .doc("The uri of the token server")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();
}
