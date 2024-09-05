/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.secret;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

class KerberosSecretConfig extends Config {

  private static final String SECRET_KERBEROS_PREFIX = "gravitino.secret.kerberos.";
  public static final String HOST = SECRET_KERBEROS_PREFIX + "host";
  public static final String USER = SECRET_KERBEROS_PREFIX + "user";
  public static final String PASSWORD = SECRET_KERBEROS_PREFIX + "password";
  public static final String FORMAT = SECRET_KERBEROS_PREFIX + "format";

  static final ConfigEntry<String> KERBEROS_HOST =
      new ConfigBuilder(HOST)
          .doc("The host of the keytab server")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  static final ConfigEntry<String> KERBEROS_USER =
      new ConfigBuilder(USER)
          .doc("The username of the keytab server")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  static final ConfigEntry<String> KERBEROS_PASSWORD =
      new ConfigBuilder(PASSWORD)
          .doc("The password of the keytab server")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  static final ConfigEntry<String> KERBEROS_FORMAT =
      new ConfigBuilder(FORMAT)
          .doc("The format of the keytab")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(
              i ->
                  i.equals("s_workspace_%s_krb@XIAOMI.HADOOP")
                      || i.equals("s_workspace_%s_test_krb@XIAOMI.HADOOP"),
              "The format of the keytab is not correct")
          .create();

  KerberosSecretConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
