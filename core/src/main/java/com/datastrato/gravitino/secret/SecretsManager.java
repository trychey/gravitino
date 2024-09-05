/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.storage.IdGenerator;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("unused")
public class SecretsManager {

  public static final String KERBEROS_SECRET_TYPE = "kerberos";

  private final EntityStore store;
  private final IdGenerator idGenerator;

  private final Config config;

  private final KerberosSecretPlugin kerberosSecretPlugin;

  public SecretsManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.config = config;
    this.kerberosSecretPlugin = new KerberosSecretPlugin(store, idGenerator, config);
  }

  public Secret getSecret(String metalake, String type) {
    if (StringUtils.isEmpty(type) || type.equalsIgnoreCase(KERBEROS_SECRET_TYPE)) {
      return kerberosSecretPlugin.getSecret(metalake);
    }
    throw new IllegalArgumentException("Invalid secret type: " + type);
  }
}
