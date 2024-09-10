/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.secret.Secret;

/** Interface for supporting secrets. It includes methods for getting secrets. */
@Evolving
public interface SupportsSecrets {

  /**
   * Get the secret in the metalake.
   *
   * @param type The type of the secret.
   * @return The corresponding secret.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  Secret getSecret(String type) throws NoSuchMetalakeException;
}
