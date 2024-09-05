/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Map;

/** The interface of a secret. The secret is the entity which contains any credentials. */
@Evolving
public interface Secret extends Auditable {

  /**
   * The name of the secret.
   *
   * @return The name of the secret.
   */
  String name();

  /**
   * The secret value. The value is a string, use base64 encoding if the value is binary.
   *
   * @return The value of the secret.
   */
  String value();

  /**
   * The type is the entity which the secret is used for.
   *
   * @return The type of the secret.
   */
  String type();

  /**
   * The properties of the secret. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the secret.
   */
  Map<String, String> properties();
}
