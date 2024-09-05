/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.secret.SecretDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a response for a secret. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SecretResponse extends BaseResponse {

  @JsonProperty("secret")
  private final SecretDTO secret;

  /**
   * Constructor for SecretResponse.
   *
   * @param secret The secret data transfer object.
   */
  public SecretResponse(SecretDTO secret) {
    super(0);
    this.secret = secret;
  }

  /** Default constructor for SecretResponse. (Used for Jackson deserialization.) */
  public SecretResponse() {
    super();
    this.secret = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the secret is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(secret != null, "secret must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secret.name()), "secret 'name' must not be null and empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secret.value()), "secret 'value' must not be null and empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secret.type()), "secret 'target' must not be null and empty");
    Preconditions.checkArgument(secret.auditInfo() != null, "secret 'auditInfo' must not be null");
  }
}
