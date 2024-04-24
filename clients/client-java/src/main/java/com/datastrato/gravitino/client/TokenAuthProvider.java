/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static java.util.Objects.requireNonNull;

import com.datastrato.gravitino.auth.AuthConstants;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class TokenAuthProvider implements AuthDataProvider {
  private final String token;

  public TokenAuthProvider(String token) {
    this.token = requireNonNull(token, "token is null");
  }

  @Override
  public boolean hasTokenData() {
    return true;
  }

  @Override
  public byte[] getTokenData() {
    return (AuthConstants.AUTHORIZATION_TOKEN_HEADER
            + new String(
                Base64.getEncoder().encode(token.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8))
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws IOException {
    // nothing to be released
  }
}
