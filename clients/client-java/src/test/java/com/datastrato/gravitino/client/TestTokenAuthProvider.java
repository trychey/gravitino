/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.auth.AuthConstants;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

public class TestTokenAuthProvider {
  @Test
  public void testAuthentication() throws IOException {
    try (AuthDataProvider provider = new TokenAuthProvider("test:token")) {
      assertTrue(provider.hasTokenData());
      String token = new String(provider.getTokenData(), StandardCharsets.UTF_8);
      assertTrue(token.startsWith(AuthConstants.AUTHORIZATION_TOKEN_HEADER));
      String tokenString =
          new String(
              Base64.getDecoder()
                  .decode(
                      token
                          .substring(AuthConstants.AUTHORIZATION_TOKEN_HEADER.length())
                          .getBytes(StandardCharsets.UTF_8)),
              StandardCharsets.UTF_8);
      assertEquals("test:token", tokenString);
    }
  }

  @Test
  public void testNull() {
    assertThrows(NullPointerException.class, () -> new TokenAuthProvider(null));
  }
}
