/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSimpleTokenProvider {

  @MethodSource("EmptyTokenProvider")
  @ParameterizedTest
  public void testAuthentication(String... tokens) throws IOException {
    try (AuthDataProvider provider = new SimpleTokenProvider(tokens)) {
      Assertions.assertTrue(provider.hasTokenData());
      String user = System.getenv("GRAVITINO_USER");
      String token = new String(provider.getTokenData(), StandardCharsets.UTF_8);
      Assertions.assertTrue(token.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER));
      String tokenString =
          new String(
              Base64.getDecoder()
                  .decode(
                      token
                          .substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length())
                          .getBytes(StandardCharsets.UTF_8)),
              StandardCharsets.UTF_8);
      if (user != null) {
        Assertions.assertEquals(user + ":dummy", tokenString);
      } else {
        user = System.getProperty("user.name");
        Assertions.assertEquals(user + ":dummy", tokenString);
      }
    }
  }

  private static Stream<Arguments> EmptyTokenProvider() {
    return Stream.of(Arguments.of((Object) null), Arguments.of((Object) new String[] {}));
  }

  private static Stream<Arguments> tokenProvider() {
    return Stream.of(
        Arguments.of((Object) new String[] {"token"}),
        Arguments.of((Object) new String[] {"token", "token1"}));
  }

  @MethodSource("tokenProvider")
  @ParameterizedTest
  public void testAuthenticationWithToken(String... tokens) throws IOException {
    String expected = "token";
    try (AuthDataProvider provider = new SimpleTokenProvider(tokens)) {
      Assertions.assertTrue(provider.hasTokenData());
      String token = new String(provider.getTokenData(), StandardCharsets.UTF_8);
      Assertions.assertTrue(token.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER));
      String tokenString =
          new String(
              Base64.getDecoder()
                  .decode(
                      token
                          .substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length())
                          .getBytes(StandardCharsets.UTF_8)),
              StandardCharsets.UTF_8);
      Assertions.assertEquals(expected, tokenString);
    }
  }
}
