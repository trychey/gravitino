/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleAuthenticator {

  @Test
  public void testAuthentication() {
    SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator();
    Config config = new Config(false) {};
    simpleAuthenticator.initialize(config);
    Assertions.assertTrue(simpleAuthenticator.isDataFromToken());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER, simpleAuthenticator.authenticateToken(null).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator.authenticateToken("".getBytes(StandardCharsets.UTF_8)).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator.authenticateToken("abc".getBytes(StandardCharsets.UTF_8)).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator
            .authenticateToken(
                AuthConstants.AUTHORIZATION_BASIC_HEADER.getBytes(StandardCharsets.UTF_8))
            .getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER + "xx").getBytes(StandardCharsets.UTF_8))
            .getName());
    Assertions.assertEquals(
        "gravitino",
        simpleAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER
                        + new String(
                            Base64.getEncoder()
                                .encode("gravitino:gravitino".getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8))
            .getName());
  }

  @Test
  public void testProductionAuthentication() {
    SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator();
    Config config = new Config(false) {};
    config.set(SimpleConfig.LOCAL_ENV, false);
    config.set(SimpleConfig.SUPER_USERS, "credential1 ,credential2");
    simpleAuthenticator.initialize(config);

    byte[] credential1 =
        ("Basic "
                + Base64.getEncoder()
                    .encodeToString("credential1".getBytes(StandardCharsets.UTF_8)))
            .getBytes(StandardCharsets.UTF_8);

    Assertions.assertEquals(
        new UserPrincipal("credential1"), simpleAuthenticator.authenticateToken(credential1));

    byte[] credential2 =
        ("Basic "
                + Base64.getEncoder()
                    .encodeToString("credential2".getBytes(StandardCharsets.UTF_8)))
            .getBytes(StandardCharsets.UTF_8);
    Assertions.assertEquals(
        new UserPrincipal("credential2"), simpleAuthenticator.authenticateToken(credential2));

    byte[] unknown =
        ("Basic " + Base64.getEncoder().encodeToString("unknown".getBytes(StandardCharsets.UTF_8)))
            .getBytes(StandardCharsets.UTF_8);
    Assertions.assertThrows(
        UnauthorizedException.class, () -> simpleAuthenticator.authenticateToken(unknown));

    byte[] unknownHeader =
        ("ErrorHeader "
                + Base64.getEncoder()
                    .encodeToString("credential2".getBytes(StandardCharsets.UTF_8)))
            .getBytes(StandardCharsets.UTF_8);
    Assertions.assertThrows(
        UnauthorizedException.class, () -> simpleAuthenticator.authenticateToken(unknownHeader));
  }
}
