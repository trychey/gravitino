/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

public class TestTokenAuthenticator {
  private static TokenAuthenticator MOCK_AUTHENTICATOR;
  private static ClientAndServer MOCK_SERVER;

  @BeforeAll
  public static void setup() {
    MOCK_SERVER = startClientAndServer();
    Config config = new Config(false) {};
    config.set(
        TokenAuthConfig.SERVER_URI,
        String.format("http://127.0.0.1:%d/api/user", MOCK_SERVER.getPort()));
    MOCK_AUTHENTICATOR = new TokenAuthenticator();
    MOCK_AUTHENTICATOR.initialize(config);
  }

  @AfterAll
  public static void stop() {
    MOCK_SERVER.stop();
  }

  @Test
  public void testInvalidToken() {
    UnauthorizedException emptyToken =
        assertThrows(UnauthorizedException.class, () -> MOCK_AUTHENTICATOR.authenticateToken(null));
    assertEquals("Empty token authorization header", emptyToken.getMessage());
    UnauthorizedException invalidToken1 =
        assertThrows(
            UnauthorizedException.class,
            () -> MOCK_AUTHENTICATOR.authenticateToken("".getBytes(StandardCharsets.UTF_8)));
    assertEquals("Invalid token authorization header", invalidToken1.getMessage());
    UnauthorizedException invalidToken2 =
        assertThrows(
            UnauthorizedException.class,
            () -> MOCK_AUTHENTICATOR.authenticateToken("test".getBytes(StandardCharsets.UTF_8)));
    assertEquals("Invalid token authorization header", invalidToken2.getMessage());
  }

  @Test
  public void testFailed() {
    mockTokenResponse("1", 500, null);
    UnauthorizedException failedAuth1 =
        assertThrows(
            UnauthorizedException.class,
            () ->
                MOCK_AUTHENTICATOR.authenticateToken(
                    (AuthConstants.AUTHORIZATION_TOKEN_HEADER
                            + new String(
                                Base64.getEncoder().encode("1".getBytes(StandardCharsets.UTF_8)),
                                StandardCharsets.UTF_8))
                        .getBytes(StandardCharsets.UTF_8)));
    assertEquals(
        String.format("Unable to authenticate with token: %s, code: 500", "1"),
        failedAuth1.getMessage());

    mockTokenResponse("2", 400, "{\"code\":\"-1\",\"msg\":\"token not exist: 2\"}");
    UnauthorizedException failedAuth2 =
        assertThrows(
            UnauthorizedException.class,
            () ->
                MOCK_AUTHENTICATOR.authenticateToken(
                    (AuthConstants.AUTHORIZATION_TOKEN_HEADER
                            + new String(
                                Base64.getEncoder().encode("2".getBytes(StandardCharsets.UTF_8)),
                                StandardCharsets.UTF_8))
                        .getBytes(StandardCharsets.UTF_8)));
    assertEquals(
        String.format("Unable to authenticate with token: %s, code: 400", "2"),
        failedAuth2.getMessage());

    mockTokenResponse("3", 200, null);
    UnauthorizedException failedAuth3 =
        assertThrows(
            UnauthorizedException.class,
            () ->
                MOCK_AUTHENTICATOR.authenticateToken(
                    (AuthConstants.AUTHORIZATION_TOKEN_HEADER
                            + new String(
                                Base64.getEncoder().encode("3".getBytes(StandardCharsets.UTF_8)),
                                StandardCharsets.UTF_8))
                        .getBytes(StandardCharsets.UTF_8)));
    assertEquals(
        String.format("Unable to authenticate with token: %s", "3"), failedAuth3.getMessage());
  }

  @Test
  public void testSuccess() {
    mockTokenResponse(
        "4",
        200,
        "{\"username\":\"zhangsan\",\"workspaceId\":10001,\"role\":\"admin\",\"rangerRole\":\"workspace_10001.admin\"}");
    UserPrincipal principal =
        (UserPrincipal)
            MOCK_AUTHENTICATOR.authenticateToken(
                (AuthConstants.AUTHORIZATION_TOKEN_HEADER
                        + new String(
                            Base64.getEncoder().encode("4".getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8));
    assertEquals(new UserPrincipal("zhangsan:10001:admin"), principal);
  }

  private void mockTokenResponse(String token, int statusCode, String jsonResponse) {
    HttpRequest mockRequest =
        request("/api/user")
            .withMethod(Method.GET.name().toUpperCase(Locale.ROOT))
            .withQueryStringParameter("token", token);
    HttpResponse mockResponse = response().withStatusCode(statusCode);
    mockResponse = mockResponse.withBody(jsonResponse);
    MOCK_SERVER.when(mockRequest).respond(mockResponse);
  }
}
