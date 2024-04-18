/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Map;
import java.util.Optional;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AuthenticationFilter implements Filter {

  private final Map<String, Authenticator> filterAuthenticators = Maps.newHashMap();

  public AuthenticationFilter() {
    Authenticator[] authenticators = ServerAuthenticator.getInstance().authenticators();
    for (Authenticator authenticator : authenticators) {
      filterAuthenticators.put(authenticator.name(), authenticator);
    }
  }

  @VisibleForTesting
  AuthenticationFilter(Authenticator... authenticators) {
    for (Authenticator authenticator : authenticators) {
      filterAuthenticators.put(authenticator.name(), authenticator);
    }
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      HttpServletRequest req = (HttpServletRequest) request;
      String authenticatorType =
          Optional.ofNullable(req.getHeader(AuthConstants.HTTP_HEADER_AUTHORIZATION_TYPE))
              .orElse(AuthenticatorType.SIMPLE.name().toLowerCase());
      Authenticator authenticator =
          Preconditions.checkNotNull(
              filterAuthenticators.get(authenticatorType), "The Authenticator should not be null");

      Enumeration<String> headerData = req.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION);
      byte[] authData = null;
      if (headerData.hasMoreElements()) {
        authData = headerData.nextElement().getBytes(StandardCharsets.UTF_8);
      }
      if (authenticator.isDataFromToken()) {
        Principal principal = authenticator.authenticateToken(authData);
        request.setAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME, principal);
      }
      chain.doFilter(request, response);
    } catch (UnauthorizedException ue) {
      HttpServletResponse resp = (HttpServletResponse) response;
      if (!ue.getChallenges().isEmpty()) {
        // For some authentication, HTTP response can provide some challenge information
        // to let client to create correct authenticated request.
        // Refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/WWW-Authenticate
        for (String challenge : ue.getChallenges()) {
          resp.setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, challenge);
        }
      }
      resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, ue.getMessage());
    }
  }

  @Override
  public void destroy() {}
}
