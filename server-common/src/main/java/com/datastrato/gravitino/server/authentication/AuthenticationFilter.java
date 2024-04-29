/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Enumeration;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;

public class AuthenticationFilter implements Filter {

  private final Authenticator filterAuthenticator;

  public AuthenticationFilter() {
    filterAuthenticator = null;
  }

  @VisibleForTesting
  AuthenticationFilter(Authenticator authenticator) {
    this.filterAuthenticator = authenticator;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      Authenticator authenticator;
      if (filterAuthenticator == null) {
        authenticator = ServerAuthenticator.getInstance().authenticator();
      } else {
        authenticator = filterAuthenticator;
      }
      HttpServletRequest req = (HttpServletRequest) request;
      Enumeration<String> headerData = req.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION);
      String proxyUser = req.getHeader(AuthConstants.PROXY_USER);
      byte[] authData = null;
      if (headerData.hasMoreElements()) {
        authData = headerData.nextElement().getBytes(StandardCharsets.UTF_8);
      }
      if (authenticator.isDataFromToken()) {
        Principal principal = authenticator.authenticateToken(authData);
        request.setAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME, principal);
      }
      if (supportedProxyUser(authenticator) && StringUtils.isNotBlank(proxyUser)) {
        // We will use the proxy-user override the principal from the authData. If we want to use
        // principal from the authData, so do not pass the proxy-user in header.
        Principal principal = new UserPrincipal(proxyUser);
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

  // Only simple and kerberos support proxy user. We will not support proxy-user
  // for OAuth and Token.
  private boolean supportedProxyUser(Authenticator authenticator) {
    return authenticator instanceof SimpleAuthenticator
        || authenticator instanceof KerberosAuthenticator;
  }
}
