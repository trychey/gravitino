/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.authentication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

public class TestAuthenticationFilter {

  @Test
  public void testDoFilterNormal() throws ServletException, IOException {

    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testDoFilterWithException() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED");
  }

  @Test
  public void testSimpleFilterWithProxyUser() throws ServletException, IOException {
    Authenticator authenticator = mock(SimpleAuthenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(mockRequest.getHeader(AuthConstants.PROXY_USER)).thenReturn("proxy-user");
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockRequest, times(2))
        .setAttribute(eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME), any());
    verify(mockRequest, times(1))
        .setAttribute(
            eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME),
            eq(new UserPrincipal("proxy-user")));
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testKerberosFilterWithProxyUser() throws ServletException, IOException {
    Authenticator authenticator = mock(KerberosAuthenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(mockRequest.getHeader(AuthConstants.PROXY_USER)).thenReturn("proxy-user");
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockRequest, times(2))
        .setAttribute(eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME), any());
    verify(mockRequest, times(1))
        .setAttribute(
            eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME),
            eq(new UserPrincipal("proxy-user")));
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testOAuthFilterWithNoProxyUser() throws ServletException, IOException {
    Authenticator authenticator = mock(OAuth2TokenAuthenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(mockRequest.getHeader(AuthConstants.PROXY_USER)).thenReturn("proxy-user");
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockRequest, times(1))
        .setAttribute(eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME), any());
    verify(mockRequest, never())
        .setAttribute(
            eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME),
            eq(new UserPrincipal("proxy-user")));
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testMultiFilterNormal() throws ServletException, IOException {
    Authenticator authenticator1 = mock(Authenticator.class);
    Authenticator authenticator2 = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator1, authenticator2));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator1.supportsToken(any())).thenReturn(false);
    when(authenticator1.isDataFromToken()).thenReturn(true);
    when(authenticator1.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    when(authenticator2.supportsToken(any())).thenReturn(true);
    when(authenticator2.isDataFromToken()).thenReturn(true);
    when(authenticator2.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testMultiFilterWithException() throws ServletException, IOException {
    Authenticator authenticator1 = mock(Authenticator.class);
    Authenticator authenticator2 = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator1, authenticator2));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator1.supportsToken(any())).thenReturn(false);
    when(authenticator1.isDataFromToken()).thenReturn(true);
    when(authenticator1.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    when(authenticator2.supportsToken(any())).thenReturn(true);
    when(authenticator2.isDataFromToken()).thenReturn(true);
    when(authenticator2.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED");
  }
}
