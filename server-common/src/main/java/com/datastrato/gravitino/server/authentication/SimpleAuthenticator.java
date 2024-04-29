/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.cipher.CipherUtils;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * SimpleAuthenticator will provide a basic authentication mechanism. SimpleAuthenticator will use
 * the identifier provided by the user without any validation.
 */
class SimpleAuthenticator implements Authenticator {

  private final Principal ANONYMOUS_PRINCIPAL = new UserPrincipal(AuthConstants.ANONYMOUS_USER);

  private final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  private String[] superUsers;

  private boolean localEnv;

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (localEnv) {
      return localEnvAuthenticateToken(tokenData);
    } else {
      return productionEnvAuthenticateToken(tokenData);
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    String encryptedConfig = config.get(SimpleConfig.SUPER_USERS);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(encryptedConfig), "The super users should not be null or empty.");
    String users = CipherUtils.decryptStringWithoutCompress(encryptedConfig);
    this.superUsers = SPLITTER.splitToStream(users).distinct().toArray(String[]::new);
    this.localEnv = config.get(SimpleConfig.LOCAL_ENV);
  }

  private Principal localEnvAuthenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      return ANONYMOUS_PRINCIPAL;
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)) {
      return ANONYMOUS_PRINCIPAL;
    }
    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      return ANONYMOUS_PRINCIPAL;
    }
    String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    if (StringUtils.isBlank(credential)) {
      return ANONYMOUS_PRINCIPAL;
    }
    try {
      String[] userInformation =
          new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8).split(":");
      if (userInformation.length != 2) {
        return ANONYMOUS_PRINCIPAL;
      }
      return new UserPrincipal(userInformation[0]);
    } catch (IllegalArgumentException ie) {
      return ANONYMOUS_PRINCIPAL;
    }
  }

  private Principal productionEnvAuthenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      throw new UnauthorizedException("Empty token authorization header");
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)
        || !authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      throw new UnauthorizedException("Invalid token authorization header");
    }
    String token = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    if (StringUtils.isBlank(token)) {
      throw new UnauthorizedException("Blank token found");
    }

    String userInformation = new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8);
    boolean authenticated = Arrays.asList(superUsers).contains(userInformation);
    if (authenticated) {
      return new UserPrincipal(userInformation);
    } else {
      throw new UnauthorizedException("Invalid authenticated user.");
    }
  }
}
