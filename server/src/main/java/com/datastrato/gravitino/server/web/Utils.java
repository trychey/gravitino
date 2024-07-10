/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.cipher.CipherUtils;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.server.authentication.SimpleConfig;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.base.Splitter;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;

public class Utils {

  private static final String REMOTE_USER = "gravitino";
  private static final boolean LOCAL_ENV =
      GravitinoEnv.getInstance().config().get(SimpleConfig.LOCAL_ENV);
  private static final String ENCRYPTED_READ_ONLY_USERS =
      GravitinoEnv.getInstance().config().get(SimpleConfig.READONLY_SUPER_USERS);
  private static volatile Set<String> READ_ONLY_USERS = null;
  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  private Utils() {}

  public static String remoteUser(HttpServletRequest httpRequest) {
    return Optional.ofNullable(httpRequest.getRemoteUser()).orElse(REMOTE_USER);
  }

  public static <T> Response ok(T t) {
    return Response.status(Response.Status.OK).entity(t).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response ok() {
    return Response.status(Response.Status.NO_CONTENT).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response illegalArguments(String message) {
    return illegalArguments(message, null);
  }

  public static Response illegalArguments(String message, Throwable throwable) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(ErrorResponse.illegalArguments(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response internalError(String message) {
    return internalError(message, null);
  }

  public static Response internalError(String message, Throwable throwable) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(ErrorResponse.internalError(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response notFound(String type, String message) {
    return notFound(type, message, null);
  }

  public static Response notFound(String message, Throwable throwable) {
    return notFound(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response notFound(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(ErrorResponse.notFound(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response alreadyExists(String type, String message) {
    return alreadyExists(type, message, null);
  }

  public static Response alreadyExists(String message, Throwable throwable) {
    return alreadyExists(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response alreadyExists(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.alreadyExists(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response nonEmpty(String type, String message) {
    return nonEmpty(type, message, null);
  }

  public static Response nonEmpty(String message, Throwable throwable) {
    return nonEmpty(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response nonEmpty(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.nonEmpty(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response unsupportedOperation(String message) {
    return unsupportedOperation(message, null);
  }

  public static Response unsupportedOperation(String message, Throwable throwable) {
    return Response.status(Response.Status.METHOD_NOT_ALLOWED)
        .entity(ErrorResponse.unsupportedOperation(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response doAs(
      HttpServletRequest httpRequest, PrivilegedExceptionAction<Response> action) throws Exception {
    UserPrincipal principal =
        (UserPrincipal)
            httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    if (LOCAL_ENV) {
      if (principal == null) {
        principal = new UserPrincipal(AuthConstants.ANONYMOUS_USER);
      }
    } else {
      if (principal == null) {
        return Response.status(Response.Status.UNAUTHORIZED).build();
      }
      if (StringUtils.isNotBlank(ENCRYPTED_READ_ONLY_USERS)) {
        if (READ_ONLY_USERS == null) {
          synchronized (Utils.class) {
            if (READ_ONLY_USERS == null) {
              // load read only users from config
              String readOnlyUsers =
                  CipherUtils.decryptStringWithoutCompress(ENCRYPTED_READ_ONLY_USERS);
              READ_ONLY_USERS = SPLITTER.splitToStream(readOnlyUsers).collect(Collectors.toSet());
            }
          }
        }
        // check read only users
        if (READ_ONLY_USERS.contains(principal.getName())) {
          // check whether it is `GET` request
          if (!httpRequest.getMethod().equals("GET")) {
            return Response.status(Response.Status.FORBIDDEN).build();
          }
        }
      }
    }
    return PrincipalUtils.doAs(principal, action);
  }
}
