/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.dto.responses.SecretResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.secret.SecretsManager;
import com.datastrato.gravitino.server.web.Utils;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/metalakes/{metalake}/secrets")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SecretOperations {

  @Context private HttpServletRequest httpRequest;

  private final SecretsManager secretsManager;

  public SecretOperations() {
    this.secretsManager = GravitinoEnv.getInstance().secretsManager();
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-secret." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-secret", absolute = true)
  public Response getSecret(
      @PathParam("metalake") String metalake, @QueryParam("type") String type) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new SecretResponse(
                      DTOConverters.toDTO(secretsManager.getSecret(metalake, type)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleSecretException(OperationType.GET, type, metalake, e);
    }
  }
}
