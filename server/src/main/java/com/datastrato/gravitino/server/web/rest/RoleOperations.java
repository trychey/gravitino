/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.catalog.SchemaDispatcher;
import com.datastrato.gravitino.catalog.TableDispatcher;
import com.datastrato.gravitino.catalog.TopicDispatcher;
import com.datastrato.gravitino.dto.authorization.SecurableObjectDTO;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.dto.responses.DeleteResponse;
import com.datastrato.gravitino.dto.responses.RoleResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import com.google.common.base.Preconditions;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/roles")
public class RoleOperations {
  private static final Logger LOG = LoggerFactory.getLogger(RoleOperations.class);

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public RoleOperations() {
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
  }

  @GET
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-role", absolute = true)
  public Response getRole(@PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RoleResponse(
                      DTOConverters.toDTO(accessControlManager.getRole(metalake, role)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.GET, role, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-role", absolute = true)
  public Response createRole(@PathParam("metalake") String metalake, RoleCreateRequest request) {
    try {
      // TODO: Supports multiple securable objects. There will be some limited support for multiple
      //  securable objects in the future.
      //  The securable objects in the same role should under the same catalog.
      //  If a role contains a metalake securable object, the role can't contain any other securable
      //  object.
      Preconditions.checkArgument(
          request.getSecurableObjects() != null && request.getSecurableObjects().length == 1,
          "The size of securable objects must be 1");

      for (SecurableObjectDTO object : request.getSecurableObjects()) {
        checkSecurableObjects(metalake, object);
      }

      return Utils.doAs(
          httpRequest,
          () -> {
            SecurableObject securableObject =
                SecurableObjects.parse(
                    request.getSecurableObjects()[0].fullName(),
                    request.getSecurableObjects()[0].type(),
                    request.getSecurableObjects()[0].privileges().stream()
                        .map(
                            privilege -> {
                              if (privilege.condition().equals(Privilege.Condition.ALLOW)) {
                                return Privileges.allow(privilege.name());
                              } else {
                                return Privileges.deny(privilege.name());
                              }
                            })
                        .collect(Collectors.toList()));

            return Utils.ok(
                new RoleResponse(
                    DTOConverters.toDTO(
                        accessControlManager.createRole(
                            metalake,
                            request.getName(),
                            request.getProperties(),
                            securableObject))));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-role", absolute = true)
  public Response deleteRole(
      @PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = accessControlManager.deleteRole(metalake, role);
            if (!deleted) {
              LOG.warn("Failed to delete role {} under metalake {}", role, metalake);
            }
            return Utils.ok(new DeleteResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.DELETE, role, metalake, e);
    }
  }

  // Check every securable object whether exists and is imported.
  private void checkSecurableObjects(String metalake, SecurableObjectDTO object) {
    NameIdentifier identifier;

    // Securable object ignores the metalake namespace, so we should add it back.
    if (object.type() == MetadataObject.Type.METALAKE) {
      identifier = NameIdentifier.parse(object.fullName());
    } else {
      identifier = NameIdentifier.parse(String.format("%s.%s", metalake, object.fullName()));
    }

    String existErrMsg = "Securable object % doesn't exist";

    TreeLockUtils.doWithTreeLock(
        identifier,
        LockType.READ,
        () -> {
          switch (object.type()) {
            case METALAKE:
              if (!GravitinoEnv.getInstance().metalakeDispatcher().metalakeExists(identifier)) {
                throw new IllegalArgumentException(String.format(existErrMsg, object.fullName()));
              }

              break;
            case CATALOG:
              if (!GravitinoEnv.getInstance().catalogDispatcher().catalogExists(identifier)) {
                throw new IllegalArgumentException(String.format(existErrMsg, object.fullName()));
              }

              break;
            case SCHEMA:
              {
                SchemaDispatcher dispatcher = GravitinoEnv.getInstance().schemaDispatcher();
                if (!dispatcher.schemaExists(identifier)) {
                  throw new IllegalArgumentException(String.format(existErrMsg, object.fullName()));
                }
                break;
              }
            case FILESET:
              if (!GravitinoEnv.getInstance().filesetDispatcher().filesetExists(identifier)) {
                throw new IllegalArgumentException(String.format(existErrMsg, object.fullName()));
              }

              break;
            case TABLE:
              {
                TableDispatcher dispatcher = GravitinoEnv.getInstance().tableDispatcher();
                if (!dispatcher.tableExists(identifier)) {
                  throw new IllegalArgumentException(String.format(existErrMsg, object.fullName()));
                }

                break;
              }
            case TOPIC:
              {
                TopicDispatcher dispatcher = GravitinoEnv.getInstance().topicDispatcher();
                if (!dispatcher.topicExists(identifier)) {
                  throw new IllegalArgumentException(String.format(existErrMsg, object.fullName()));
                }

                break;
              }
            default:
              throw new IllegalArgumentException(
                  String.format("Doesn't support the type %s", object.type()));
          }

          return null;
        });
  }
}
