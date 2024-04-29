/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.dto.authorization.GroupDTO;
import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.requests.RoleRevokeRequest;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.GroupListResponse;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.UserListResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPermissionOperations extends JerseyTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
    GravitinoEnv.getInstance().setAccessControlManager(manager);
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(PermissionOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testGrantRolesToUser() {
    UserEntity userEntity =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withRoleNames(Lists.newArrayList("roles"))
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.grantRolesToUser(any(), any(), any())).thenReturn(userEntity);

    RoleGrantRequest request = new RoleGrantRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    UserResponse userResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    User user = userResponse.getUser();
    Assertions.assertEquals(userEntity.roles(), user.roles());
    Assertions.assertEquals(userEntity.name(), user.name());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(manager)
        .grantRolesToUser(any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(manager)
        .grantRolesToUser(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchRoleException
    doThrow(new NoSuchRoleException("mock error"))
        .when(manager)
        .grantRolesToUser(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchRoleException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).grantRolesToUser(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testGrantRolesToGroup() {
    GroupEntity groupEntity =
        GroupEntity.builder()
            .withId(1L)
            .withName("group")
            .withRoleNames(Lists.newArrayList("roles"))
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.grantRolesToGroup(any(), any(), any())).thenReturn(groupEntity);

    RoleGrantRequest request = new RoleGrantRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    GroupResponse grantResponse = resp.readEntity(GroupResponse.class);
    Assertions.assertEquals(0, grantResponse.getCode());

    Group group = grantResponse.getGroup();
    Assertions.assertEquals(groupEntity.roles(), group.roles());
    Assertions.assertEquals(groupEntity.name(), group.name());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchRoleException
    doThrow(new NoSuchRoleException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchRoleException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testRevokeRolesFromUser() {
    UserEntity userEntity =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withRoleNames(Lists.newArrayList())
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.revokeRolesFromUser(any(), any(), any())).thenReturn(userEntity);
    RoleRevokeRequest request = new RoleRevokeRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/users/user1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    UserResponse revokeResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, revokeResponse.getCode());

    User user = revokeResponse.getUser();
    Assertions.assertEquals(userEntity.roles(), user.roles());
    Assertions.assertEquals(userEntity.name(), user.name());

    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .revokeRolesFromUser(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/users/user1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testRevokeRolesFromGroup() {
    GroupEntity groupEntity =
        GroupEntity.builder()
            .withId(1L)
            .withName("group")
            .withRoleNames(Lists.newArrayList())
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.revokeRolesFromGroup(any(), any(), any())).thenReturn(groupEntity);
    RoleRevokeRequest request = new RoleRevokeRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/groups/group1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    GroupResponse revokeResponse = resp.readEntity(GroupResponse.class);
    Assertions.assertEquals(0, revokeResponse.getCode());

    Group group = revokeResponse.getGroup();
    Assertions.assertEquals(groupEntity.roles(), group.roles());
    Assertions.assertEquals(groupEntity.name(), group.name());

    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .revokeRolesFromGroup(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/groups/group1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testListGroupsByRole() {
    GroupEntity group1 =
        GroupEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("group1")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
            .build();
    GroupEntity group2 =
        GroupEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("group2")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
            .build();
    when(manager.listGroupsByRole(any(), any()))
        .thenReturn(Lists.newArrayList(group1, group2).toArray(new GroupEntity[0]));

    Response resp =
        target("/metalakes/metalake1/permissions/roles/role1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    GroupListResponse groupListResponse = resp.readEntity(GroupListResponse.class);
    Assertions.assertEquals(0, groupListResponse.getCode());

    GroupDTO[] groups = groupListResponse.getGroups();
    Assertions.assertEquals(2, groups.length);
    Assertions.assertEquals(
        Sets.newHashSet(group1.name(), group2.name()),
        Arrays.stream(groups).map(GroupDTO::name).collect(Collectors.toSet()));

    doThrow(new RuntimeException("mock error")).when(manager).listGroupsByRole(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/permissions/roles/role1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testListUsersByRole() {
    UserEntity user1 =
        UserEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("user1")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
            .build();
    UserEntity user2 =
        UserEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("user2")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
            .build();
    when(manager.listUsersByRole(any(), any()))
        .thenReturn(Lists.newArrayList(user1, user2).toArray(new UserEntity[0]));

    Response resp =
        target("/metalakes/metalake1/permissions/roles/role1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    UserListResponse userListResponse = resp.readEntity(UserListResponse.class);
    Assertions.assertEquals(0, userListResponse.getCode());

    UserDTO[] users = userListResponse.getUsers();
    Assertions.assertEquals(2, users.length);
    Assertions.assertEquals(
        Sets.newHashSet(user1.name(), user2.name()),
        Arrays.stream(users).map(UserDTO::name).collect(Collectors.toSet()));

    doThrow(new RuntimeException("mock error")).when(manager).listUsersByRole(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/permissions/roles/role1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }
}
