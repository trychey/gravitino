/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static com.datastrato.gravitino.secret.SecretsManager.KERBEROS_SECRET_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.dto.responses.SecretResponse;
import com.datastrato.gravitino.dto.secret.SecretDTO;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.secret.KerberosSecret;
import com.datastrato.gravitino.secret.Secret;
import com.datastrato.gravitino.secret.SecretsManager;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
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

public class TestSecretOperations extends JerseyTest {

  private static final SecretsManager manager = mock(SecretsManager.class);

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
    GravitinoEnv.getInstance().setSecretsManager(manager);
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
    resourceConfig.register(SecretOperations.class);
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
  public void testGetSecret() {
    String ticketCache = "ticketCache";
    Secret secret = buildKerberosSecret(ticketCache);

    when(manager.getSecret(any(), any())).thenReturn(secret);

    Response resp =
        target("/metalakes/metalake1/secrets")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    SecretResponse secretResponse = resp.readEntity(SecretResponse.class);
    Assertions.assertEquals(0, secretResponse.getCode());
    SecretDTO secretDto = secretResponse.getSecret();
    Assertions.assertEquals(ticketCache, secretDto.value());
    Assertions.assertTrue(secretDto.properties().containsKey("expireTime"));
    Assertions.assertEquals(
        String.valueOf(
            LocalDateTime.of(2024, 8, 8, 8, 8, 8).toInstant(ZoneOffset.ofHours(8)).toEpochMilli()),
        secretDto.properties().get("expireTime"));
  }

  private Secret buildKerberosSecret(String ticketCache) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        "expireTime",
        String.valueOf(
            LocalDateTime.of(2024, 8, 8, 8, 8, 8).toInstant(ZoneOffset.ofHours(8)).toEpochMilli()));

    return KerberosSecret.builder()
        .withName("kerberos_ticket")
        .withValue(ticketCache)
        .withType(KERBEROS_SECRET_TYPE)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreateTime(Instant.now()).withCreator("creator").build())
        .build();
  }
}
