/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import static com.datastrato.gravitino.secret.SecretsManager.KERBEROS_SECRET_TYPE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.meta.AuditInfo;
import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TestSecretsManager {
  @Mock private KerberosSecretPlugin kerberosSecretPlugin;
  @InjectMocks private SecretsManager secretsManager;

  @BeforeEach
  public void setUpEach() throws NoSuchFieldException, IllegalAccessException {
    MockitoAnnotations.openMocks(this);

    Field kerberosField = SecretsManager.class.getDeclaredField("kerberosSecretPlugin");
    kerberosField.setAccessible(true);
    kerberosField.set(secretsManager, kerberosSecretPlugin);
  }

  @Test
  public void testGetSecret() {
    Secret secretExpected = buildKerberosSecret("ticketCache");
    when(kerberosSecretPlugin.getSecret(anyString())).thenReturn(secretExpected);

    Secret secret = secretsManager.getSecret("metalake", SecretsManager.KERBEROS_SECRET_TYPE);
    Assertions.assertEquals("kerberos_ticket", secret.name());
    Assertions.assertEquals(SecretsManager.KERBEROS_SECRET_TYPE, secret.type());
    Assertions.assertEquals("ticketCache", secret.value());
    Assertions.assertNotNull(secret.properties().get("expireTime"));
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
