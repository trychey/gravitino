/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Locale;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

class TestKerberosSecretPlugin {
  private static ClientAndServer MOCK_SERVER;

  @Mock private Config config;

  private KerberosHelper kerberosHelper;

  private KerberosSecretPlugin kerberosSecretPlugin;

  @BeforeAll
  public static void setUp() {
    MOCK_SERVER = startClientAndServer();
  }

  @BeforeEach
  public void setUpEach() throws NoSuchFieldException, IllegalAccessException {
    MockitoAnnotations.openMocks(this);

    when(config.get(KerberosSecretConfig.KERBEROS_HOST))
        .thenReturn(String.format("http://127.0.0.1:%d", MOCK_SERVER.getPort()));
    when(config.get(KerberosSecretConfig.KERBEROS_USER)).thenReturn("user");
    when(config.get(KerberosSecretConfig.KERBEROS_PASSWORD)).thenReturn("password");
    when(config.get(KerberosSecretConfig.KERBEROS_FORMAT))
        .thenReturn("s_workspace_%s_krb@XIAOMI.HADOOP");

    kerberosHelper = spy(new KerberosHelper(config));
    kerberosSecretPlugin = new KerberosSecretPlugin(null, null, config);

    Field kerberosHelperField = KerberosSecretPlugin.class.getDeclaredField("kerberosHelper");
    kerberosHelperField.setAccessible(true);
    kerberosHelperField.set(kerberosSecretPlugin, kerberosHelper);
  }

  @AfterEach
  public void tearDownEach() throws IOException {
    String ticketPath = System.getProperty("user.dir") + "/kerberos";
    if (Files.exists(Paths.get(ticketPath))) {
      FileUtils.deleteDirectory(new File(ticketPath));
    }
  }

  @AfterAll
  public static void tearDown() {
    MOCK_SERVER.stop();
  }

  @Test
  public void testGetSecretSuccess() throws Exception {
    LocalDateTime now = LocalDateTime.now();
    String kerberos = String.format(config.get(KerberosSecretConfig.KERBEROS_FORMAT), "10001");
    String ticketCachePath = String.format(KerberosSecretPlugin.TICKET_FORMAT, kerberos);
    String ticketCache = getTicketCacheString(ticketCachePath, kerberos, now.plusDays(1));

    mockSecretResponse(kerberos, 200, "keytab".getBytes(StandardCharsets.UTF_8));

    doAnswer(
            i -> {
              File ticketFile = new File(ticketCachePath);
              if (!ticketFile.exists()) {
                FileUtils.writeByteArrayToFile(
                    ticketFile, ticketCache.getBytes(StandardCharsets.UTF_8));
              }
              return null;
            })
        .when(kerberosHelper)
        .kinit(anyString(), anyString(), anyString());
    doReturn(ticketCache).when(kerberosHelper).klist(anyString());

    PrincipalUtils.doAs(
        new UserPrincipal("zhangsan:10001:admin"),
        () -> {
          Secret secret = kerberosSecretPlugin.getSecret("metalake");
          Assertions.assertEquals("s_workspace_10001_krb@XIAOMI.HADOOP", secret.name());
          Assertions.assertEquals(SecretsManager.KERBEROS_SECRET_TYPE, secret.type());
          Assertions.assertEquals(
              Base64.getEncoder().encodeToString(ticketCache.getBytes(StandardCharsets.UTF_8)),
              secret.value());
          Assertions.assertNotNull(
              now.plusDays(1).format(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")),
              secret.properties().get("expireTime"));

          Mockito.verify(kerberosHelper, Mockito.times(1))
              .kinit(anyString(), anyString(), anyString());
          Mockito.verify(kerberosHelper, Mockito.times(1)).klist(anyString());
          return null;
        });

    // get from cache, kinit, klist should not be called
    PrincipalUtils.doAs(
        new UserPrincipal("zhangsan:10001:admin"),
        () -> {
          Secret secret = kerberosSecretPlugin.getSecret("metalake");
          Assertions.assertEquals("s_workspace_10001_krb@XIAOMI.HADOOP", secret.name());
          Assertions.assertEquals(SecretsManager.KERBEROS_SECRET_TYPE, secret.type());
          Assertions.assertEquals(
              Base64.getEncoder().encodeToString(ticketCache.getBytes(StandardCharsets.UTF_8)),
              secret.value());
          Assertions.assertNotNull(
              now.plusDays(1).format(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")),
              secret.properties().get("expireTime"));

          Mockito.verify(kerberosHelper, Mockito.times(1))
              .kinit(anyString(), anyString(), anyString());
          Mockito.verify(kerberosHelper, Mockito.times(1)).klist(anyString());
          return null;
        });
  }

  @Test
  public void testGetSecretFail() throws Exception {
    LocalDateTime now = LocalDateTime.now();
    String kerberos = String.format(config.get(KerberosSecretConfig.KERBEROS_FORMAT), "10002");
    String ticketCachePath = String.format(KerberosSecretPlugin.TICKET_FORMAT, kerberos);
    String ticketCache = getTicketCacheString(ticketCachePath, kerberos, now.plusDays(1));

    mockSecretResponse(kerberos, 500, "null".getBytes(StandardCharsets.UTF_8));

    doAnswer(
            i -> {
              File ticketFile = new File(ticketCachePath);
              if (!ticketFile.exists()) {
                FileUtils.writeByteArrayToFile(
                    ticketFile, ticketCache.getBytes(StandardCharsets.UTF_8));
              }
              return null;
            })
        .when(kerberosHelper)
        .kinit(anyString(), anyString(), anyString());
    doReturn(ticketCache).when(kerberosHelper).klist(anyString());

    PrincipalUtils.doAs(
        new UserPrincipal("zhangsan:10002:admin"),
        () -> {
          RuntimeException exception =
              Assertions.assertThrows(
                  RuntimeException.class, () -> kerberosSecretPlugin.getSecret("metalake"));
          Assertions.assertTrue(exception.getMessage().contains("Failed to fetch keytab"));
          return null;
        });
  }

  private void mockSecretResponse(String kerberos, int statusCode, byte[] secretResponse) {
    HttpRequest mockRequest =
        request("/kerberos/keytab")
            .withMethod(Method.GET.name().toUpperCase(Locale.ROOT))
            .withQueryStringParameter("account", kerberos)
            .withQueryStringParameter("user", config.get(KerberosSecretConfig.KERBEROS_USER))
            .withQueryStringParameter(
                "auth_token", config.get(KerberosSecretConfig.KERBEROS_PASSWORD));
    HttpResponse mockResponse = response().withStatusCode(statusCode);
    mockResponse = mockResponse.withBody(secretResponse);
    MOCK_SERVER.when(mockRequest).respond(mockResponse);
  }

  private String getTicketCacheString(
      String ticketCachePath, String kerberos, LocalDateTime expiredTime) {
    File ticketFile = new File(ticketCachePath);
    if (ticketFile.exists()) {
      try {
        return FileUtils.readFileToString(ticketFile, StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException("Failed to read ticket cache from " + ticketCachePath, e);
      }
    }

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
    String expires = expiredTime.format(formatter);

    return "Ticket cache: FILE:"
        + ticketCachePath
        + "\n"
        + "Default principal: "
        + kerberos
        + "\n"
        + "\n"
        + "Valid starting       Expires              Service principal\n"
        + "08/28/2024 12:00:01  "
        + expires
        + "krbtgt/XIAOMI.HADOOP@XIAOMI.HADOOP\n"
        + "\trenew until 08/26/2034 12:00:01";
  }
}
