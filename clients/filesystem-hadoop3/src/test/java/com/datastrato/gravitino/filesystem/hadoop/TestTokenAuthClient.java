/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.json.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

public class TestTokenAuthClient extends TestGvfsBase {
  private static final String TOKEN = "token/test";
  private static final String METALAKE = "tokenTest";

  @BeforeAll
  public static void setup() {
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE);
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY, TOKEN);
  }

  @Test
  public void testAuth() throws IOException {
    HttpRequest mockRequest =
        HttpRequest.request("/api/metalakes/" + METALAKE).withMethod(Method.GET.name());
    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(METALAKE)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);

    AtomicReference<String> actualTokenValue = new AtomicReference<>();
    GravitinoMockServerBase.mockServer()
        .when(mockRequest, Times.unlimited())
        .respond(
            httpRequest -> {
              List<Header> headers = httpRequest.getHeaders().getEntries();
              for (Header header : headers) {
                if (header.getName().equalsIgnoreCase("Authorization")) {
                  actualTokenValue.set(header.getValues().get(0).getValue());
                }
              }
              HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
              String respJson = JsonUtils.objectMapper().writeValueAsString(resp);
              mockResponse = mockResponse.withBody(respJson);
              return mockResponse;
            });

    Path newPath = new Path(managedFilesetPath.toString().replace(metalakeName, METALAKE));

    Configuration config = new Configuration(conf);
    config.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, METALAKE);
    newPath.getFileSystem(config);

    assertEquals(
        AuthConstants.AUTHORIZATION_TOKEN_HEADER
            + new String(
                Base64.getEncoder().encode(TOKEN.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8),
        actualTokenValue.get());
  }
}
