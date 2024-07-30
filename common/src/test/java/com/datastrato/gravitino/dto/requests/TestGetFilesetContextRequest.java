/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGetFilesetContextRequest {
  @Test
  public void testGetFilesetContextRequest() throws JsonProcessingException {
    Map<String, String> extraInfo = Maps.newHashMap();
    extraInfo.put("key1", "value1");
    GetFilesetContextRequest request =
        GetFilesetContextRequest.builder()
            .subPath("/test/1.txt")
            .operation(FilesetDataOperation.CREATE)
            .clientType(ClientType.HADOOP_GVFS)
            .ip("127.0.0.1")
            .sourceEngineType(SourceEngineType.SPARK)
            .appId("application_1_1")
            .extraInfo(extraInfo)
            .build();
    String jsonString = JsonUtils.objectMapper().writeValueAsString(request);
    String expected =
        "{\"subPath\":\"/test/1.txt\",\"operation\":\"create\",\"clientType\":\"hadoop_gvfs\",\"ip\":\"127.0.0.1\",\"sourceEngineType\":\"spark\",\"appId\":\"application_1_1\",\"extraInfo\":{\"key1\":\"value1\"}}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));
  }
}
