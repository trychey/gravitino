/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.file;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetContextDTO {
  @Test
  void testJsonSerDe() throws JsonProcessingException {
    FilesetDTO filesetDTO =
        FilesetDTO.builder()
            .name("test")
            .type(Fileset.Type.MANAGED)
            .audit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .storageLocation("hdfs://host/test")
            .build();

    FilesetContextDTO dto =
        FilesetContextDTO.builder()
            .fileset(filesetDTO)
            .actualPaths(new String[] {"hdfs://host/test/1.txt"})
            .build();
    String value = JsonUtils.objectMapper().writeValueAsString(dto);

    String expectedValue =
        String.format(
            "{\n"
                + "     \"fileset\": {\n"
                + "          \"name\": \"test\",\n"
                + "          \"comment\": null,\n"
                + "          \"type\": \"managed\",\n"
                + "          \"storageLocation\": \"hdfs://host/test\",\n"
                + "          \"properties\": null,\n"
                + "          \"audit\": {\n"
                + "               \"creator\": \"creator\",\n"
                + "               \"createTime\": \"%s\",\n"
                + "               \"lastModifier\": null,\n"
                + "               \"lastModifiedTime\": null\n"
                + "          }\n"
                + "     },\n"
                + "     \"actualPaths\": [\n"
                + "          \"hdfs://host/test/1.txt\"\n"
                + "     ]\n"
                + "}",
            filesetDTO.auditInfo().createTime());
    JsonNode expected = JsonUtils.objectMapper().readTree(expectedValue);
    JsonNode actual = JsonUtils.objectMapper().readTree(value);
    Assertions.assertEquals(expected, actual);
  }
}
