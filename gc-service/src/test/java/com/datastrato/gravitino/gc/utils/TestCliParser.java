/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.gc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.gc.constants.CliConstants;
import org.junit.jupiter.api.Test;

public class TestCliParser {
  @Test
  public void testInvalidConfig() {
    String[] args1 = new String[] {"1", "2"};
    assertThrows(NullPointerException.class, () -> new CliParser(args1));

    String[] args2 =
        new String[] {"--" + CliConstants.GRAVITINO_SERVER_URI, "http://localhost:8090"};
    assertThrows(NullPointerException.class, () -> new CliParser(args2));

    String[] args3 =
        new String[] {
          "--" + CliConstants.GRAVITINO_SERVER_URI,
          "http://localhost:8090",
          "--" + CliConstants.GRAVITINO_METALAKE,
          "test_metalake"
        };
    assertThrows(NullPointerException.class, () -> new CliParser(args3));

    String[] args4 =
        new String[] {
          "--" + CliConstants.GRAVITINO_SERVER_URI,
          "http://localhost:8090",
          "--" + CliConstants.GRAVITINO_METALAKE,
          "test_metalake",
          "--" + CliConstants.FILESET_CATALOG,
          "test_catalog"
        };
    assertThrows(NullPointerException.class, () -> new CliParser(args4));
  }

  @Test
  public void testValidConfig() {
    String[] args5 =
        new String[] {
          "--" + CliConstants.GRAVITINO_SERVER_URI,
          "http://localhost:8090",
          "--" + CliConstants.GRAVITINO_METALAKE,
          "test_metalake",
          "--" + CliConstants.FILESET_CATALOG,
          "test_catalog",
          "--" + CliConstants.DATE,
          "20240408",
          "--" + CliConstants.PRINCIPAL_FILE_PATH,
          "file:/xxx/xxx/xxx"
        };
    CliParser cliParser = new CliParser(args5);
    assertEquals("http://localhost:8090", cliParser.getGravitinoServerUri());
    assertEquals("test_metalake", cliParser.getGravitinoMetalake());
    assertEquals("test_catalog", cliParser.getFilesetCatalog());
    assertEquals("20240408", cliParser.getDate());
    assertEquals("file:/xxx/xxx/xxx", cliParser.getPrincipalFilePath());
  }
}
