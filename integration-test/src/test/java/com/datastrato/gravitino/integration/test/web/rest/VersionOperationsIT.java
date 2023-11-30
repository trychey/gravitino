/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.rest;

import com.datastrato.gravitino.client.GravitinoVersion;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VersionOperationsIT extends AbstractIT {
  @Test
  public void testGetVersion() {
    GravitinoVersion gravitinoVersion = client.getVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
    Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());
    final String gitCommitId = readGitCommitIdFromGitFile();
    Assertions.assertEquals(gitCommitId, gravitinoVersion.gitCommit());
  }
}
