/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.secret.Secret;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDelegateFileSystemContext {

  @Test
  public void testInitializeAndClose() throws IOException {
    Secret mockSecret = Mockito.mock(Secret.class);
    Mockito.when(mockSecret.name()).thenReturn("testSecret");
    Mockito.when(mockSecret.value())
        .thenReturn(
            Base64.getEncoder().encodeToString("testValue".getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockSecret.type()).thenReturn("kerberos");
    URI uri = new Path("/tmp").toUri();
    DelegateFileSystemContext context =
        new DelegateFileSystemContext(mockSecret, uri, new Configuration());

    // test tmp cred file
    File tmpCredentialFile = new File(context.localCredentialPath());
    assertFalse(tmpCredentialFile.exists());

    // test secret
    Secret secret = context.getSecret();
    assertEquals("testSecret", secret.name());
    assertEquals(
        "testValue",
        new String(Base64.getDecoder().decode(secret.value()), StandardCharsets.UTF_8));
    assertEquals("kerberos", secret.type());

    // test close
    context.close();
    assertFalse(tmpCredentialFile.exists());
    assertThrows(
        IOException.class,
        () -> context.getFileSystem().getFileStatus(new Path(context.localCredentialPath())));
  }
}
