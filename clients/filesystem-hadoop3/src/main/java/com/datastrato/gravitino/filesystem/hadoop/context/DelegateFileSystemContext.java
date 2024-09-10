/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop.context;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.secret.Secret;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class DelegateFileSystemContext implements FileSystemContext {
  public static final String DEFAULT_CRED_PATH = "/tmp/gravitino/cred";

  static {
    // Check default local credential path
    File tmpDir = new File(DEFAULT_CRED_PATH);
    try {
      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      }
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          "Failed to create temp dir: %s, msg: %s", DEFAULT_CRED_PATH, e.getCause());
    }
  }

  private final Secret secret;
  private final String localCredentialPath;
  private final FileSystem fileSystem;

  public DelegateFileSystemContext(Secret secret, URI uri, Configuration configuration)
      throws IOException {
    if (!secret.type().equals("kerberos")) {
      throw new UnsupportedOperationException("Unsupported secret type:" + secret.type());
    }

    this.secret = secret;

    // Check if local credential file exists
    this.localCredentialPath =
        DEFAULT_CRED_PATH
            + "/"
            + secret.name()
            + "_"
            + UUID.randomUUID().toString().replace("-", "");

    deleteTempCredFile();

    String encryptedValueString = secret.value();
    byte[] decryptedValue = Base64.getDecoder().decode(encryptedValueString);
    // Save the ticket
    try (FileOutputStream out = new FileOutputStream(localCredentialPath)) {
      out.write(decryptedValue);
    } catch (IOException ioe) {
      throw new GravitinoRuntimeException(
          "Failed to create local credential file: %s, msg: %s",
          localCredentialPath, ioe.getCause());
    }

    // Init the fs with the local credential file
    Configuration conf = new Configuration(configuration);
    conf.set("hadoop.security.kerberos.ticket.cache.path", localCredentialPath);
    try {
      this.fileSystem = FileSystem.newInstance(uri, conf, secret.name());
    } catch (IOException | InterruptedException e) {
      throw new GravitinoRuntimeException(
          "Failed to init filesystem for secret: %s, msg: %s", secret.name(), e.getCause());
    }

    deleteTempCredFile();
  }

  private void deleteTempCredFile() {
    File tmpFile = new File(localCredentialPath);
    try {
      if (tmpFile.exists()) {
        tmpFile.delete();
      }
    } catch (Exception e) {
      // Ignore
    }
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public Secret getSecret() {
    return secret;
  }

  @VisibleForTesting
  String localCredentialPath() {
    return localCredentialPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DelegateFileSystemContext)) return false;
    DelegateFileSystemContext that = (DelegateFileSystemContext) o;
    return Objects.equals(secret, that.secret)
        && Objects.equals(localCredentialPath, that.localCredentialPath)
        && Objects.equals(fileSystem, that.fileSystem);
  }

  @Override
  public int hashCode() {
    return Objects.hash(secret, localCredentialPath, fileSystem);
  }

  @Override
  public void close() {
    deleteTempCredFile();

    try {
      fileSystem.close();
    } catch (Exception e) {
      // Ignore
    }
  }
}
