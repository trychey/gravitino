/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

/** Configuration class for Gravitino Virtual File System. */
class GravitinoVirtualFileSystemConfiguration {
  public static final String GVFS_FILESET_PREFIX = "gvfs://fileset";
  public static final String GVFS_SCHEME = "gvfs";
  public static final String LOCAL_SCHEME = "file";
  public static final String HDFS_SCHEME = "hdfs";
  public static final String LAVAFS_SCHEME = "lavafs";
  public static final String JUICEFS_SCHEME = "jfs";

  /** The configuration key for the Gravitino server URI. */
  public static final String FS_GRAVITINO_SERVER_URI_KEY = "fs.gravitino.server.uri";

  /** The configuration key for the Gravitino client Metalake. */
  public static final String FS_GRAVITINO_CLIENT_METALAKE_KEY = "fs.gravitino.client.metalake";

  /** The configuration key for the Gravitino client auth type. */
  public static final String FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY = "fs.gravitino.client.authType";

  public static final String SIMPLE_AUTH_TYPE = "simple";
  public static final String OAUTH2_AUTH_TYPE = "oauth2";
  public static final String TOKEN_AUTH_TYPE = "token";

  // simple
  /** The configuration key for the superuser of the simple auth. */
  public static final String FS_GRAVITINO_CLIENT_SIMPLE_SUPER_USER_KEY =
      "fs.gravitino.client.simple.superUser";

  /** The configuration key for the proxy user of the simple auth. */
  public static final String FS_GRAVITINO_CLIENT_SIMPLE_PROXY_USER_KEY =
      "fs.gravitino.client.simple.proxyUser";

  // oauth2
  /** The configuration key for the URI of the default OAuth server. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY =
      "fs.gravitino.client.oauth2.serverUri";

  /** The configuration key for the client credential. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY =
      "fs.gravitino.client.oauth2.credential";

  /** The configuration key for the path which to get the token. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY =
      "fs.gravitino.client.oauth2.path";

  /** The configuration key for the scope of the token. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY =
      "fs.gravitino.client.oauth2.scope";

  public static final String FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY = "fs.gravitino.client.token.value";

  /** The configuration key for the maximum capacity of the Gravitino fileset cache. */
  public static final String FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY =
      "fs.gravitino.fileset.cache.maxCapacity";

  public static final int FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT = 1000;

  /**
   * The configuration key for the eviction time of the Gravitino fileset cache, measured in mills
   * after access.
   */
  public static final String FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY =
      "fs.gravitino.fileset.cache.evictionMillsAfterAccess";

  public static final long FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT = -1L;

  public static final String FS_GRAVITINO_TESTING = "fs.gravitino.testing";

  public static final String FS_GRAVITINO_TESTING_DEFAULT = "false";

  public static final String FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY =
      "fs.gravitino.fileset.write.primaryOnly";

  public static final boolean FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY_DEFAULT = false;

  public static final String FS_GRAVITINO_INTERNAL_FILESYSTEM_DELAY_CLOSE_MILLS_KEY =
      "fs.gravitino.internalFilesystem.delayCloseMills";

  public static final long FS_GRAVITINO_INTERNAL_FILESYSTEM_DELAY_CLOSE_MILLS_DEFAULT =
      1000L * 60 * 60;

  private GravitinoVirtualFileSystemConfiguration() {}
}
