/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.HDFS_SCHEME;
import static com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.JUICEFS_SCHEME;
import static com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.LAVAFS_SCHEME;
import static com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.LOCAL_SCHEME;

import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.TokenAuthProvider;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.filesystem.hadoop.context.DelegateFileSystemContext;
import com.datastrato.gravitino.filesystem.hadoop.context.FileSystemContext;
import com.datastrato.gravitino.filesystem.hadoop.context.SimpleFileSystemContext;
import com.datastrato.gravitino.secret.Secret;
import com.datastrato.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.datastrato.gravitino.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.StringUtils;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.math.NumberUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalFileSystemManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(InternalFileSystemManager.class);
  private static final String SECRET_EXPIRE_TIME_PROP = "expireTime";
  private static final String KERBEROS_SECRET_TYPE = "kerberos";
  private static final String JUICEFS_MASTER_SERVER_KEY = "juicefs.master";
  private final String authType;
  private final DelayQueue<FileSystemContextCloseTask> delayCloseQueue = new DelayQueue<>();

  private GravitinoClient gravitinoClient;
  private Cache<String, FileSystemContext> internalContextCache;
  private ScheduledThreadPoolExecutor contextRemoveScheduler;
  private Thread contextCloser;

  public InternalFileSystemManager(Configuration configuration) {
    String authTypeEnv = System.getenv("GRAVITINO_CLIENT_AUTH_TYPE");
    if (StringUtils.isNotBlank(authTypeEnv)) {
      this.authType = authTypeEnv;
    } else {
      this.authType =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
              GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
    }
    if (authType.equals(GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE)) {
      initializeGravitinoClient(configuration);
    }
    initializeContextCache(configuration);
    initContextCloser();
  }

  private void initializeGravitinoClient(Configuration configuration) {
    String serverUriEnv = System.getenv("GRAVITINO_SERVER");
    String serverUri;
    if (StringUtils.isNotBlank(serverUriEnv)) {
      serverUri = serverUriEnv;
    } else {
      serverUri =
          configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    }
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);

    String metalakeNameEnv = System.getenv("GRAVITINO_METALAKE");
    String metalakeName;
    if (StringUtils.isNotBlank(metalakeNameEnv)) {
      metalakeName = metalakeNameEnv;
    } else {
      metalakeName =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    }
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    String tokenEnv = System.getenv("GRAVITINO_CLIENT_AUTH_TOKEN");
    String token;
    if (StringUtils.isNotBlank(tokenEnv)) {
      token = tokenEnv;
    } else {
      token =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY);
    }
    Preconditions.checkArgument(
        StringUtils.isNotBlank(token),
        "%s should not be null.",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY);
    TokenAuthProvider tokenAuthProvider = new TokenAuthProvider(token);
    gravitinoClient =
        GravitinoClient.builder(serverUri)
            .withMetalake(metalakeName)
            .withTokenAuth(tokenAuthProvider)
            .build();
  }

  private void initializeContextCache(Configuration configuration) {
    int maxCapacity =
        configuration.getInt(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT);
    Preconditions.checkArgument(
        maxCapacity > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY);

    long expireAfterAccess =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT);
    Preconditions.checkArgument(
        expireAfterAccess != 0,
        "'%s' should not be 0",
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY);
    long fsDelayCloseMills =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_INTERNAL_FILESYSTEM_DELAY_CLOSE_MILLS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_INTERNAL_FILESYSTEM_DELAY_CLOSE_MILLS_DEFAULT);
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.contextRemoveScheduler =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("internal-context-remover-%d")
                .build());
    Caffeine<Object, Object> cacheBuilder =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            .scheduler(Scheduler.forScheduledExecutorService(contextRemoveScheduler))
            .removalListener(
                (key, value, cause) -> {
                  FileSystemContext context = (FileSystemContext) value;
                  if (context != null) {
                    // Delay to close the filesystem context after 60 minutes
                    delayCloseQueue.add(new FileSystemContextCloseTask(context, fsDelayCloseMills));
                  }
                });
    if (authType.equals(GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE)) {
      Expiry<String, FileSystemContext> expiryStrategy =
          new Expiry<String, FileSystemContext>() {
            @Override
            public long expireAfterCreate(
                @NonNull String key, @NonNull FileSystemContext value, long currentTime) {
              // We remove the context cache by the ticket remaining valid time
              try {
                String expiryTimeString =
                    value.getSecret().properties().get(SECRET_EXPIRE_TIME_PROP);
                if (!NumberUtils.isCreatable(expiryTimeString)) {
                  throw new GravitinoRuntimeException(
                      "Failed to get the expiry time: %s, key: %s, value: %s",
                      expiryTimeString, key, value.getSecret().name());
                }
                long expiryTime = Long.parseLong(expiryTimeString);
                if (expiryTime <= 0) {
                  return Long.MAX_VALUE;
                } else {
                  // We reserve 1 hour to avoid the expiry time is too short
                  long reservedTime = 1000 * 60 * 60;
                  long duration = expiryTime - reservedTime - System.currentTimeMillis();
                  if (duration > 0) {
                    return TimeUnit.MILLISECONDS.toNanos(duration);
                  } else {
                    // If the duration is less than 0, we set the expiry time to 0, which means it
                    // will be removed immediately
                    return 0;
                  }
                }
              } catch (Exception e) {
                throw new GravitinoRuntimeException(
                    "Exception occurs when setting the expiry time, key: %s, value: %s, msg: %s",
                    key, value.toString(), e.getCause());
              }
            }

            @Override
            public long expireAfterUpdate(
                @NonNull String key,
                @NonNull FileSystemContext value,
                long currentTime,
                @NonNegative long currentDuration) {
              return currentDuration;
            }

            @Override
            public long expireAfterRead(
                @NonNull String key,
                @NonNull FileSystemContext value,
                long currentTime,
                @NonNegative long currentDuration) {
              return currentDuration;
            }
          };
      cacheBuilder.expireAfter(expiryStrategy);
    }
    if (expireAfterAccess > 0
        && !authType.equals(GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE)) {
      cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
    }

    this.internalContextCache = cacheBuilder.build();
  }

  private void initContextCloser() {
    this.contextCloser =
        new Thread(
            () -> {
              while (true) {
                try {
                  FileSystemContextCloseTask task = delayCloseQueue.take();
                  task.close();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            });
    contextCloser.setDaemon(true);
    contextCloser.setName("context-closer-" + contextCloser.getId());
    contextCloser.setUncaughtExceptionHandler(
        (t, throwable) -> LOG.error("{} uncaught exception: ", t.getName(), throwable));
    contextCloser.start();
  }

  @VisibleForTesting
  Cache<String, FileSystemContext> getInternalContextCache() {
    return internalContextCache;
  }

  public FileSystem getFileSystem(URI uri, String subPath, Configuration configuration) {
    String scheme = uri.getScheme();
    if (StringUtils.isBlank(scheme)) {
      URI defaultUri = FileSystem.getDefaultUri(configuration);
      scheme = defaultUri.getScheme();
    }
    String path = uri.toString();
    FileSystemContext context;
    String cacheKey;
    String pathPrefix = path.substring(0, path.length() - subPath.length());
    switch (scheme) {
      case HDFS_SCHEME:
      case LAVAFS_SCHEME:
      case LOCAL_SCHEME:
        // this cache key is storage location like
        // `hdfs://zjyprc-hadoop/user/h_data_platform/fileset/fileset_zjyprc_hadoop/tmp/test_fileset`
        // or `hdfs://zjyprc-hadoop/user/ad/my_test`
        context =
            internalContextCache.get(
                pathPrefix, str -> createInternalFileSystem(uri, configuration));
        break;
      case JUICEFS_SCHEME:
        // juicefs's path is not contain the cluster info, so we need to get the master key from
        // configuration
        String masterKey = configuration.get(JUICEFS_MASTER_SERVER_KEY);
        if (StringUtils.isBlank(masterKey)) {
          throw new GravitinoRuntimeException("juicefs master key is not set for uri: %s", uri);
        }
        // this cache key is storage location like
        // `jfs://test-volume/fileset/fileset_zjyprc_hadoop/tmp/test_fileset#ak-common`
        // or `jfs://test-volume#ak-common`
        cacheKey = String.format("%s#%s", pathPrefix, masterKey);
        context =
            internalContextCache.get(cacheKey, str -> createInternalFileSystem(uri, configuration));
        break;
      default:
        throw new GravitinoRuntimeException("Unsupported scheme: %s for uri: %s", scheme, uri);
    }
    if (context == null) {
      throw new GravitinoRuntimeException("FileSystem context not found for uri: %s", uri);
    }
    return context.getFileSystem();
  }

  private FileSystemContext createInternalFileSystem(URI uri, Configuration configuration) {
    if (!authType.equals(GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE)) {
      try {
        return new SimpleFileSystemContext(uri, configuration);
      } catch (IOException ioe) {
        throw new GravitinoRuntimeException(
            "Exception occurs when create new FileSystem for actual uri: %s, msg: %s", uri, ioe);
      }
    }
    // TODO need support juicefs when we can get the ak/sk
    switch (uri.getScheme()) {
      case HDFS_SCHEME:
      case LAVAFS_SCHEME:
        // Just for tests
      case LOCAL_SCHEME:
        Secret secret = gravitinoClient.getSecret(KERBEROS_SECRET_TYPE);
        try {
          return new DelegateFileSystemContext(secret, uri, configuration);
        } catch (IOException ioe) {
          throw new GravitinoRuntimeException(
              "Exception occurs when create new FileSystem for actual uri: %s, msg: %s", uri, ioe);
        }
      default:
        throw new GravitinoRuntimeException("Unsupported storage type: %s", uri.getScheme());
    }
  }

  @Override
  public void close() {
    // Close the client
    try {
      if (gravitinoClient != null) {
        gravitinoClient.close();
      }
    } catch (Exception e) {
      // Ignore
    }

    // Close all actual FileSystems
    for (FileSystemContext fileSystemContext : internalContextCache.asMap().values()) {
      fileSystemContext.close();
    }
    internalContextCache.invalidateAll();

    contextRemoveScheduler.shutdownNow();
    contextCloser.interrupt();
  }
}
