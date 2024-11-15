/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemCache implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemCache.class);
  private static final String HDFS_SCHEME = "hdfs";
  private static final String LAVAFS_SCHEME = "lavafs";
  private static final String LOCAL_SCHEME = "file";
  private static final String JUICEFS_SCHEME = "jfs";
  private static final String JUICEFS_MASTER_SERVER_KEY = "juicefs.master";

  private static final FileSystemCache INSTANCE = new FileSystemCache();
  private static final ScheduledThreadPoolExecutor filesystemRemover =
      new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("filesystem-remover-%d")
              .build());
  private static final Cache<String, FileSystem> cache =
      Caffeine.newBuilder()
          .maximumSize(10000)
          .expireAfterAccess(24, TimeUnit.HOURS)
          .scheduler(Scheduler.forScheduledExecutorService(filesystemRemover))
          .removalListener(
              (key, value, cause) -> {
                try {
                  FileSystem fs = (FileSystem) value;
                  if (fs != null) {
                    fs.close();
                  }
                } catch (IOException e) {
                  LOG.warn(
                      "Failed to close filesystem for key: {}, value: {}, msg: ", key, value, e);
                }
              })
          .build();

  private FileSystemCache() {}

  public static FileSystemCache getInstance() {
    return INSTANCE;
  }

  public FileSystem getFileSystem(
      Path path, Configuration configuration, Map<String, String> properties) throws IOException {
    Configuration newConf = new Configuration(configuration);
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    String concatKey;
    if (StringUtils.isBlank(scheme)) {
      FileSystem defaultFs = FileSystem.get(configuration);
      Path formalizedPath = path.makeQualified(defaultFs.getUri(), defaultFs.getWorkingDirectory());
      URI formalizedUri = formalizedPath.toUri();
      concatKey = String.format("%s#%s", formalizedUri.getScheme(), formalizedUri.getAuthority());
    } else {
      switch (scheme) {
        case HDFS_SCHEME:
        case LAVAFS_SCHEME:
        case LOCAL_SCHEME:
          concatKey = String.format("%s#%s", uri.getScheme(), uri.getAuthority());
          break;
        case JUICEFS_SCHEME:
          // The juicefs uri is like `jfs://<volume_name>/<sub_dir>` which doesn't contain the
          // master server address. We need to get the master server address from the configuration
          String masterServer =
              properties.get(
                  String.format("%s%s", CATALOG_BYPASS_PREFIX, JUICEFS_MASTER_SERVER_KEY));
          if (StringUtils.isBlank(masterServer)) {
            throw new IllegalArgumentException(
                "The master server address is not specified for juicefs path: " + uri);
          }
          newConf.set(JUICEFS_MASTER_SERVER_KEY, masterServer);
          concatKey = String.format("%s#%s#%s", uri.getScheme(), uri.getAuthority(), masterServer);
          break;
        default:
          throw new IllegalArgumentException("Unsupported storage scheme: " + scheme);
      }
    }
    return cache.get(
        concatKey,
        key -> {
          try {
            return FileSystem.newInstance(path.toUri(), newConf);
          } catch (IOException e) {
            throw new GravitinoRuntimeException(
                e, "Failed to create filesystem for path: %s, msg: ", path);
          }
        });
  }

  @Override
  public synchronized void close() throws Exception {
    for (Map.Entry<String, FileSystem> entry : cache.asMap().entrySet()) {
      try {
        FileSystem fs = entry.getValue();
        if (fs != null) {
          fs.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close filesystem for path: {}", entry.getKey(), e);
      }
    }
    // clear the cache after all file systems are
    cache.invalidateAll();
  }
}
