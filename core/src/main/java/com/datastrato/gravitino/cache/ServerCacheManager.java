/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.cache.processor.CacheAsyncProcessor;
import com.datastrato.gravitino.cache.processor.RocketMQCacheAsyncProcessor;
import com.google.common.base.Preconditions;
import java.io.IOException;

public class ServerCacheManager implements AutoCloseable {
  private static volatile CacheService CACHE_SERVICE;
  private static volatile CacheAsyncProcessor ASYNC_CACHE_PROCESSOR;
  private static final ServerCacheManager INSTANCE = new ServerCacheManager();

  public static ServerCacheManager getInstance() {
    return INSTANCE;
  }

  private ServerCacheManager() {}

  public void initialize(Config config) {
    if (CACHE_SERVICE == null) {
      synchronized (ServerCacheManager.class) {
        if (CACHE_SERVICE == null) {
          String cacheImplName = config.get(Configs.SERVER_CACHE_IMPL);
          Preconditions.checkNotNull(cacheImplName, "Cache implementation not specified");
          switch (cacheImplName) {
            case Configs.REDIS_CACHE_KEY:
              CACHE_SERVICE = new RedisCacheService(config);
              break;
            default:
              throw new IllegalArgumentException("Invalid cache implementation");
          }
        }
      }
    }

    if (ASYNC_CACHE_PROCESSOR == null) {
      synchronized (ServerCacheManager.class) {
        if (ASYNC_CACHE_PROCESSOR == null) {
          // Only supports the MQ async cache processor for now.
          String asyncCacheProcessorImpl = config.get(Configs.CACHE_ASYNC_PROCESSOR_IMPL);
          Preconditions.checkNotNull(
              asyncCacheProcessorImpl, "Async cache processor not specified");
          switch (asyncCacheProcessorImpl) {
            case Configs.ROCKETMQ_KEY:
              ASYNC_CACHE_PROCESSOR = new RocketMQCacheAsyncProcessor(config, CACHE_SERVICE);
              break;
            default:
              throw new IllegalArgumentException("Invalid async cache processor implementation");
          }
        }
      }
    }
  }

  public CacheService cacheService() {
    Preconditions.checkState(CACHE_SERVICE != null, "Cache service is not initialized.");
    return CACHE_SERVICE;
  }

  public CacheAsyncProcessor asyncCacheProcessor() {
    Preconditions.checkState(
        ASYNC_CACHE_PROCESSOR != null, "Async cache processor is not initialized.");
    return ASYNC_CACHE_PROCESSOR;
  }

  @Override
  public synchronized void close() throws IOException {
    if (ASYNC_CACHE_PROCESSOR != null) {
      try {
        ASYNC_CACHE_PROCESSOR.close();
      } catch (Exception e) {
        // Ignore.
      }
      ASYNC_CACHE_PROCESSOR = null;
    }

    if (CACHE_SERVICE != null) {
      try {
        CACHE_SERVICE.close();
      } catch (Exception e) {
        // Ignore.
      }
      CACHE_SERVICE = null;
    }
  }
}
