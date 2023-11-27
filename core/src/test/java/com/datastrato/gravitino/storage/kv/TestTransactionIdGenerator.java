/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTransactionIdGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestTransactionalKvBackend.class);

  private Config getConfig() {
    File file = Files.createTempDir();
    file.deleteOnExit();
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3000L);
    return config;
  }

  private KvBackend getKvBackEnd(Config config) throws IOException {
    KvBackend kvBackend = new RocksDBKvBackend();
    kvBackend.initialize(config);
    return kvBackend;
  }

  @Test
  void testSchedulerAndSkewTime() throws IOException, InterruptedException {
    Config config = getConfig();
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    transactionIdGenerator.start();
    // Make sure the scheduler has schedule once
    Thread.sleep(2000 + 500);
    Assertions.assertNotNull(kvBackend.get(TransactionIdGeneratorImpl.LAST_TIMESTAMP));
  }

  @ParameterizedTest
  @ValueSource(ints = {16})
  @Disabled("It's very time-consuming, so we disable it by default.")
  void testTransactionIdGeneratorQPS(int threadNum) throws IOException, InterruptedException {
    Config config = getConfig();
    String path = config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH);
    LOGGER.info("testTransactionIdGeneratorQPS path: {}", path);
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            threadNum,
            threadNum,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("testTransactionIdGenerator-%d")
                .build());

    AtomicLong atomicLong = new AtomicLong(0);
    for (int i = 0; i < threadNum; i++) {
      threadPoolExecutor.execute(
          () -> {
            long current = System.currentTimeMillis();
            while (System.currentTimeMillis() - current <= 2000) {
              transactionIdGenerator.nextId();
              atomicLong.getAndIncrement();
            }
          });
    }
    Thread.currentThread().sleep(100);
    threadPoolExecutor.shutdown();
    threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
    LOGGER.info(String.format("%d thread qps is: %d/s", threadNum, atomicLong.get() / 2));
  }
}
