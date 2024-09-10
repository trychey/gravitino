/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.filesystem.hadoop.context.FileSystemContext;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestFileSystemContextCloseTask {
  @Test
  public void testTakeTask() {
    DelayQueue<FileSystemContextCloseTask> delayQueue = new DelayQueue<>();
    FileSystemContext context = Mockito.mock(FileSystemContext.class);
    FileSystemContextCloseTask task = new FileSystemContextCloseTask(context, 2000);
    delayQueue.add(task);
    Thread takeThread =
        new Thread(
            () -> {
              while (!delayQueue.isEmpty()) {
                try {
                  delayQueue.take();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });
    takeThread.start();

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(0, delayQueue.size()));

    takeThread.interrupt();
  }
}
