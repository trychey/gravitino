/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.filesystem.hadoop.context.FileSystemContext;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class FileSystemContextCloseTask implements Delayed {
  private final FileSystemContext context;
  private final long expireTime;

  public FileSystemContextCloseTask(FileSystemContext context, long delay) {
    this.context = context;
    this.expireTime = System.currentTimeMillis() + delay;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(expireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed object) {
    return Long.compare(this.expireTime, ((FileSystemContextCloseTask) object).expireTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileSystemContextCloseTask)) return false;
    FileSystemContextCloseTask that = (FileSystemContextCloseTask) o;
    return expireTime == that.expireTime && Objects.equals(context, that.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context, expireTime);
  }

  public void close() {
    this.context.close();
  }
}
