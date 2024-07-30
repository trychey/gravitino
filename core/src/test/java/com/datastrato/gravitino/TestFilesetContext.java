/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.BaseFilesetContext;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestFilesetContext extends BaseFilesetContext {
  public static class Builder
      extends BaseFilesetContext.BaseFilesetContextBuilder<
          TestFilesetContext.Builder, TestFilesetContext> {
    /** Creates a new instance of {@link TestFilesetContext.Builder}. */
    private Builder() {}

    @Override
    protected TestFilesetContext internalBuild() {
      TestFilesetContext context = new TestFilesetContext();
      context.fileset = fileset;
      context.actualPaths = actualPaths;
      return context;
    }
  }

  /**
   * Creates a new instance of {@link TestFilesetContext.Builder}.
   *
   * @return The new instance.
   */
  public static TestFilesetContext.Builder builder() {
    return new TestFilesetContext.Builder();
  }
}
