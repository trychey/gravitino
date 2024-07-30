/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.connector.BaseFilesetContext;

public class HadoopFilesetContext extends BaseFilesetContext {
  public static class Builder
      extends BaseFilesetContext.BaseFilesetContextBuilder<
          HadoopFilesetContext.Builder, HadoopFilesetContext> {
    /** Creates a new instance of {@link HadoopFilesetContext.Builder}. */
    private Builder() {}

    @Override
    protected HadoopFilesetContext internalBuild() {
      HadoopFilesetContext context = new HadoopFilesetContext();
      context.fileset = fileset;
      context.actualPaths = actualPaths;
      return context;
    }
  }

  /**
   * Creates a new instance of {@link HadoopFilesetContext.Builder}.
   *
   * @return The new instance.
   */
  public static HadoopFilesetContext.Builder builder() {
    return new HadoopFilesetContext.Builder();
  }
}
