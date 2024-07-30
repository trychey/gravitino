/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetContext;

/**
 * An abstract class representing a base fileset context for {@link FilesetCatalog}. Developers
 * should extend this class to implement a custom fileset context for their fileset catalog.
 */
@Evolving
public abstract class BaseFilesetContext implements FilesetContext {
  protected Fileset fileset;
  protected String[] actualPaths;

  @Override
  public Fileset fileset() {
    return fileset;
  }

  @Override
  public String[] actualPaths() {
    return actualPaths;
  }

  interface Builder<
      SELF extends BaseFilesetContext.Builder<SELF, T>, T extends BaseFilesetContext> {

    SELF withFileset(Fileset fileset);

    SELF withActualPaths(String[] actualPaths);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseFilesetContext}. This class
   * should be extended by the concrete fileset context builders.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the fileset being built.
   */
  public abstract static class BaseFilesetContextBuilder<
          SELF extends BaseFilesetContext.Builder<SELF, T>, T extends BaseFilesetContext>
      implements BaseFilesetContext.Builder<SELF, T> {
    protected Fileset fileset;
    protected String[] actualPaths;

    /**
     * Sets the fileset of the fileset context.
     *
     * @param fileset The fileset object.
     * @return The builder instance.
     */
    @Override
    public SELF withFileset(Fileset fileset) {
      this.fileset = fileset;
      return self();
    }

    /**
     * Sets the actual paths of the fileset context.
     *
     * @param actualPaths The actual paths for the fileset context.
     * @return The builder instance.
     */
    @Override
    public SELF withActualPaths(String[] actualPaths) {
      this.actualPaths = actualPaths;
      return self();
    }

    /**
     * Builds the instance of the fileset with the provided attributes.
     *
     * @return The built fileset instance.
     */
    @Override
    public T build() {
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    /**
     * Builds the concrete instance of the fileset with the provided attributes.
     *
     * @return The built fileset instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
