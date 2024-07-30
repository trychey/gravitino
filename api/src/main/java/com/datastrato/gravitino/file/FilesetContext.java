/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * An interface representing a fileset context with an existing fileset {@link Fileset}. This
 * interface defines some contextual information related to Fileset that can be passed.
 *
 * <p>{@link FilesetContext} defines the basic properties of a fileset context object. A catalog
 * implementation with {@link FilesetCatalog} should implement this interface.
 */
@Evolving
public interface FilesetContext {
  /** @return The fileset object. */
  Fileset fileset();

  /** @return The actual storage paths after processing. */
  String[] actualPaths();
}
