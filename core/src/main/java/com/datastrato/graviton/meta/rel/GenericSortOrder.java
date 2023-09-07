/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.Transform;
import lombok.Builder;

@Builder
public class GenericSortOrder implements SortOrder {

  private final Transform transform;

  protected final Direction direction;

  protected final NullOrder nullOrder;

  @Override
  public Transform transform() {
    return transform;
  }

  @Override
  public Direction direction() {
    return direction;
  }

  @Override
  public NullOrder nullOrder() {
    return nullOrder;
  }
}
