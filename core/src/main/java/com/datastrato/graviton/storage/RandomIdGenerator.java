/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import java.util.UUID;

public class RandomIdGenerator implements IdGenerator {

  private final UUID uuid = UUID.randomUUID();

  public static final RandomIdGenerator INSTACNE = new RandomIdGenerator();

  @Override
  public long nextId() {
    return uuid.getLeastSignificantBits();
  }
}
