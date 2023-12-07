/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.utils;

@FunctionalInterface
public interface Executable<R, E extends Exception> {

  R execute() throws E;
}
