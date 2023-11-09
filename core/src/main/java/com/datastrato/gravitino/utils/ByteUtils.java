/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Utility class containing methods to convert between primitive types and byte arrays. */
public class ByteUtils {

  /**
   * Converts an integer value to a byte array representation.
   *
   * @param v The integer value to convert.
   * @return A byte array representation of the integer value.
   */
  public static byte[] intToByte(int v) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(v);
    return buffer.array();
  }

  /**
   * Converts a long value to a byte array representation.
   *
   * @param v The long value to convert.
   * @return A byte array representation of the long value.
   */
  public static byte[] longToByte(long v) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(v);
    return buffer.array();
  }

  public static byte[] longToByteInSmallEndian(long v) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(v);
    return buffer.array();
  }

  /**
   * Converts a byte array to an integer value.
   *
   * @param bytes The byte array to convert.
   * @return The integer value obtained from the byte array.
   */
  public static int byteToInt(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getInt();
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes The byte array to convert.
   * @return The long value obtained from the byte array.
   */
  public static long byteToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getLong();
  }

  /**
   * Format a byte array to a human-readable string. For example, if the byte array is [0x00, 0x01,
   * 0x02, 0x03], the result is '0x00010203'
   *
   * @param bytes Bytes to be formatted
   * @return A human-readable string
   */
  public static String formatByteArray(byte[] bytes) {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  //  public static byte[] copyOf(byte[] original, int from, int length) {
  //    byte[] copy = new byte[length];
  //    if (from >= original.length) {
  //      return new byte[0];
  //    }
  //
  //    System.arraycopy(original, from, copy, 0, Math.min(original.length - from, length));
  //    return copy;
  //  }
  //
  //  public static byte[] copyOf(byte[] original, int from) {
  //    return copyOf(original, from, original.length - from);
  //  }
}
