/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.rest;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

/** Utility class for working with REST related operations. */
public class RESTUtils {

  private static final Joiner.MapJoiner FORM_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Splitter.MapSplitter FORM_SPLITTER =
      Splitter.on("&").withKeyValueSeparator("=");

  private RESTUtils() {}

  /**
   * Remove trailing slashes from a path.
   *
   * @param path The path to strip trailing slashes from.
   * @return The path with trailing slashes removed.
   */
  public static String stripTrailingSlash(String path) {
    if (path == null) {
      return null;
    }

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * Encode a map of form data into an URL encoded string.
   *
   * @param formData The form data to encode.
   * @return The URL encoded form data string.
   */
  public static String encodeFormData(Map<?, ?> formData) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    formData.forEach(
        (key, value) ->
            builder.put(encodeString(String.valueOf(key)), encodeString(String.valueOf(value))));
    return FORM_JOINER.join(builder.build());
  }

  /**
   * Decode an URL encoded form data string into a map.
   *
   * @param formString The URL encoded form data string.
   * @return The decoded form data map.
   */
  public static Map<String, String> decodeFormData(String formString) {
    return FORM_SPLITTER.split(formString).entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                e -> decodeString(e.getKey()), e -> decodeString(e.getValue())));
  }

  /**
   * URL encode a string.
   *
   * @param toEncode The string to encode.
   * @return The URL encoded string.
   * @throws IllegalArgumentException If the input string is null.
   * @throws UncheckedIOException If URL encoding fails.
   */
  public static String encodeString(String toEncode) {
    Preconditions.checkArgument(toEncode != null, "Invalid string to encode: null");
    try {
      return URLEncoder.encode(toEncode, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new UncheckedIOException(
          String.format("Failed to URL encode '%s': UTF-8 encoding is not supported", toEncode), e);
    }
  }

  /**
   * Decode an URL encoded string.
   *
   * @param encoded The URL encoded string to decode.
   * @return The decoded string.
   * @throws IllegalArgumentException if the input string is null.
   * @throws UncheckedIOException if URL decoding fails.
   */
  public static String decodeString(String encoded) {
    Preconditions.checkArgument(encoded != null, "Invalid string to decode: null");
    try {
      return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new UncheckedIOException(
          String.format("Failed to URL decode '%s': UTF-8 encoding is not supported", encoded), e);
    }
  }

  /**
   * Find an available port in the port range.
   *
   * @param portRangeStart the start of the port range
   * @param portRangeEnd the end of the port range
   * @return the available port
   * @throws IOException if no available port in the port range
   */
  public static int findAvailablePort(final int portRangeStart, final int portRangeEnd)
      throws IOException {
    // valid user registered port https://en.wikipedia.org/wiki/Registered_port
    if (portRangeStart > portRangeEnd) {
      throw new IOException("Invalidate port range: " + portRangeStart + ":" + portRangeEnd);
    }

    int portStart = portRangeStart;
    int portEnd = portRangeEnd;
    if (portStart == 0 && portEnd == 0) {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      } catch (IOException e) {
        throw new IOException("Failed to allocate a random port", e);
      }
    }

    if (portStart < 1024) {
      portStart = 1024;
    }
    if (portStart > 65535) {
      portStart = 65535;
    }
    if (portEnd < 1024) {
      portEnd = 1024;
    }
    if (portEnd > 65535) {
      portEnd = 65535;
    }
    Random random = new Random();
    for (int i = portStart; i <= portEnd; ++i) {
      int randomNumber = random.nextInt(portEnd - portStart + 1) + portStart;
      try (ServerSocket socket = new ServerSocket(randomNumber)) {
        return socket.getLocalPort();
      } catch (IOException e) {
        // ignore this
      }
    }
    throw new IOException("No available port in the range: " + portRangeStart + ":" + portRangeEnd);
  }
}
