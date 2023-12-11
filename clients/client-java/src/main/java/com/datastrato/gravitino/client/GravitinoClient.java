/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SupportsMetalakes;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Client for interacting with the Gravitino API, allowing the client to list, load,
 * create, and alter Metalakes.
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public class GravitinoClient implements SupportsMetalakes, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoClient.class);

  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();

  private final RESTClient restClient;

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authenticator The type of authenticator.
   * @param authDataProvider The provider of the data which is used for authentication.
   */
  private GravitinoClient(String uri, AuthDataProvider authDataProvider) {
    this.restClient =
        HTTPClient.builder(Collections.emptyMap())
            .uri(uri)
            .withAuthDataProvider(authDataProvider)
            .build();
  }

  /**
   * Retrieves a list of Metalakes from the Gravitino API.
   *
   * @return An array of GravitinoMetaLake objects representing the Metalakes.
   */
  @Override
  public GravitinoMetaLake[] listMetalakes() {
    MetalakeListResponse resp =
        restClient.get(
            "api/metalakes",
            MetalakeListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return Arrays.stream(resp.getMetalakes())
        .map(o -> DTOConverters.toMetaLake(o, restClient))
        .toArray(GravitinoMetaLake[]::new);
  }

  /**
   * Loads a specific Metalake from the Gravitino API.
   *
   * @param ident The identifier of the Metalake to be loaded.
   * @return A GravitinoMetaLake instance representing the loaded Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   */
  @Override
  public GravitinoMetaLake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    NameIdentifier.checkMetalake(ident);

    MetalakeResponse resp =
        restClient.get(
            "api/metalakes/" + ident.name(),
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Creates a new Metalake using the Gravitino API.
   *
   * @param ident The identifier of the new Metalake.
   * @param comment The comment for the new Metalake.
   * @param properties The properties of the new Metalake.
   * @return A GravitinoMetaLake instance representing the newly created Metalake.
   * @throws MetalakeAlreadyExistsException If a Metalake with the specified identifier already
   *     exists.
   */
  @Override
  public GravitinoMetaLake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    NameIdentifier.checkMetalake(ident);

    MetalakeCreateRequest req = new MetalakeCreateRequest(ident.name(), comment, properties);
    req.validate();

    MetalakeResponse resp =
        restClient.post(
            "api/metalakes",
            req,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Alters a specific Metalake using the Gravitino API.
   *
   * @param ident The identifier of the Metalake to be altered.
   * @param changes The changes to be applied to the Metalake.
   * @return A GravitinoMetaLake instance representing the updated Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid or not applicable.
   */
  @Override
  public GravitinoMetaLake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    NameIdentifier.checkMetalake(ident);

    List<MetalakeUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toMetalakeUpdateRequest)
            .collect(Collectors.toList());
    MetalakeUpdatesRequest updatesRequest = new MetalakeUpdatesRequest(reqs);
    updatesRequest.validate();

    MetalakeResponse resp =
        restClient.put(
            "api/metalakes/" + ident.name(),
            updatesRequest,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Drops a specific Metalake using the Gravitino API.
   *
   * @param ident The identifier of the Metalake to be dropped.
   * @return True if the Metalake was successfully dropped, false otherwise.
   */
  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    NameIdentifier.checkMetalake(ident);

    try {
      DropResponse resp =
          restClient.delete(
              "api/metalakes/" + ident.name(),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.metalakeErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (Exception e) {
      LOG.warn("Failed to drop metadata {}", ident, e);
      return false;
    }
  }

  public GravitinoVersion getVersion() {
    VersionResponse resp =
        restClient.get(
            "api/version",
            VersionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.restErrorHandler());
    resp.validate();

    return new GravitinoVersion(resp.getVersion());
  }

  /** Closes the GravitinoClient and releases any underlying resources. */
  @Override
  public void close() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (Exception e) {
        // Swallow the exception
        LOG.warn("Failed to close the HTTP REST client", e);
      }
    }
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static Builder builder(String uri) {
    return new Builder(uri);
  }

  /** Builder class for constructing a GravitinoClient. */
  public static class Builder {

    private String uri;
    private AuthDataProvider authDataProvider;

    /**
     * Private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    private Builder(String uri) {
      this.uri = uri;
    }

    /**
     * Sets the simple mode authentication the Graivitno
     *
     * @return This Builder instance for method chaining.
     */
    public Builder withSimpleAuth() {
      this.authDataProvider = new SimpleTokenProvider();
      return this;
    }

    /**
     * Sets OAuth2TokenProvider for the GravitinoClient.
     *
     * @param dataProvider The OAuth2TokenProvider used as the provider of authentication data for
     *     GravitinoClient.
     * @return This Builder instance for method chaining.
     */
    public Builder withOAuth(OAuth2TokenProvider dataProvider) {
      this.authDataProvider = dataProvider;
      return this;
    }

    /**
     * Builds a new GravitinoClient instance.
     *
     * @return A new instance of GravitinoClient with the specified base URI.
     * @throws IllegalArgumentException If the base URI is null or empty.
     */
    public GravitinoClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");

      return new GravitinoClient(uri, authDataProvider);
    }
  }
}
