/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** Request to get a fileset context. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Builder
@ToString
public class GetFilesetContextRequest implements RESTRequest {
  @JsonProperty("subPath")
  private String subPath;

  @JsonProperty("operation")
  private FilesetDataOperation operation;

  @JsonProperty("clientType")
  private ClientType clientType;

  @JsonProperty("ip")
  private String ip;

  @JsonProperty("sourceEngineType")
  private SourceEngineType sourceEngineType;

  @JsonProperty("appId")
  private String appId;

  @JsonProperty("extraInfo")
  private Map<String, String> extraInfo;

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(subPath != null, "\"subPath\" field cannot be null");
    Preconditions.checkArgument(
        operation != null, "\"operation\" field is required and cannot be null");
    Preconditions.checkArgument(
        clientType != null, "\"clientType\" field is required and cannot be null");
    Preconditions.checkArgument(
        sourceEngineType != null, "\"sourceEngineType\" field is required and cannot be null");
  }
}
