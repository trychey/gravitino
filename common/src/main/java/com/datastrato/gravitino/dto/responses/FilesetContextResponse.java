/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.file.FilesetContextDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Response for fileset context. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class FilesetContextResponse extends BaseResponse {
  @JsonProperty("filesetContext")
  private final FilesetContextDTO context;

  /** Constructor for FilesetContextResponse. */
  public FilesetContextResponse() {
    super(0);
    this.context = null;
  }

  /**
   * Constructor for FilesetContextResponse.
   *
   * @param context the fileset context DTO.
   */
  public FilesetContextResponse(FilesetContextDTO context) {
    super(0);
    this.context = context;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(context != null, "context must not be null");
  }
}
