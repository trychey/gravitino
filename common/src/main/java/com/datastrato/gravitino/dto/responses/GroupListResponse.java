/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.authorization.GroupDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of group with their information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class GroupListResponse extends BaseResponse {

  @JsonProperty("groups")
  private final GroupDTO[] groups;

  /**
   * Creates a new GroupListResponse.
   *
   * @param groups The list of groups.
   */
  public GroupListResponse(GroupDTO[] groups) {
    super(0);
    this.groups = groups;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * GroupListResponse.
   */
  public GroupListResponse() {
    super();
    this.groups = null;
  }
}
