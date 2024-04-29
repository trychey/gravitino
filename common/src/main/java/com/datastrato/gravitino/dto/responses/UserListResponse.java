/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of user with their information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class UserListResponse extends BaseResponse {

  @JsonProperty("users")
  private final UserDTO[] users;

  /**
   * Creates a new UserListResponse.
   *
   * @param users The list of users.
   */
  public UserListResponse(UserDTO[] users) {
    super(0);
    this.users = users;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * UserListResponse.
   */
  public UserListResponse() {
    super();
    this.users = null;
  }
}
