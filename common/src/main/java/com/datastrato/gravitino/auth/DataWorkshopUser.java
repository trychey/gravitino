/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.auth;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import java.util.List;
import org.apache.commons.lang3.math.NumberUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataWorkshopUser {
  private static final String SEP = ":";

  public static final Splitter COLON = Splitter.on(SEP).omitEmptyStrings().trimResults();

  @JsonProperty("username")
  private String username;

  @JsonProperty("workspaceId")
  private Long workspaceId;

  @JsonProperty("role")
  private String role;

  private DataWorkshopUser() {}

  public DataWorkshopUser(String username, Long workspaceId, String role) {
    this.username = username;
    this.workspaceId = workspaceId;
    this.role = role;
  }

  public String username() {
    return username;
  }

  public Long workspaceId() {
    return workspaceId;
  }

  public String role() {
    return role;
  }

  public static DataWorkshopUser fromUserPrincipal(String userPrincipal) {
    final List<String> parts = COLON.splitToList(userPrincipal);
    if (parts.size() != 3) {
      throw new IllegalArgumentException(
          "Invalid user principal: "
              + userPrincipal
              + ". Expected format: username:workspaceId:role");
    }
    if (!NumberUtils.isDigits(parts.get(1))) {
      throw new IllegalArgumentException(
          "Invalid token user, workspaceId: " + parts.get(1) + " expected to be a number");
    }
    return new DataWorkshopUser(parts.get(0), Long.parseLong(parts.get(1)), parts.get(2));
  }

  public String toUserPrincipal() {
    return String.join(SEP, username, workspaceId.toString(), role);
  }
}
