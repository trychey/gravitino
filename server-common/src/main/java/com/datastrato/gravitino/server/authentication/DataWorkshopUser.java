package com.datastrato.gravitino.server.authentication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataWorkshopUser {
  private final String username;
  private final String workspaceId;
  private final String role;
  private final String rangerRole;

  @JsonCreator
  public DataWorkshopUser(
      @JsonProperty("username") String username,
      @JsonProperty("workspaceId") String workspaceId,
      @JsonProperty("role") String role,
      @JsonProperty("rangerRole") String rangerRole) {
    this.username = username;
    this.workspaceId = workspaceId;
    this.role = role;
    this.rangerRole = rangerRole;
  }

  public String getUsername() {
    return username;
  }

  public String getWorkspaceId() {
    return workspaceId;
  }

  public String getRole() {
    return role;
  }

  public String getRangerRole() {
    return rangerRole;
  }
}
