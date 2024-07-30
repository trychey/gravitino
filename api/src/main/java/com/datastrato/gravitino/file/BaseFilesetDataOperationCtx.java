/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.Map;

/** Base implementation of {@link FilesetDataOperationCtx}. */
public class BaseFilesetDataOperationCtx implements FilesetDataOperationCtx {
  private String subPath;
  private FilesetDataOperation operation;
  private ClientType clientType;
  private String ip;
  private SourceEngineType sourceEngineType;
  private String appId;
  private Map<String, String> extraInfo;

  @Override
  public String subPath() {
    return subPath;
  }

  @Override
  public FilesetDataOperation operation() {
    return operation;
  }

  @Override
  public ClientType clientType() {
    return clientType;
  }

  @Override
  public String ip() {
    return ip;
  }

  @Override
  public SourceEngineType sourceEngineType() {
    return sourceEngineType;
  }

  @Override
  public String appId() {
    return appId;
  }

  @Override
  public Map<String, String> extraInfo() {
    return extraInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BaseFilesetDataOperationCtx)) return false;
    BaseFilesetDataOperationCtx that = (BaseFilesetDataOperationCtx) o;
    return Objects.equal(subPath, that.subPath)
        && operation == that.operation
        && clientType == that.clientType
        && Objects.equal(ip, that.ip)
        && sourceEngineType == that.sourceEngineType
        && Objects.equal(appId, that.appId)
        && Objects.equal(extraInfo, that.extraInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(subPath, operation, clientType, ip, sourceEngineType, appId, extraInfo);
  }

  public static class Builder {
    private BaseFilesetDataOperationCtx context;
    /** Creates a new instance of {@link BaseFilesetDataOperationCtx.Builder}. */
    private Builder() {
      context = new BaseFilesetDataOperationCtx();
    }

    public BaseFilesetDataOperationCtx.Builder withSubPath(String subPath) {
      context.subPath = subPath;
      return this;
    }

    public BaseFilesetDataOperationCtx.Builder withOperation(FilesetDataOperation operation) {
      context.operation = operation;
      return this;
    }

    public BaseFilesetDataOperationCtx.Builder withClientType(ClientType clientType) {
      context.clientType = clientType;
      return this;
    }

    public BaseFilesetDataOperationCtx.Builder withIp(String ip) {
      context.ip = ip;
      return this;
    }

    public BaseFilesetDataOperationCtx.Builder withSourceEngineType(
        SourceEngineType sourceEngineType) {
      context.sourceEngineType = sourceEngineType;
      return this;
    }

    public BaseFilesetDataOperationCtx.Builder withAppId(String appId) {
      context.appId = appId;
      return this;
    }

    public BaseFilesetDataOperationCtx.Builder withExtraInfo(Map<String, String> extraInfo) {
      context.extraInfo = extraInfo;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(context.subPath != null, "subPath cannot be null");
      Preconditions.checkArgument(context.operation != null, "operation is required");
      Preconditions.checkArgument(context.clientType != null, "clientType is required");
      Preconditions.checkArgument(context.sourceEngineType != null, "sourceEngineType is required");
    }

    public BaseFilesetDataOperationCtx build() {
      validate();
      return context;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
