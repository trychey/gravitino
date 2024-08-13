package com.datastrato.gravitino.audit.entity;

import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;
import com.datastrato.gravitino.file.SourceEngineType;
import com.google.common.base.Objects;
import java.util.Arrays;
import java.util.Map;

public class GetFilesetContextAuditEntity {
  private String subPath;
  private FilesetDataOperation operation;
  private ClientType clientType;
  private String ip;
  private SourceEngineType sourceEngineType;
  private String appId;
  private Map<String, String> extraInfo;
  private Fileset.Type filesetType;
  private String storageLocation;
  private String[] actualPaths;

  private GetFilesetContextAuditEntity() {}

  private GetFilesetContextAuditEntity(
      FilesetContext filesetContext, FilesetDataOperationCtx dataOperationCtx) {
    this.subPath = dataOperationCtx.subPath();
    this.operation = dataOperationCtx.operation();
    this.clientType = dataOperationCtx.clientType();
    this.ip = dataOperationCtx.ip();
    this.sourceEngineType = dataOperationCtx.sourceEngineType();
    this.appId = dataOperationCtx.appId();
    this.extraInfo = dataOperationCtx.extraInfo();
    this.filesetType = filesetContext.fileset().type();
    this.storageLocation = filesetContext.fileset().storageLocation();
    this.actualPaths = filesetContext.actualPaths();
  }

  public String subPath() {
    return subPath;
  }

  public FilesetDataOperation operation() {
    return operation;
  }

  public ClientType clientType() {
    return clientType;
  }

  public String ip() {
    return ip;
  }

  public SourceEngineType sourceEngineType() {
    return sourceEngineType;
  }

  public String appId() {
    return appId;
  }

  public Map<String, String> extraInfo() {
    return extraInfo;
  }

  public Fileset.Type filesetType() {
    return filesetType;
  }

  public String storageLocation() {
    return storageLocation;
  }

  public String[] actualPaths() {
    return actualPaths;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GetFilesetContextAuditEntity)) return false;
    GetFilesetContextAuditEntity entity = (GetFilesetContextAuditEntity) o;
    return Objects.equal(subPath, entity.subPath)
        && operation == entity.operation
        && clientType == entity.clientType
        && Objects.equal(ip, entity.ip)
        && sourceEngineType == entity.sourceEngineType
        && Objects.equal(appId, entity.appId)
        && Objects.equal(extraInfo, entity.extraInfo)
        && filesetType == entity.filesetType
        && Objects.equal(storageLocation, entity.storageLocation)
        && Arrays.equals(actualPaths, entity.actualPaths);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        subPath,
        operation,
        clientType,
        ip,
        sourceEngineType,
        appId,
        extraInfo,
        filesetType,
        storageLocation,
        Arrays.hashCode(actualPaths));
  }

  public static Builder builder() {
    return new GetFilesetContextAuditEntity.Builder();
  }

  /** Builder to create a get fileset context audit entity. */
  public static class Builder {

    /** The fileset context. */
    protected FilesetContext filesetContext;

    /** The data operation context. */
    protected FilesetDataOperationCtx dataOperationContext;

    /**
     * Set the fileset context.
     *
     * @param context The fileset context
     * @return The builder for creating a new instance of GetFilesetContextAuditEntity.
     */
    public GetFilesetContextAuditEntity.Builder withFilesetContext(FilesetContext context) {
      this.filesetContext = context;
      return this;
    }

    /**
     * Set the data operation context.
     *
     * @param dataOperationCtx The data operation context
     * @return The builder for creating a new instance of GetFilesetContextAuditEntity.
     */
    public GetFilesetContextAuditEntity.Builder withDataOperationCtx(
        FilesetDataOperationCtx dataOperationCtx) {
      this.dataOperationContext = dataOperationCtx;
      return this;
    }

    /**
     * Build a new instance of GetFilesetContextAuditEntity.
     *
     * @return The new instance.
     */
    public GetFilesetContextAuditEntity build() {
      return new GetFilesetContextAuditEntity(filesetContext, dataOperationContext);
    }
  }
}
