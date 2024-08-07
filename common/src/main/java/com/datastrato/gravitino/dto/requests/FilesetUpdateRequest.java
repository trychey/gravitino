/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Request to update a fileset. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = FilesetUpdateRequest.RenameFilesetRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.UpdateFilesetCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.RemoveFilesetCommentRequest.class,
      name = "removeComment"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.SetFilesetPropertiesRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.RemoveFilesetPropertiesRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.AddFilesetBackupStorageLocationRequest.class,
      name = "addBackupStorageLocation"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.RemoveFilesetBackupStorageLocationRequest.class,
      name = "removeBackupStorageLocation"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.UpdateFilesetBackupStorageLocationRequest.class,
      name = "updateBackupStorageLocation"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.UpdateFilesetPrimaryStorageLocationRequest.class,
      name = "updatePrimaryStorageLocation"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.SwitchFilesetBackupStorageLocationRequest.class,
      name = "switchBackupStorageLocation"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.SwitchFilesetPrimaryAndBackupStorageLocationRequest.class,
      name = "switchPrimaryAndBackupStorageLocation")
})
public interface FilesetUpdateRequest extends RESTRequest {

  /**
   * Returns the fileset change.
   *
   * @return the fileset change.
   */
  FilesetChange filesetChange();

  /** The fileset update request for renaming a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RenameFilesetRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Returns the fileset change.
     *
     * @return the fileset change.
     */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.rename(newName);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }
  }

  /** The fileset update request for updating the comment of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateFilesetCommentRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /** @return The fileset change. */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.updateComment(newComment);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }
  }

  /** The fileset update request for setting the properties of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class SetFilesetPropertiesRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.setProperty(property, value);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }
  }

  /** The fileset update request for removing the properties of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveFilesetPropertiesRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /** @return The fileset change. */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.removeProperty(property);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }
  }

  /** The fileset update request for removing the comment of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @ToString
  class RemoveFilesetCommentRequest implements FilesetUpdateRequest {

    /** @return The fileset change. */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.removeComment();
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** Request to add a backup storage location to a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class AddFilesetBackupStorageLocationRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("backupStorageLocationKey")
    private final String backupStorageLocationKey;

    @Getter
    @JsonProperty("backupStorageLocationValue")
    private final String backupStorageLocationValue;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.addBackupStorageLocation(
          backupStorageLocationKey, backupStorageLocationValue);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(backupStorageLocationKey),
          "\"backupStorageLocationKey\" field is required and cannot be empty");
      Preconditions.checkArgument(
          backupStorageLocationValue != null,
          "\"backupStorageLocationValue\" field is required and cannot be null");
    }
  }

  /** Request to remove a backup storage location from a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveFilesetBackupStorageLocationRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("backupStorageLocationKey")
    private final String backupStorageLocationKey;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.removeBackupStorageLocation(backupStorageLocationKey);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(backupStorageLocationKey),
          "\"backupStorageLocationKey\" field is required and cannot be empty");
    }
  }

  /** Request to update a backup storage location of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateFilesetBackupStorageLocationRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("backupStorageLocationKey")
    private final String backupStorageLocationKey;

    @Getter
    @JsonProperty("backupStorageLocationNewValue")
    private final String backupStorageLocationNewValue;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.updateBackupStorageLocation(
          backupStorageLocationKey, backupStorageLocationNewValue);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(backupStorageLocationKey),
          "\"backupStorageLocationKey\" field is required and cannot be empty");
      Preconditions.checkArgument(
          backupStorageLocationNewValue != null,
          "\"backupStorageLocationNewValue\" field is required and cannot be null");
    }
  }

  /** Request to update the primary storage location of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateFilesetPrimaryStorageLocationRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newPrimaryStorageLocation")
    private final String newPrimaryStorageLocation;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.updatePrimaryStorageLocation(newPrimaryStorageLocation);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newPrimaryStorageLocation),
          "\"newPrimaryStorageLocation\" field is required and cannot be empty");
    }
  }

  /** Request to switch the backup storage locations of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class SwitchFilesetBackupStorageLocationRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("firstBackupStorageLocationKey")
    private final String firstBackupStorageLocationKey;

    @Getter
    @JsonProperty("secondBackupStorageLocationKey")
    private final String secondBackupStorageLocationKey;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.switchBackupStorageLocation(
          firstBackupStorageLocationKey, secondBackupStorageLocationKey);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(firstBackupStorageLocationKey),
          "\"firstBackupStorageLocationKey\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(secondBackupStorageLocationKey),
          "\"secondBackupStorageLocationKey\" field is required and cannot be empty");
    }
  }

  /** Request to switch the primary and backup storage location of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class SwitchFilesetPrimaryAndBackupStorageLocationRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("backupStorageLocationKey")
    private final String backupStorageLocationKey;

    /** @return The fileset change. */
    public FilesetChange filesetChange() {
      return FilesetChange.switchPrimaryAndBackupStorageLocation(backupStorageLocationKey);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(backupStorageLocationKey),
          "\"backupStorageLocationKey\" field is required and cannot be empty");
    }
  }
}
