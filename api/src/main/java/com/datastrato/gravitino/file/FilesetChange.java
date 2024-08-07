/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Objects;

/**
 * A fileset change is a change to a fileset. It can be used to rename a fileset, update the comment
 * of a fileset, set a property and value pair for a fileset, or remove a property from a fileset.
 */
@Evolving
public interface FilesetChange {

  /**
   * Creates a new fileset change to rename the fileset.
   *
   * @param newName The new name of the fileset.
   * @return The fileset change.
   */
  static FilesetChange rename(String newName) {
    return new RenameFileset(newName);
  }

  /**
   * Creates a new fileset change to update the fileset comment.
   *
   * @param newComment The new comment for the fileset.
   * @return The fileset change.
   */
  static FilesetChange updateComment(String newComment) {
    return new UpdateFilesetComment(newComment);
  }

  /**
   * Creates a new fileset change to set the property and value for the fileset.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The fileset change.
   */
  static FilesetChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new fileset change to remove a property from the fileset.
   *
   * @param property The property name to remove.
   * @return The fileset change.
   */
  static FilesetChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Creates a new fileset change to remove comment from the fileset.
   *
   * @return The fileset change.
   */
  static FilesetChange removeComment() {
    return RemoveComment.getInstance();
  }

  /**
   * Creates a new fileset change to add new backup storage location.
   *
   * @param backupStorageLocationKey the key of the backup storage location
   * @param backupStorageLocationValue the value of the backup storage location
   * @return The fileset change.
   */
  static FilesetChange addBackupStorageLocation(
      String backupStorageLocationKey, String backupStorageLocationValue) {
    return new AddBackupStorageLocation(backupStorageLocationKey, backupStorageLocationValue);
  }

  /**
   * Creates a new fileset change to remove backup storage location.
   *
   * @param backupStorageLocationKey the key of the backup storage location
   * @return The fileset change.
   */
  static FilesetChange removeBackupStorageLocation(String backupStorageLocationKey) {
    return new RemoveBackupStorageLocation(backupStorageLocationKey);
  }

  /**
   * Creates a new fileset change to update backup storage location.
   *
   * @param backupStorageLocationKey the key of the backup storage location
   * @param backupStorageLocationNewValue the new value of the backup storage location
   * @return The fileset change.
   */
  static FilesetChange updateBackupStorageLocation(
      String backupStorageLocationKey, String backupStorageLocationNewValue) {
    return new UpdateBackupStorageLocation(backupStorageLocationKey, backupStorageLocationNewValue);
  }

  /**
   * Creates a new fileset change to update primary storage location.
   *
   * @param newPrimaryStorageLocation the new value of the primary storage location
   * @return The fileset change.
   */
  static FilesetChange updatePrimaryStorageLocation(String newPrimaryStorageLocation) {
    return new UpdatePrimaryStorageLocation(newPrimaryStorageLocation);
  }

  /**
   * Creates a new fileset change to switch backup storage locations.
   *
   * @param firstBackupStorageLocationKey the key of the first backup storage location
   * @param secondBackupStorageLocationKey the key of the second backup storage location
   * @return The fileset change.
   */
  static FilesetChange switchBackupStorageLocation(
      String firstBackupStorageLocationKey, String secondBackupStorageLocationKey) {
    return new SwitchBackupStorageLocation(
        firstBackupStorageLocationKey, secondBackupStorageLocationKey);
  }

  /**
   * Creates a new fileset change to switch primary and backup storage locations.
   *
   * @param backupStorageLocationKey the key of the backup storage location
   * @return The fileset change.
   */
  static FilesetChange switchPrimaryAndBackupStorageLocation(String backupStorageLocationKey) {
    return new SwitchPrimaryAndBackupStorageLocation(backupStorageLocationKey);
  }

  /** A fileset change to rename the fileset. */
  final class RenameFileset implements FilesetChange {
    private final String newName;

    private RenameFileset(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name set for the fileset.
     *
     * @return The new name of the fileset.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameFileset instance with another object for equality. Two instances are
     * considered equal if they designate the same new name for the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents an identical fileset renaming operation; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameFileset that = (RenameFileset) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameFileset instance. The hash code is primarily based on
     * the new name for the fileset.
     *
     * @return A hash code value for this renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    /**
     * Provides a string representation of the RenameFile instance. This string includes the class
     * name followed by the new name of the fileset.
     *
     * @return A string summary of this renaming operation.
     */
    @Override
    public String toString() {
      return "RENAMEFILESET " + newName;
    }
  }

  /** A fileset change to update the fileset comment. */
  final class UpdateFilesetComment implements FilesetChange {
    private final String newComment;

    private UpdateFilesetComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment intended for the fileset.
     *
     * @return The new comment that has been set for the fileset.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateFilesetComment instance with another object for equality. Two instances
     * are considered equal if they designate the same new comment for the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateFilesetComment that = (UpdateFilesetComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateFileComment instance. The hash code is based on the new
     * comment for the fileset.
     *
     * @return A hash code representing this comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateFilesetComment instance. This string format
     * includes the class name followed by the new comment for the fileset.
     *
     * @return A string summary of this comment update operation.
     */
    @Override
    public String toString() {
      return "UPDATEFILESETCOMMENT " + newComment;
    }
  }

  /** A fileset change to set the property and value for the fileset. */
  final class SetProperty implements FilesetChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property being set in the fileset.
     *
     * @return The name of the property.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value assigned to the property in the fileset.
     *
     * @return The value of the property.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they have the same property and value for the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property setting; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on both the
     * property name and its assigned value.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property and its value.
     *
     * @return A string summary of the property setting.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A fileset change to remove a property from the fileset. */
  final class RemoveProperty implements FilesetChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the fileset.
     *
     * @return The name of the property for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal from the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property removal; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. The hash code is based on the
     * property name that is to be removed from the fileset.
     *
     * @return A hash code value for this property removal operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    /**
     * Provides a string representation of the RemoveProperty instance. This string format includes
     * the class name followed by the property name to be removed.
     *
     * @return A string summary of the property removal operation.
     */
    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }

  /** A fileset change to remove comment from the fileset. */
  final class RemoveComment implements FilesetChange {
    private static final RemoveComment INSTANCE = new RemoveComment();

    private static RemoveComment getInstance() {
      return INSTANCE;
    }

    /**
     * Compares this RemoveComment instance with another object for equality. Two instances are
     * considered equal if they are RemoveComment instance.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents remove comment change; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      return o != null && getClass() == o.getClass();
    }

    /**
     * Generates a hash code for this RemoveComment instance. The hash code is based on the
     * RemoveComment instance name.
     *
     * @return A hash code value for instance.
     */
    @Override
    public int hashCode() {
      return Objects.hash("REMOVECOMMENT");
    }

    /**
     * Provides a string representation of the RemoveComment instance. This string format includes
     * the class name.
     *
     * @return A string summary of the comment removal operation.
     */
    @Override
    public String toString() {
      return "REMOVECOMMENT";
    }
  }

  /** A fileset change to add a backup storage location for the fileset. */
  final class AddBackupStorageLocation implements FilesetChange {
    private final String backupStorageLocationKey;
    private final String backupStorageLocationValue;

    private AddBackupStorageLocation(
        String backupStorageLocationKey, String backupStorageLocationValue) {
      this.backupStorageLocationKey = backupStorageLocationKey;
      this.backupStorageLocationValue = backupStorageLocationValue;
    }

    /**
     * Retrieves the key of the backup storage location to be added.
     *
     * @return The key of the backup storage location.
     */
    public String getBackupStorageLocationKey() {
      return backupStorageLocationKey;
    }

    /**
     * Retrieves the value of the backup storage location to be added.
     *
     * @return The value of the backup storage location.
     */
    public String getBackupStorageLocationValue() {
      return backupStorageLocationValue;
    }

    /**
     * Compares this AddBackupStorageLocation instance with another object for equality. Two
     * instances are considered equal if they target the same backup storage location key and value
     * of the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same backup storage location key and value;
     *     false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddBackupStorageLocation that = (AddBackupStorageLocation) o;
      return Objects.equals(backupStorageLocationKey, that.backupStorageLocationKey)
          && Objects.equals(backupStorageLocationValue, that.backupStorageLocationValue);
    }

    /**
     * Returns a hash code value for this AddBackupStorageLocation instance. The hash code is based
     * on the backup storage location key and value.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
      return Objects.hash(backupStorageLocationKey, backupStorageLocationValue);
    }

    /**
     * Returns a string representation of this AddBackupStorageLocation instance. The string
     * representation includes the backup storage location key and value that are to be added for
     * the fileset.
     *
     * @return A string representation of adding a backup storage location operation.
     */
    @Override
    public String toString() {
      return "ADDBACKUPSTORAGELOCATION "
          + backupStorageLocationKey
          + " "
          + backupStorageLocationValue;
    }
  }

  /** A fileset change to remove a backup storage location from the fileset. */
  final class RemoveBackupStorageLocation implements FilesetChange {
    private final String backupStorageLocationKey;

    private RemoveBackupStorageLocation(String backupStorageLocationKey) {
      this.backupStorageLocationKey = backupStorageLocationKey;
    }

    /**
     * Retrieves the key of the backup storage location to be removed.
     *
     * @return The key of the backup storage location.
     */
    public String getBackupStorageLocationKey() {
      return backupStorageLocationKey;
    }

    /**
     * Compares this RemoveBackupStorageLocation instance with another object for equality. Two
     * instances are considered equal if they target the same backup storage location key of the
     * fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same backup storage location key; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveBackupStorageLocation that = (RemoveBackupStorageLocation) o;
      return Objects.equals(backupStorageLocationKey, that.backupStorageLocationKey);
    }

    /**
     * Returns a hash code value for this RemoveBackupStorageLocation instance. The hash code is
     * based on the backup storage location key.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
      return Objects.hash(backupStorageLocationKey);
    }

    /**
     * Returns a string representation of this RemoveBackupStorageLocation instance. The string
     * representation includes the backup storage location key that is to be removed from the
     * fileset.
     *
     * @return A string representation of removing a backup storage location operation.
     */
    @Override
    public String toString() {
      return "REMOVEBACKUPSTORAGELOCATION " + backupStorageLocationKey;
    }
  }

  /** A fileset change to update a backup storage location for the fileset. */
  final class UpdateBackupStorageLocation implements FilesetChange {
    private final String backupStorageLocationKey;
    private final String backupStorageLocationNewValue;

    private UpdateBackupStorageLocation(
        String backupStorageLocationKey, String backupStorageLocationNewValue) {
      this.backupStorageLocationKey = backupStorageLocationKey;
      this.backupStorageLocationNewValue = backupStorageLocationNewValue;
    }

    /**
     * Retrieves the key of the backup storage location to be removed.
     *
     * @return The key of the backup storage location.
     */
    public String getBackupStorageLocationKey() {
      return backupStorageLocationKey;
    }

    /**
     * Retrieves the new value of the backup storage location for the fileset.
     *
     * @return The new value of the backup storage location.
     */
    public String getBackupStorageLocationNewValue() {
      return backupStorageLocationNewValue;
    }

    /**
     * Compares this UpdateBackupStorageLocation instance with another object for equality. Two
     * instances are considered equal if they target the same backup storage location key and the
     * new value of the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same backup storage location key and the new
     *     value; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateBackupStorageLocation that = (UpdateBackupStorageLocation) o;
      return Objects.equals(backupStorageLocationKey, that.backupStorageLocationKey)
          && Objects.equals(backupStorageLocationNewValue, that.backupStorageLocationNewValue);
    }

    /**
     * Returns a hash code value for this UpdateBackupStorageLocation instance. The hash code is
     * based on the backup storage location key and the new value.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
      return Objects.hash(backupStorageLocationKey, backupStorageLocationNewValue);
    }

    /**
     * Returns a string representation of this UpdateBackupStorageLocation instance. The string
     * representation includes the backup storage location key and the new value that are to be
     * updated for the fileset.
     *
     * @return A string representation of updating a backup storage location operation.
     */
    @Override
    public String toString() {
      return "UPDATEBACKUPSTORAGELOCATION "
          + backupStorageLocationKey
          + " "
          + backupStorageLocationNewValue;
    }
  }

  /** A fileset change to update the primary storage location for the fileset. */
  final class UpdatePrimaryStorageLocation implements FilesetChange {
    private final String newPrimaryStorageLocation;

    private UpdatePrimaryStorageLocation(String newPrimaryStorageLocation) {
      this.newPrimaryStorageLocation = newPrimaryStorageLocation;
    }

    /**
     * Returns the new primary storage location for the fileset.
     *
     * @return The new primary storage location for the fileset.
     */
    public String getNewPrimaryStorageLocation() {
      return newPrimaryStorageLocation;
    }

    /**
     * Compares this UpdatePrimaryStorageLocation instance with another object for equality. Two
     * instances are considered equal if they target the same new primary storage location of the
     * fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same new primary storage location; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdatePrimaryStorageLocation that = (UpdatePrimaryStorageLocation) o;
      return Objects.equals(newPrimaryStorageLocation, that.newPrimaryStorageLocation);
    }

    /**
     * Returns a hash code value for this UpdatePrimaryStorageLocation instance. The hash code is
     * based on the new primary storage location.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newPrimaryStorageLocation);
    }

    /**
     * Returns a string representation of this UpdatePrimaryStorageLocation instance. The string
     * representation includes the primary storage location that is to be updated for the fileset.
     *
     * @return A string representation of updating the primary storage location operation.
     */
    @Override
    public String toString() {
      return "UPDATEPRIMARYSTORAGELOCATION " + newPrimaryStorageLocation;
    }
  }

  /** A fileset change to switch backup storage locations for the fileset. */
  final class SwitchBackupStorageLocation implements FilesetChange {
    private final String firstBackupStorageLocationKey;
    private final String secondBackupStorageLocationKey;

    private SwitchBackupStorageLocation(
        String firstBackupStorageLocationKey, String secondBackupStorageLocationKey) {
      this.firstBackupStorageLocationKey = firstBackupStorageLocationKey;
      this.secondBackupStorageLocationKey = secondBackupStorageLocationKey;
    }

    /**
     * Returns the first backup storage location key of the fileset.
     *
     * @return The first backup storage location key.
     */
    public String getFirstBackupStorageLocationKey() {
      return firstBackupStorageLocationKey;
    }

    /**
     * Returns the second backup storage location key of the fileset.
     *
     * @return The second backup storage location key.
     */
    public String getSecondBackupStorageLocationKey() {
      return secondBackupStorageLocationKey;
    }

    /**
     * Compares this SwitchBackupStorageLocation instance with another object for equality. Two
     * instances are considered equal if they target the same firstBackupStorageLocationKey and
     * secondBackupStorageLocationKey of the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same firstBackupStorageLocationKey and
     *     secondBackupStorageLocationKey; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SwitchBackupStorageLocation that = (SwitchBackupStorageLocation) o;
      return Objects.equals(firstBackupStorageLocationKey, that.firstBackupStorageLocationKey)
          && Objects.equals(secondBackupStorageLocationKey, that.secondBackupStorageLocationKey);
    }

    /**
     * Returns a hash code value for this SwitchBackupStorageLocation instance. The hash code is
     * based on the firstBackupStorageLocationKey and secondBackupStorageLocationKey.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
      return Objects.hash(firstBackupStorageLocationKey, secondBackupStorageLocationKey);
    }

    /**
     * Returns a string representation of this SwitchBackupStorageLocation instance. The string
     * representation includes the firstBackupStorageLocationKey and secondBackupStorageLocationKey
     * that are to be updated for the fileset.
     *
     * @return A string representation of switching backup storage locations operation.
     */
    @Override
    public String toString() {
      return "SWITCHBACKUPSTORAGELOCATION "
          + firstBackupStorageLocationKey
          + " "
          + secondBackupStorageLocationKey;
    }
  }

  /** A fileset change to switch backup storage locations for the fileset. */
  final class SwitchPrimaryAndBackupStorageLocation implements FilesetChange {
    private final String backupStorageLocationKey;

    private SwitchPrimaryAndBackupStorageLocation(String backupStorageLocationKey) {
      this.backupStorageLocationKey = backupStorageLocationKey;
    }

    /**
     * Returns the backup storage location key.
     *
     * @return The backup storage location key.
     */
    public String getBackupStorageLocationKey() {
      return backupStorageLocationKey;
    }

    /**
     * Compares this SwitchPrimaryAndBackupStorageLocation instance with another object for
     * equality. Two instances are considered equal if they target the same backupStorageLocationKey
     * of the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same backupStorageLocationKey; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SwitchPrimaryAndBackupStorageLocation that = (SwitchPrimaryAndBackupStorageLocation) o;
      return Objects.equals(backupStorageLocationKey, that.backupStorageLocationKey);
    }

    /**
     * Returns a hash code value for this SwitchPrimaryAndBackupStorageLocation instance. The hash
     * code is based on the backupStorageLocationKey.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
      return Objects.hash(backupStorageLocationKey);
    }

    /**
     * Returns a string representation of this SwitchPrimaryAndBackupStorageLocation instance. The
     * string representation includes the backupStorageLocationKey that is to be updated for the
     * fileset.
     *
     * @return A string representation of switching primary and backup storage locations operation.
     */
    @Override
    public String toString() {
      return "SWITCHPRIMARYANDBACKUPSTORAGELOCATION " + backupStorageLocationKey;
    }
  }
}
