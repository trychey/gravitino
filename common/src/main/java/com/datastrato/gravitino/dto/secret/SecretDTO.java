/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.secret;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.secret.Secret;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Represents a Secret Data Transfer Object (DTO). */
public class SecretDTO implements Secret {

  @JsonProperty("name")
  private String name;

  @JsonProperty("value")
  private String value;

  @JsonProperty("type")
  private String type;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  protected SecretDTO() {}

  /**
   * Creates a new instance of SecretDTO.
   *
   * @param name The name of the Secret DTO.
   * @param value The value of the Secret DTO.
   * @param type The target of the Secret DTO.
   * @param properties The properties of the Secret DTO.
   * @param audit The audit information of the Secret DTO.
   */
  protected SecretDTO(
      String name, String value, String type, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.value = value;
    this.type = type;
    this.properties = properties;
    this.audit = audit;
  }

  /**
   * The name of the secret.
   *
   * @return The name of the secret.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The value of the secret.
   *
   * @return The value of the secret.
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * The type of the secret. The target is the entity which the secret is used for.
   *
   * @return The target of the secret.
   */
  @Override
  public String type() {
    return type;
  }

  /**
   * The properties of the secret. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the secret.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * The audit information of the secret.
   *
   * @return The audit information of the secret.
   */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing an Secret DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing a SecretDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the secret. */
    protected String name;

    /** The value of the secret. */
    protected String value;

    /** The type of the secret. */
    protected String type;

    /** The properties of the secret. */
    protected Map<String, String> properties;

    /** The audit information of the secret. */
    protected AuditDTO audit;

    /**
     * Sets the name of the secret.
     *
     * @param name The name of the secret.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the value of the secret.
     *
     * @param value The value of the secret.
     * @return The builder instance.
     */
    public S withValue(String value) {
      this.value = value;
      return (S) this;
    }

    /**
     * Sets the type of the secret.
     *
     * @param type The type of the secret.
     * @return The builder instance.
     */
    public S withType(String type) {
      this.type = type;
      return (S) this;
    }

    /**
     * Sets the properties of the secret.
     *
     * @param properties The properties of the secret.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the secret.
     *
     * @param audit The audit information of the secret.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of SecretDTO using the builder's properties.
     *
     * @return An instance of SecretDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public SecretDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "value cannot be null or empty");
      Preconditions.checkArgument(StringUtils.isNotBlank(type), "type cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new SecretDTO(name, value, type, properties, audit);
    }
  }
}
