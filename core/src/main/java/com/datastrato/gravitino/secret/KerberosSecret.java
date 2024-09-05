/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.meta.AuditInfo;
import java.util.Map;
import java.util.Objects;

public class KerberosSecret implements Secret {

  public static final Field NAME =
      Field.required("name", String.class, "The name of the kerberos secret.");

  public static final Field VALUE =
      Field.required("value", String.class, "The value of the kerberos secret.");

  public static final Field TYPE =
      Field.required("type", String.class, "The type of the kerberos secret.");

  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the kerberos secret.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the kerberos secret.");

  private String name;
  private String value;
  private String type;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private KerberosSecret() {}

  /**
   * Returns the name of the group.
   *
   * @return The name of the group.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the value of the secret.
   *
   * @return The value of the secret.
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * Returns the type of the secret.
   *
   * @return The type of the secret.
   */
  @Override
  public String type() {
    return type;
  }

  /**
   * Returns the properties of the secret.
   *
   * @return The properties of the secret.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the audit details of the secret.
   *
   * @return The audit details of the secret.
   */
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof KerberosSecret)) return false;

    KerberosSecret that = (KerberosSecret) o;
    return Objects.equals(name, that.name)
        && Objects.equals(value, that.value)
        && Objects.equals(type, that.type)
        && Objects.equals(properties, that.properties)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, type, properties, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final KerberosSecret kerberosSecret;

    private Builder() {
      this.kerberosSecret = new KerberosSecret();
    }

    /**
     * Sets the name of the kerberos Secret.
     *
     * @param name The name of the kerberos Secret.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      kerberosSecret.name = name;
      return this;
    }

    /**
     * Sets the value of the secret.
     *
     * @param value The value of the secret.
     * @return The builder instance.
     */
    public Builder withValue(String value) {
      kerberosSecret.value = value;
      return this;
    }

    /**
     * Sets the type of the secret.
     *
     * @param type The type of the secret.
     * @return The builder instance.
     */
    public Builder withType(String type) {
      kerberosSecret.type = type;
      return this;
    }

    /**
     * Sets the properties of the secret.
     *
     * @param properties The properties of the secret.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      kerberosSecret.properties = properties;
      return this;
    }

    /**
     * Sets the audit details of the kerberos Secret.
     *
     * @param auditInfo The audit details of the kerberos Secret.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      kerberosSecret.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds the kerberos Secret.
     *
     * @return The built kerberosSecret.
     */
    public KerberosSecret build() {
      return kerberosSecret;
    }
  }
}
