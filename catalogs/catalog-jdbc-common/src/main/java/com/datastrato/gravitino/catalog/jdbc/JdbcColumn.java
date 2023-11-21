/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.catalog.rel.BaseColumn;

/** Represents a column in the Jdbc column. */
public class JdbcColumn extends BaseColumn {
  private String defaultValue;

  private JdbcColumn() {}

  public String getDefaultValue() {
    return defaultValue;
  }

  /** A builder class for constructing JdbcColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, JdbcColumn> {

    /**
     * The default value for this field. This value will be used if the corresponding value is null.
     */
    private String defaultValue;

    public Builder withDefaultValue(String defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    /**
     * Internal method to build a JdbcColumn instance using the provided values.
     *
     * @return A new JdbcColumn instance with the configured values.
     */
    @Override
    protected JdbcColumn internalBuild() {
      JdbcColumn jdbcColumn = new JdbcColumn();
      jdbcColumn.name = name;
      jdbcColumn.comment = comment;
      jdbcColumn.dataType = dataType;
      jdbcColumn.nullable = nullable;
      jdbcColumn.defaultValue = defaultValue;
      return jdbcColumn;
    }
  }
}
