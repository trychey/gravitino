/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.postgresql.integration;

import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-it")
public class CatalogPostgreSqlVersion12IT extends CatalogPostgreSqlIT {
  public CatalogPostgreSqlVersion12IT() {
    postgreImageName = "postgres:12";
  }
}
