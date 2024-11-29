/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino;

import org.apache.gravitino.annotation.Evolving;

/**
 * A Catalog provider is a class that provides a short name for a catalog. This short name is used
 * when creating a catalog.
 */
@Evolving
public interface CatalogProvider {
  enum CatalogName {
    HIVE("hive"),
    HADOOP("hadoop"),
    KAFKA("kafka"),
    JDBC_DORIS("jdbc-doris"),
    JDBC_MYSQL("jdbc-mysql"),
    JDBC_OCEANBASE("jdbc-oceanbase"),
    JDBC_POSTGRESQL("jdbc-postgresql"),
    LAKEHOUSE_ICEBERG("lakehouse-iceberg"),
    LAKEHOUSE_HUDI("lakehouse-hudi"),
    LAKEHOUSE_PAIMON("lakehouse-paimon");
    private final String name;

    CatalogName(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }
  /**
   * The string that represents the catalog that this provider uses. This is overridden by children
   * to provide a nice alias for the catalog.
   *
   * @return The string that represents the catalog that this provider uses.
   */
  String shortName();
}
