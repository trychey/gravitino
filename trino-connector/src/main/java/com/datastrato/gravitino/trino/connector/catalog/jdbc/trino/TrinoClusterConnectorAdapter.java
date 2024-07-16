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
package com.datastrato.gravitino.trino.connector.catalog.jdbc.trino;

import static com.datastrato.gravitino.trino.connector.GravitinoConnectorPluginManager.CONNECTOR_CLUSTER;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_URL_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Support trino Memory connector for testing. Transforming Memory connector configuration and
 * components into Gravitino connector.
 */
public class TrinoClusterConnectorAdapter implements CatalogConnectorAdapter {

  public static final String TRINO_CLUSTER_URL_KEY = "cluster.connection-url";
  public static final String TRINO_CLUSTER_USER_KEY = "cluster.connection-user";
  public static final String TRINO_CLUSTER_PASSWORD_KEY = "cluster.connection-password";
  public static final String TRINO_CLUSTER_DEFAULT_USER = "admin";

  private final HasPropertyMeta propertyMetadata;

  public TrinoClusterConnectorAdapter() {
    this.propertyMetadata = new TrinoClusterPropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog) {
    Map<String, String> config = new HashMap<>();
    // Todo(yuhui) we retrieve the cluster information from the catalog attributes.
    String jdbc_url = catalog.getProperty(TRINO_CLUSTER_URL_KEY, "");
    if (StringUtils.isEmpty(jdbc_url)) {
      throw new TrinoException(
          GRAVITINO_MISSING_CONFIG, "Missing jdbc url config for the cluster catalog");
    }
    jdbc_url += "/" + catalog.getName();
    config.put(JDBC_CONNECTION_URL_KEY, jdbc_url);

    String user = catalog.getProperty(TRINO_CLUSTER_USER_KEY, TRINO_CLUSTER_DEFAULT_USER);
    config.put(JDBC_CONNECTION_USER_KEY, user);

    String password = catalog.getProperty(TRINO_CLUSTER_PASSWORD_KEY, "");
    config.put(JDBC_CONNECTION_PASSWORD_KEY, password);
    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_CLUSTER;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new TrinoClusterMetadataAdapter(
        getTableProperties(), Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }
}
