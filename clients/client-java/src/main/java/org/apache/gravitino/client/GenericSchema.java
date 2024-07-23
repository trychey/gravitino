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
package org.apache.gravitino.client;

import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Schema;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.tag.SupportsTags;

/** Represents a generic schema. */
public class GenericSchema implements Schema, SupportsTagOperations {

  private final SchemaDTO schemaDTO;

  private final RESTClient restClient;

  private final String metalake;

  private final String catalog;

  GenericSchema(SchemaDTO schemaDTO, RESTClient restClient, String metalake, String catalog) {
    this.schemaDTO = schemaDTO;
    this.restClient = restClient;
    this.metalake = metalake;
    this.catalog = catalog;
  }

  @Override
  public SupportsTags supportsTags() {
    return this;
  }

  @Override
  public String name() {
    return schemaDTO.name();
  }

  @Override
  public String comment() {
    return schemaDTO.comment();
  }

  @Override
  public Map<String, String> properties() {
    return schemaDTO.properties();
  }

  @Override
  public Audit auditInfo() {
    return schemaDTO.auditInfo();
  }

  @Override
  public String metalakeName() {
    return metalake;
  }

  @Override
  public MetadataObject metadataObject() {
    return MetadataObjects.of(catalog, name(), MetadataObject.Type.SCHEMA);
  }

  @Override
  public RESTClient restClient() {
    return restClient;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericSchema)) {
      return false;
    }

    GenericSchema that = (GenericSchema) obj;
    return schemaDTO.equals(that.schemaDTO);
  }

  @Override
  public int hashCode() {
    return schemaDTO.hashCode();
  }
}
