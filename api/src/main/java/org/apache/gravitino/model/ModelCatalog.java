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
package org.apache.gravitino.model;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/**
 * The ModelCatalog interface defines the public API for managing model objects in a schema. If the
 * catalog implementation supports model objects, it should implement this interface.
 */
@Evolving
public interface ModelCatalog {

  /**
   * List the models in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of model identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Get a model metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A model identifier.
   * @return The model metadata.
   * @throws NoSuchModelException If the model does not exist.
   */
  Model getModel(NameIdentifier ident) throws NoSuchModelException;

  /**
   * Check if a model exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A model identifier.
   * @return true If the model exists, false if the model does not exist.
   */
  default boolean modelExists(NameIdentifier ident) {
    try {
      getModel(ident);
      return true;
    } catch (NoSuchModelException e) {
      return false;
    }
  }

  /**
   * Register a model in the catalog if the model is not existed, otherwise the {@link
   * ModelAlreadyExistsException} will be throw. The {@link Model} object will be created when the
   * model is registered, in the meantime, the model version (version 0) will also be created and
   * linked to the registered model.
   *
   * @param ident The name identifier of the model.
   * @param aliases The aliases of the model version. The alias are optional and can be empty.
   * @param comment The comment of the model. The comment is optional and can be null.
   * @param url The model artifact URL.
   * @param properties The properties of the model. The properties are optional and can be null or
   *     empty.
   * @return The registered model object.
   * @throws ModelAlreadyExistsException If the model already registered.
   */
  Model registerModel(
      NameIdentifier ident,
      String[] aliases,
      String comment,
      String url,
      Map<String, String> properties)
      throws ModelAlreadyExistsException;

  /**
   * Delete the model from the catalog. If the model does not exist, return false. Otherwise, return
   * true. The deletion of the model will also delete all the model versions linked to this model.
   *
   * @param ident The name identifier of the model.
   * @return True if the model is deleted, false if the model does not exist.
   */
  boolean deleteModel(NameIdentifier ident);
}
