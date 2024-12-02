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

package org.apache.gravitino.cli.commands;

import com.google.common.base.Joiner;
import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** List the topics. */
public class ListTopics extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;

  /**
   * List the names of all topics in a schema.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param authentication Authentication type i.e. "simple"
   * @param userName User name for simple authentication.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   */
  public ListTopics(
      String url,
      boolean ignoreVersions,
      String authentication,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    super(url, ignoreVersions, authentication, userName);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
  }

  /** List the names of all topics in a schema. */
  @Override
  public void handle() {
    NameIdentifier[] topics = new NameIdentifier[0];
    Namespace name = Namespace.of(schema);

    try {
      GravitinoClient client = buildClient(metalake);
      topics = client.loadCatalog(catalog).asTopicCatalog().listTopics(name);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    String all = Joiner.on(",").join(Arrays.stream(topics).map(topic -> topic.name()).iterator());
    System.out.println(all);
  }
}
