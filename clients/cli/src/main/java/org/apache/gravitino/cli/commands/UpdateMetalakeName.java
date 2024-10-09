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

import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** Update the name of a metalake. */
public class UpdateMetalakeName extends Command {

  protected String metalake;
  protected String name;

  /**
   * Update the name of a metalake.
   *
   * @param url The URL of the Gravitino server.
   * @param metalake The name of the metalake.
   * @param name The new metalake name.
   */
  public UpdateMetalakeName(String url, String metalake, String name) {
    super(url);
    this.metalake = metalake;
    this.name = name;
  }

  /** Update the name of a metalake. */
  public void handle() {
    try {
      GravitinoAdminClient client = buildAdminClient();
      MetalakeChange change = MetalakeChange.rename(name);
      client.alterMetalake(metalake, change);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(metalake + " name changed.");
  }
}
