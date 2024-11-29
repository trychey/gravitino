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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated after a Metalake is successfully removed from the system.
 */
@DeveloperApi
public final class DropMetalakeEvent extends MetalakeEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DropMetalakeEvent} instance, encapsulating information about the
   * outcome of a metalake drop operation.
   *
   * @param user The user who initiated the drop metalake operation.
   * @param identifier The identifier of the metalake that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the metalake existed at the time of the drop
   *     operation.
   */
  public DropMetalakeEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the Metalake at the time of the removal operation.
   *
   * @return A boolean value indicating whether the Metalake existed. {@code true} if the Metalake
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DROP_METALAKE;
  }
}
