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
 * Represents an event that is triggered when an attempt to list partitions within a namespace fails
 * due to an exception.
 */
@DeveloperApi
public final class ListPartitionFailureEvent extends PartitionFailureEvent {
  /**
   * Constructs a {@code ListPartitionFailureEvent} instance.
   *
   * @param user The username of the individual who initiated the operation to list partitions.
   * @param ident The identifier from which partitions were listed.
   * @param exception The exception encountered during the attempt to list partitions.
   */
  public ListPartitionFailureEvent(String user, NameIdentifier ident, Exception exception) {
    super(user, ident, exception);
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_PARTITION;
  }
}
