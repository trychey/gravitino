/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.TableInfo;
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;

/** Represents an event triggered before creating a partition. */
@DeveloperApi
public class AddPartitionPreEvent extends PartitionPreEvent {

  private final PartitionInfo createdPartitionInfo;

  public AddPartitionPreEvent(
      String user, NameIdentifier identifier, PartitionInfo createdPartitionInfo) {
    super(user, identifier);
    this.createdPartitionInfo = createdPartitionInfo;
  }

  /**
   * Retrieves the create partition request.
   *
   * @return A {@link TableInfo} instance encapsulating the comprehensive details of create
   *     partition request.
   */
  public PartitionInfo createdPartitionRequest() {
    return createdPartitionInfo;
  }
}
