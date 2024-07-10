/*
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

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered upon the successful list of tables within a namespace.
 *
 * <p>To optimize memory usage and avoid the potential overhead associated with storing a large
 * number of tables directly within the ListTableEvent, the actual tables listed are not maintained
 * in this event. This design decision helps in managing resource efficiency, especially in
 * environments with extensive table listings.
 */
@DeveloperApi
public final class ListTableEvent extends TableEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListTableEvent}.
   *
   * @param user The username of the individual who initiated the table listing.
   * @param namespace The namespace from which tables were listed.
   */
  public ListTableEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which tables were listed.
   */
  public Namespace namespace() {
    return namespace;
  }
}
