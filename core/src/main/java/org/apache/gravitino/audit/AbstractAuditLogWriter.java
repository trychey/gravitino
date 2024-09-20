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

package org.apache.gravitino.audit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.listener.api.event.Event;

public abstract class AbstractAuditLogWriter implements AuditLogWriter {

  private Formatter formatter;

  public AbstractAuditLogWriter(Formatter formatter) {
    this.formatter = formatter;
  }

  public void write(Event event) {
    Object formatted = formatter.format(event);
    doWrite(formatted);
  }

  protected abstract void doWrite(Object event);

  @VisibleForTesting
  Formatter getFormatter() {
    return formatter;
  }
}
