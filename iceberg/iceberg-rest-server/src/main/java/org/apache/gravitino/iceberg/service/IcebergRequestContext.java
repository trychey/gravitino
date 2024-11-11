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

package org.apache.gravitino.iceberg.service;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.Getter;
import org.apache.gravitino.utils.PrincipalUtils;

/** The general request context information for Iceberg REST operations. */
public class IcebergRequestContext {

  @Getter private final String catalogName;
  @Getter private final String userName;
  @Getter private final String remoteHostName;
  @Getter private final Map<String, String> httpHeaders;

  public IcebergRequestContext(HttpServletRequest httpRequest, String catalogName) {
    this.remoteHostName = httpRequest.getRemoteHost();
    this.httpHeaders = IcebergRestUtils.getHttpHeaders(httpRequest);
    this.catalogName = catalogName;
    this.userName = PrincipalUtils.getCurrentUserName();
  }
}
