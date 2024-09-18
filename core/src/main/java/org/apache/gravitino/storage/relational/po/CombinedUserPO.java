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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Objects;

/**
 * This PO is only used for reading the data from multiple joined tables. So we don't need the inner
 * class Builder.
 */
public class CombinedUserPO extends UserPO {

  private String roleNames;
  private String roleIds;

  public String getRoleNames() {
    return roleNames;
  }

  public String getRoleIds() {
    return roleIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CombinedUserPO)) {
      return false;
    }
    CombinedUserPO combinedUserPO = (CombinedUserPO) o;
    return Objects.equal(getUserId(), combinedUserPO.getUserId())
        && Objects.equal(getUserName(), combinedUserPO.getUserName())
        && Objects.equal(getMetalakeId(), combinedUserPO.getMetalakeId())
        && Objects.equal(getAuditInfo(), combinedUserPO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), combinedUserPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), combinedUserPO.getLastVersion())
        && Objects.equal(getDeletedAt(), combinedUserPO.getDeletedAt())
        && Objects.equal(getRoleIds(), combinedUserPO.getRoleIds())
        && Objects.equal(getRoleNames(), combinedUserPO.getRoleNames());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getUserId(),
        getUserName(),
        getMetalakeId(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt(),
        getRoleIds(),
        getRoleNames());
  }
}
