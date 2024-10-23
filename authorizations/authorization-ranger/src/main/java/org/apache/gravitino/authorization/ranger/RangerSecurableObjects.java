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
package org.apache.gravitino.authorization.ranger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.authorization.ranger.RangerMetadataObjects.RangerMetadataObjectImpl;

/** The helper class for {@link RangerSecurableObject}. */
public class RangerSecurableObjects {
  public static class RangerSecurableObjectImpl extends RangerMetadataObjectImpl
      implements RangerSecurableObject {

    private final List<RangerPrivilege> privileges;

    /**
     * Create the Ranger securable object with the given name, parent and type.
     *
     * @param parent The parent of the metadata object
     * @param name The name of the metadata object
     * @param type The type of the metadata object
     */
    public RangerSecurableObjectImpl(
        String parent,
        String name,
        RangerMetadataObject.Type type,
        Set<RangerPrivilege> privileges) {
      super(parent, name, type);
      this.privileges = ImmutableList.copyOf(Sets.newHashSet(privileges));
    }

    @Override
    public String fullName() {
      return super.fullName();
    }

    @Override
    public RangerMetadataObject.Type type() {
      return super.type();
    }

    @Override
    public List<RangerPrivilege> privileges() {
      return privileges;
    }
  }
}
