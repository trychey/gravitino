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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerPrivileges.RangerHivePrivilege;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.gravitino.exceptions.AuthorizationPluginException;

public class RangerAuthorizationHivePlugin extends RangerAuthorizationPlugin {
  private static volatile RangerAuthorizationHivePlugin instance = null;

  private RangerAuthorizationHivePlugin(Map<String, String> config) {
    super(config);
  }

  public static synchronized RangerAuthorizationHivePlugin getInstance(Map<String, String> config) {
    if (instance == null) {
      synchronized (RangerAuthorizationHivePlugin.class) {
        if (instance == null) {
          instance = new RangerAuthorizationHivePlugin(config);
        }
      }
    }
    return instance;
  }

  @Override
  /** Set the default mapping Gravitino privilege name to the Ranger rule */
  public Map<Privilege.Name, Set<RangerPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerHivePrivilege.UPDATE, RangerHivePrivilege.ALTER, RangerHivePrivilege.WRITE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(RangerHivePrivilege.READ, RangerHivePrivilege.SELECT));
  }

  @Override
  /** Set the default owner rule. */
  public Set<RangerPrivilege> ownerMappingRule() {
    return ImmutableSet.of(RangerHivePrivilege.ALL);
  }

  @Override
  /** Set Ranger policy resource rule. */
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(
        PolicyResource.DATABASE.getName(),
        PolicyResource.TABLE.getName(),
        PolicyResource.COLUMN.getName());
  }

  @Override
  /** Allow privilege operation defines rule. */
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_CATALOG,
        Privilege.Name.CREATE_SCHEMA,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.MODIFY_TABLE,
        Privilege.Name.SELECT_TABLE);
  }

  /** Translate the Gravitino securable object to the Ranger owner securable object. */
  public List<RangerSecurableObject> translateOwner(MetadataObject metadataObject) {
    List<RangerSecurableObject> rangerSecurableObjects = new ArrayList<>();

    switch (metadataObject.type()) {
      case METALAKE:
      case CATALOG:
        // Add `*` for the SCHEMA permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                ImmutableList.of(RangerHelper.RESOURCE_STAR),
                MetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `*.*` for the TABLE permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                ImmutableList.of(RangerHelper.RESOURCE_STAR, RangerHelper.RESOURCE_STAR),
                MetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `*.*.*` for the COLUMN permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                ImmutableList.of(
                    RangerHelper.RESOURCE_STAR,
                    RangerHelper.RESOURCE_STAR,
                    RangerHelper.RESOURCE_STAR),
                MetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case SCHEMA:
        // Add `{schema}` for the SCHEMA permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                ImmutableList.of(metadataObject.name() /*Schema name*/),
                MetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `{schema}.*` for the TABLE permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                ImmutableList.of(metadataObject.name() /*Schema name*/, RangerHelper.RESOURCE_STAR),
                MetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.*.*` for the COLUMN permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                ImmutableList.of(
                    metadataObject.name() /*Schema name*/,
                    RangerHelper.RESOURCE_STAR,
                    RangerHelper.RESOURCE_STAR),
                MetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case TABLE:
        // Add `{schema}.{table}` for the TABLE permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                convertToRangerMetadataObject(metadataObject),
                MetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.{table}.*` for the COLUMN permission
        rangerSecurableObjects.add(
            RangerSecurableObjects.of(
                Stream.concat(
                        convertToRangerMetadataObject(metadataObject).stream(),
                        Stream.of(RangerHelper.RESOURCE_STAR))
                    .collect(Collectors.toList()),
                MetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      default:
        throw new AuthorizationPluginException(
            "The owner privilege is not supported for the securable object: %s",
            metadataObject.type());
    }

    return rangerSecurableObjects;
  }

  /** Translate the Gravitino securable object to the Ranger securable object. */
  public List<RangerSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<RangerSecurableObject> rangerSecurableObjects = new ArrayList<>();

    securableObject.privileges().stream()
        .filter(Objects::nonNull)
        .forEach(
            privilege -> {
              Set<RangerPrivilege> rangerPrivileges = new HashSet<>();
              privilegesMappingRule().get(privilege.name()).stream()
                  .forEach(
                      rangerPrivilege ->
                          rangerPrivileges.add(
                              new RangerPrivileges.RangerHivePrivilegeImpl(
                                  rangerPrivilege, privilege.condition())));

              switch (privilege.name()) {
                case CREATE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      rangerSecurableObjects.add(
                          RangerSecurableObjects.of(
                              ImmutableList.of(RangerHelper.RESOURCE_STAR),
                              MetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          privilege.name(), securableObject.type());
                  }
                  break;
                case CREATE_TABLE:
                case MODIFY_TABLE:
                case SELECT_TABLE:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add `*.*` for the TABLE permission
                      rangerSecurableObjects.add(
                          RangerSecurableObjects.of(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_STAR, RangerHelper.RESOURCE_STAR),
                              MetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `*.*.*` for the COLUMN permission
                      rangerSecurableObjects.add(
                          RangerSecurableObjects.of(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_STAR,
                                  RangerHelper.RESOURCE_STAR,
                                  RangerHelper.RESOURCE_STAR),
                              MetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add `{schema}.*` for the TABLE permission
                      rangerSecurableObjects.add(
                          RangerSecurableObjects.of(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_STAR),
                              MetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `{schema}.*.*` for the COLUMN permission
                      rangerSecurableObjects.add(
                          RangerSecurableObjects.of(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_STAR,
                                  RangerHelper.RESOURCE_STAR),
                              MetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case TABLE:
                      if (privilege.name() == Privilege.Name.CREATE_TABLE) {
                        throw new AuthorizationPluginException(
                            "The privilege %s is not supported for the securable object: %s",
                            privilege.name(), securableObject.type());
                      } else {
                        // Add `{schema}.{table}` for the TABLE permission
                        rangerSecurableObjects.add(
                            RangerSecurableObjects.of(
                                convertToRangerMetadataObject(securableObject),
                                MetadataObject.Type.TABLE,
                                rangerPrivileges));
                        // Add `{schema}.{table}.*` for the COLUMN permission
                        rangerSecurableObjects.add(
                            RangerSecurableObjects.of(
                                Stream.concat(
                                        convertToRangerMetadataObject(securableObject).stream(),
                                        Stream.of(RangerHelper.RESOURCE_STAR))
                                    .collect(Collectors.toList()),
                                MetadataObject.Type.COLUMN,
                                rangerPrivileges));
                      }
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          privilege.name(), securableObject.type());
                  }
                  break;
                default:
                  throw new AuthorizationPluginException(
                      "The privilege %s is not supported for the securable object: %s",
                      privilege.name(), securableObject.type());
              }
            });

    return rangerSecurableObjects;
  }

  /**
   * Because the Ranger securable object is different from the Gravitino securable object, we need
   * to convert the Gravitino securable object to the Ranger securable object.
   */
  List<String> convertToRangerMetadataObject(MetadataObject metadataObject) {
    Preconditions.checkArgument(
        !(metadataObject instanceof RangerPrivileges),
        "The metadata object must be not a RangerPrivileges object.");
    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    nsMetadataObject.remove(0); // remove the catalog name
    return nsMetadataObject;
  }
}
