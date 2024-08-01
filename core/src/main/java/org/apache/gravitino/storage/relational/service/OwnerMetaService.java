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
package org.apache.gravitino.storage.relational.service;

import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** This class is an utilization class to retrieve owner relation. */
public class OwnerMetaService {

  private OwnerMetaService() {}

  private static final OwnerMetaService INSTANCE = new OwnerMetaService();

  public static OwnerMetaService getInstance() {
    return INSTANCE;
  }

  public Entity getOwner(NameIdentifier identifier, Entity.EntityType type) {
    long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(getMetalake(identifier));
    Long finalEntityId = getEntityId(metalakeId, identifier, type);

    OwnerRelPO ownerRelPO =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper -> mapper.selectOwnerMetaByEntityIdAndType(finalEntityId));

    if (ownerRelPO == null) {
      throw new NoSuchEntityException("Owner %s type %s doesn't exist", identifier.name(), type);
    }

    if (ownerRelPO.getOwnerType().equals(Entity.EntityType.USER.name())) {
      return UserMetaService.getInstance()
          .getUserById(getMetalake(identifier), ownerRelPO.getOwnerId());
    } else if (ownerRelPO.getOwnerType().equals(Entity.EntityType.GROUP.name())) {
      return GroupMetaService.getInstance()
          .getGroupById(getMetalake(identifier), ownerRelPO.getOwnerId());
    } else {
      throw new IllegalArgumentException(
          String.format("Owner type doesn't support %s", ownerRelPO.getOwnerType()));
    }
  }

  public void setOwner(
      NameIdentifier entity,
      Entity.EntityType entityType,
      NameIdentifier owner,
      Entity.EntityType ownerType) {
    long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(getMetalake(entity));

    Long entityId = getEntityId(metalakeId, entity, entityType);
    Long ownerId = getEntityId(metalakeId, owner, ownerType);

    OwnerRelPO ownerRelPO =
        POConverters.initializeOwnerRelPOsWithVersion(
            metalakeId, ownerType.name(), ownerId, entityType.name(), entityId);
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.softDeleteOwnerRelByEntityId(entityId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.insertOwnerEntityRel(ownerRelPO)));
  }

  private static long getEntityId(
      long metalakeId, NameIdentifier identifier, Entity.EntityType type) {
    long entityId;
    if (type == Entity.EntityType.USER) {
      entityId =
          UserMetaService.getInstance().getUserIdByMetalakeIdAndName(metalakeId, identifier.name());

    } else if (type == Entity.EntityType.GROUP) {
      entityId =
          GroupMetaService.getInstance()
              .getGroupIdByMetalakeIdAndName(metalakeId, identifier.name());

    } else if (type == Entity.EntityType.ROLE) {
      entityId =
          RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, identifier.name());
    } else {
      MetadataObject object = NameIdentifierUtil.toMetadataObject(identifier, type);
      entityId =
          MetadataObjectService.getMetadataObjectId(metalakeId, object.fullName(), object.type());
    }
    return entityId;
  }

  private static String getMetalake(NameIdentifier identifier) {
    if (identifier.hasNamespace()) {
      return identifier.namespace().level(0);
    } else {
      return identifier.name();
    }
  }
}
