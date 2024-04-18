/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.meta.UserEntity;
import com.google.common.collect.Lists;
import java.util.Collection;

public class UserEntitySerDe implements ProtoSerDe<UserEntity, User> {

  @Override
  public User serialize(UserEntity userEntity) {
    User.Builder builder =
        User.newBuilder()
            .setId(userEntity.id())
            .setName(userEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(userEntity.auditInfo()));

    if (isCollectionNotEmpty(userEntity.roles())) {
      builder.addAllRoles(userEntity.roles());
    }

    return builder.build();
  }

  @Override
  public UserEntity deserialize(User user, Namespace namespace) {
    UserEntity.Builder builder =
        UserEntity.builder()
            .withId(user.getId())
            .withName(user.getName())
            .withNamespace(namespace)
            .withAuditInfo(new AuditInfoSerDe().deserialize(user.getAuditInfo(), namespace))
            .withRoles(Lists.newArrayList());

    if (user.getRolesCount() > 0) {
      builder.withRoles(user.getRolesList());
    }

    return builder.build();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
