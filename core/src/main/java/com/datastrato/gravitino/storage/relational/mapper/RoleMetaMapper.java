/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import java.util.List;

import com.datastrato.gravitino.storage.relational.po.RolePO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface RoleMetaMapper {
  String ROLE_TABLE_NAME = "role_meta";
  String RELATION_TABLE_NAME = "user_role_rel";

  @Select(
      "SELECT role_id as roleId FROM "
          + ROLE_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND role_name = #{roleName}"
          + " AND deleted_at = 0")
  Long selectRoleIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String name);

  @Select(
      "SELECT ro.role_id as roleId, ro.role_name as roleName,"
              + " ro.metalake_id as metalakeId, ro.properties as properties,"
                + " ro.securable_object as securableObject, ro.privileges as privileges,"
                + " ro.audit_info as auditInfo, ro.current_version as currentVersion,"
              + " ro.last_version as lastVersion, ro.deleted_at as deletedAt"
          + " FROM "
          + ROLE_TABLE_NAME
          + " ro JOIN "
          + RELATION_TABLE_NAME
          + " re ON ro.role_id = re.role_id"
          + " WHERE re.user_id = #{userId}"
          + " AND ro.deleted_at = 0 AND re.deleted_at = 0")
  List<RolePO> listRolesByUserId(@Param("userId") Long userId);

  @Insert(
          "INSERT INTO "
                  + ROLE_TABLE_NAME
                  + "(role_id, role_name,"
                    + " metalake_id, properties,"
                    + " securable_object, privileges,"
                    + " audit_info, current_version, last_version, deleted_at)"
                  + " VALUES("
                  + " #{roleMeta.userId},"
                  + " #{roleMeta.userName},"
                  + " #{roleMeta.metalakeId},"
                  + " #{roleMeta.auditInfo},"
                  + " #{roleMeta.currentVersion},"
                  + " #{roleMeta.lastVersion},"
                  + " #{roleMeta.deletedAt}"
                  + " )")
  void insertRoleMeta(@Param("roleMeta") RolePO rolePO);

}
