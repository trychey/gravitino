/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.UserPO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface UserMetaMapper {
  String USER_TABLE_NAME = "user_meta";
  String USER_ROLE_RELATION_TABLE_NAME = "user_role_rel";

  @Select(
      "SELECT user_id as userId FROM "
          + USER_TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND user_name = #{userName}"
          + " AND deleted_at = 0")
  Long selectUserIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("userName") String name);

  @Select(
      "SELECT user_id as userId, user_name as userName,"
          + " metalake_id as metalakeId,"
          + " catalog_id as catalogId,"
          + " schema_id as schemaId,"
          + " audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + USER_TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND user_name = #{userName}"
          + " AND deleted_at = 0")
  UserPO selectUserMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("userName") String name);

  @Insert(
      "INSERT INTO "
          + USER_TABLE_NAME
          + "(user_id, user_name,"
          + " metalake_id, catalog_id, schema_id,"
          + " audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{userMeta.userId},"
          + " #{userMeta.userName},"
          + " #{userMeta.metalakeId},"
          + " #{userMeta.catalogId},"
          + " #{userMeta.schemaId},"
          + " #{userMeta.auditInfo},"
          + " #{userMeta.currentVersion},"
          + " #{userMeta.lastVersion},"
          + " #{userMeta.deletedAt}"
          + " )")
  void insertUserMeta(@Param("userMeta") UserPO userPO);

  @Insert(
      "INSERT INTO "
          + USER_TABLE_NAME
          + "(user_id, user_name,"
          + " metalake_id, catalog_id, schema_id,"
          + " audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{userMeta.userId},"
          + " #{userMeta.userName},"
          + " #{userMeta.metalakeId},"
          + " #{userMeta.catalogId},"
          + " #{userMeta.schemaId},"
          + " #{userMeta.auditInfo},"
          + " #{userMeta.currentVersion},"
          + " #{userMeta.lastVersion},"
          + " #{userMeta.deletedAt}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " user_name = #{userMeta.userName},"
          + " metalake_id = #{userMeta.metalakeId},"
          + " catalog_id = #{userMeta.catalogId},"
          + " schema_id = #{userMeta.schemaId},"
          + " audit_info = #{userMeta.auditInfo},"
          + " current_version = #{userMeta.currentVersion},"
          + " last_version = #{userMeta.lastVersion},"
          + " deleted_at = #{userMeta.deletedAt}")
  void insertUserMetaOnDuplicateKeyUpdate(@Param("userMeta") UserPO userPO);

  @Update(
      "UPDATE "
          + USER_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE user_id = #{userId} AND deleted_at = 0")
  void softDeleteUserMetaByUserId(@Param("userId") Long userId);

  @Update(
      "UPDATE "
          + USER_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  void softDeleteUserMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + USER_TABLE_NAME
          + " SET user_name = #{newUserMeta.userName},"
          + " metalake_id = #{newUserMeta.metalakeId},"
          + " catalog_id = #{newUserMeta.catalogId},"
          + " schema_id = #{newUserMeta.schemaId},"
          + " audit_info = #{newUserMeta.auditInfo},"
          + " current_version = #{newUserMeta.currentVersion},"
          + " last_version = #{newUserMeta.lastVersion},"
          + " deleted_at = #{newUserMeta.deletedAt}"
          + " WHERE user_id = #{oldUserMeta.userId}"
          + " AND user_name = #{oldUserMeta.userName}"
          + " AND metalake_id = #{oldUserMeta.metalakeId}"
          + " AND catalog_id = #{oldUserMeta.catalogId}"
          + " AND schema_id = #{oldUserMeta.schemaId}"
          + " AND audit_info = #{oldUserMeta.auditInfo}"
          + " AND current_version = #{oldUserMeta.currentVersion}"
          + " AND last_version = #{oldUserMeta.lastVersion}"
          + " AND deleted_at = 0")
  Integer updateUserMeta(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO);

  @Select(
      "SELECT us.user_id as userId, "
          + " us.user_name as userName,"
          + " us.metalake_id as metalakeId,"
          + " us.catalog_id as catalogId,"
          + " us.schema_id as schemaId,"
          + " us.audit_info as auditInfo, us.current_version as currentVersion,"
          + " us.last_version as lastVersion, us.deleted_at as deletedAt"
          + " FROM "
          + USER_TABLE_NAME
          + " us JOIN "
          + USER_ROLE_RELATION_TABLE_NAME
          + " re ON us.user_id = re.user_id"
          + " WHERE re.role_id = #{roleId}"
          + " AND us.deleted_at = 0 AND re.deleted_at = 0")
  List<UserPO> listUsersByRoleId(@Param("roleId") Long roleId);

  @Delete(
      "DELETE FROM "
          + USER_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
