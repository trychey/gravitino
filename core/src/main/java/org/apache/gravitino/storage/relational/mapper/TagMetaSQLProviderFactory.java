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
package org.apache.gravitino.storage.relational.mapper;

import static org.apache.gravitino.storage.relational.mapper.TagMetaMapper.TAG_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TagMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, TagMetaBaseSQLProvider> METALAKE_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new TagMetaMySQLProvider(),
          JDBCBackendType.H2, new TagMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new TagMetaPostgreSQLProvider());

  public static TagMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TagMetaMySQLProvider extends TagMetaBaseSQLProvider {}

  static class TagMetaH2Provider extends TagMetaBaseSQLProvider {}

  static class TagMetaPostgreSQLProvider extends TagMetaBaseSQLProvider {

    @Override
    public String softDeleteTagMetaByMetalakeAndTagName(String metalakeName, String tagName) {
      return "UPDATE "
          + TAG_TABLE_NAME
          + " tm SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE tm.metalake_id IN ("
          + " SELECT mm.metalake_id FROM "
          + MetalakeMetaMapper.TABLE_NAME
          + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
          + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0";
    }

    @Override
    public String softDeleteTagMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TAG_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String insertTagMetaOnDuplicateKeyUpdate(TagPO tagPO) {
      return "INSERT INTO "
          + TAG_TABLE_NAME
          + "(tag_id, tag_name,"
          + " metalake_id, tag_comment, properties, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{tagMeta.tagId},"
          + " #{tagMeta.tagName},"
          + " #{tagMeta.metalakeId},"
          + " #{tagMeta.comment},"
          + " #{tagMeta.properties},"
          + " #{tagMeta.auditInfo},"
          + " #{tagMeta.currentVersion},"
          + " #{tagMeta.lastVersion},"
          + " #{tagMeta.deletedAt}"
          + " )"
          + " ON CONFLICT(tag_id) DO UPDATE SET"
          + " tag_name = #{tagMeta.tagName},"
          + " metalake_id = #{tagMeta.metalakeId},"
          + " tag_comment = #{tagMeta.comment},"
          + " properties = #{tagMeta.properties},"
          + " audit_info = #{tagMeta.auditInfo},"
          + " current_version = #{tagMeta.currentVersion},"
          + " last_version = #{tagMeta.lastVersion},"
          + " deleted_at = #{tagMeta.deletedAt}";
    }

    public String updateTagMeta(
        @Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO) {
      return "UPDATE "
          + TAG_TABLE_NAME
          + " SET tag_name = #{newTagMeta.tagName},"
          + " tag_comment = #{newTagMeta.comment},"
          + " properties = #{newTagMeta.properties},"
          + " audit_info = #{newTagMeta.auditInfo},"
          + " current_version = #{newTagMeta.currentVersion},"
          + " last_version = #{newTagMeta.lastVersion},"
          + " deleted_at = #{newTagMeta.deletedAt}"
          + " WHERE tag_id = #{oldTagMeta.tagId}"
          + " AND metalake_id = #{oldTagMeta.metalakeId}"
          + " AND tag_name = #{oldTagMeta.tagName}"
          + " AND (tag_comment = #{oldTagMeta.comment} "
          + "   OR (CAST(tag_comment AS VARCHAR) IS NULL AND CAST(#{oldTagMeta.comment} AS VARCHAR) IS NULL))"
          + " AND properties = #{oldTagMeta.properties}"
          + " AND audit_info = #{oldTagMeta.auditInfo}"
          + " AND current_version = #{oldTagMeta.currentVersion}"
          + " AND last_version = #{oldTagMeta.lastVersion}"
          + " AND deleted_at = 0";
    }
  }

  public static String listTagPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listTagPOsByMetalake(metalakeName);
  }

  public static String listTagPOsByMetalakeAndTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames) {
    return getProvider().listTagPOsByMetalakeAndTagNames(metalakeName, tagNames);
  }

  public static String selectTagIdByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().selectTagIdByMetalakeAndName(metalakeName, tagName);
  }

  public static String selectTagMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().selectTagMetaByMetalakeAndName(metalakeName, tagName);
  }

  public static String insertTagMeta(@Param("tagMeta") TagPO tagPO) {
    return getProvider().insertTagMeta(tagPO);
  }

  public static String insertTagMetaOnDuplicateKeyUpdate(@Param("tagMeta") TagPO tagPO) {
    return getProvider().insertTagMetaOnDuplicateKeyUpdate(tagPO);
  }

  public static String updateTagMeta(
      @Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO) {
    return getProvider().updateTagMeta(newTagPO, oldTagPO);
  }

  public static String softDeleteTagMetaByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().softDeleteTagMetaByMetalakeAndTagName(metalakeName, tagName);
  }

  public static String softDeleteTagMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTagMetasByMetalakeId(metalakeId);
  }

  public static String deleteTagMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTagMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
