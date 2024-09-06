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

import static org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper.META_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class FilesetMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, FilesetMetaBaseSQLProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new FilesetMetaMySQLProvider(),
              JDBCBackendType.H2, new FilesetMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new FilesetMetaPostgreSQLProvider());

  public static FilesetMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class FilesetMetaMySQLProvider extends FilesetMetaBaseSQLProvider {}

  static class FilesetMetaH2Provider extends FilesetMetaBaseSQLProvider {}

  static class FilesetMetaPostgreSQLProvider extends FilesetMetaBaseSQLProvider {

    @Override
    public String softDeleteFilesetMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteFilesetMetasByCatalogId(Long catalogId) {
      return "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteFilesetMetasBySchemaId(Long schemaId) {
      return "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteFilesetMetasByFilesetId(Long filesetId) {
      return "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE fileset_id = #{filesetId} AND deleted_at = 0";
    }

    @Override
    public String insertFilesetMetaOnDuplicateKeyUpdate(FilesetPO filesetPO) {
      return "INSERT INTO "
          + META_TABLE_NAME
          + "(fileset_id, fileset_name, metalake_id,"
          + " catalog_id, schema_id, type, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{filesetMeta.filesetId},"
          + " #{filesetMeta.filesetName},"
          + " #{filesetMeta.metalakeId},"
          + " #{filesetMeta.catalogId},"
          + " #{filesetMeta.schemaId},"
          + " #{filesetMeta.type},"
          + " #{filesetMeta.auditInfo},"
          + " #{filesetMeta.currentVersion},"
          + " #{filesetMeta.lastVersion},"
          + " #{filesetMeta.deletedAt}"
          + " )"
          + " ON CONFLICT(fileset_id) DO UPDATE SET"
          + " fileset_name = #{filesetMeta.filesetName},"
          + " metalake_id = #{filesetMeta.metalakeId},"
          + " catalog_id = #{filesetMeta.catalogId},"
          + " schema_id = #{filesetMeta.schemaId},"
          + " type = #{filesetMeta.type},"
          + " audit_info = #{filesetMeta.auditInfo},"
          + " current_version = #{filesetMeta.currentVersion},"
          + " last_version = #{filesetMeta.lastVersion},"
          + " deleted_at = #{filesetMeta.deletedAt}";
    }
  }

  public static String listFilesetPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listFilesetPOsBySchemaId(schemaId);
  }

  public static String selectFilesetIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name) {
    return getProvider().selectFilesetIdBySchemaIdAndName(schemaId, name);
  }

  public static String selectFilesetMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name) {
    return getProvider().selectFilesetMetaBySchemaIdAndName(schemaId, name);
  }

  public static String selectFilesetMetaById(@Param("filesetId") Long filesetId) {
    return getProvider().selectFilesetMetaById(filesetId);
  }

  public static String insertFilesetMeta(@Param("filesetMeta") FilesetPO filesetPO) {
    return getProvider().insertFilesetMeta(filesetPO);
  }

  public static String insertFilesetMetaOnDuplicateKeyUpdate(
      @Param("filesetMeta") FilesetPO filesetPO) {
    return getProvider().insertFilesetMetaOnDuplicateKeyUpdate(filesetPO);
  }

  public static String updateFilesetMeta(
      @Param("newFilesetMeta") FilesetPO newFilesetPO,
      @Param("oldFilesetMeta") FilesetPO oldFilesetPO) {
    return getProvider().updateFilesetMeta(newFilesetPO, oldFilesetPO);
  }

  public static String softDeleteFilesetMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteFilesetMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteFilesetMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteFilesetMetasByCatalogId(catalogId);
  }

  public static String softDeleteFilesetMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteFilesetMetasBySchemaId(schemaId);
  }

  public String softDeleteFilesetMetasByFilesetId(@Param("filesetId") Long filesetId) {
    return getProvider().softDeleteFilesetMetasByFilesetId(filesetId);
  }

  public String deleteFilesetMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteFilesetMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
