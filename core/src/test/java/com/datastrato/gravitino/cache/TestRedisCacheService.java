/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache;

import com.beust.jcommander.internal.Lists;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestRedisCacheService {
  @Test
  public void testInsert() throws JsonProcessingException {
    RedisCacheService mockService = Mockito.mock(RedisCacheService.class);
    JedisPool mockJedisPool = Mockito.mock(JedisPool.class);
    Jedis mockJedis = Mockito.mock(Jedis.class);
    mockService.jedisPool = mockJedisPool;
    Mockito.when(mockJedisPool.getResource()).thenReturn(mockJedis);
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withName("test")
            .withComment("")
            .withId(1L)
            .withProperties(Maps.newHashMap())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    NameIdentifier identifier = NameIdentifier.of("test");
    String key = Entity.EntityType.METALAKE + ":" + identifier;
    String value = JsonUtils.anyFieldMapper().writeValueAsString(metalake);
    Mockito.when(mockJedis.setex(key, 900, value))
        .thenThrow(new GravitinoRuntimeException("throw an exception"));
    Mockito.doCallRealMethod().when(mockService).insert(Mockito.any(), Mockito.any());
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> mockService.insert(identifier, metalake));
  }

  @Test
  public void testGet() throws JsonProcessingException {
    RedisCacheService mockService = Mockito.mock(RedisCacheService.class);
    JedisPool mockJedisPool = Mockito.mock(JedisPool.class);
    Jedis mockJedis = Mockito.mock(Jedis.class);
    mockService.jedisPool = mockJedisPool;
    Mockito.when(mockJedisPool.getResource()).thenReturn(mockJedis);
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withName("test")
            .withComment("")
            .withId(1L)
            .withProperties(Maps.newHashMap())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    NameIdentifier identifier = NameIdentifier.of("test");
    String key = Entity.EntityType.METALAKE + ":" + identifier;

    Mockito.when(mockJedis.get(key))
        .thenReturn(JsonUtils.anyFieldMapper().writeValueAsString(metalake));

    Mockito.doCallRealMethod().when(mockService).get(Mockito.any(), Mockito.any());
    Metalake metalakeEntity = mockService.get(Entity.EntityType.METALAKE, identifier);
    Assertions.assertEquals(metalake.name(), metalakeEntity.name());
    Assertions.assertEquals(metalake.comment(), metalakeEntity.comment());
    Assertions.assertEquals(metalake.properties(), metalakeEntity.properties());
  }

  @Test
  public void testDelete() {
    RedisCacheService mockService = Mockito.mock(RedisCacheService.class);
    JedisPool mockJedisPool = Mockito.mock(JedisPool.class);
    Jedis mockJedis = Mockito.mock(Jedis.class);
    mockService.jedisPool = mockJedisPool;
    Mockito.when(mockJedisPool.getResource()).thenReturn(mockJedis);

    NameIdentifier identifier = NameIdentifier.of("test");
    String key = Entity.EntityType.METALAKE + ":" + identifier;
    Mockito.when(
            mockJedis.eval(
                mockService.deleteRecursivelyLuaScript(),
                Lists.newArrayList(key),
                Lists.newArrayList()))
        .thenReturn(1L);

    Mockito.doCallRealMethod().when(mockService).delete(Mockito.any(), Mockito.any());
    long deleteCount = mockService.delete(Entity.EntityType.METALAKE, identifier);
    Assertions.assertEquals(1, deleteCount);
  }

  @Test
  public void testDeleteRecursively() {
    RedisCacheService mockService = Mockito.mock(RedisCacheService.class);
    JedisPool mockJedisPool = Mockito.mock(JedisPool.class);
    Jedis mockJedis = Mockito.mock(Jedis.class);
    mockService.jedisPool = mockJedisPool;
    Mockito.when(mockJedisPool.getResource()).thenReturn(mockJedis);

    NameIdentifier identifier = NameIdentifier.of("test");
    String metalakeKey = Entity.EntityType.METALAKE + ":" + identifier;
    String catalogKey = Entity.EntityType.CATALOG + ":" + identifier;
    String schemaKey = Entity.EntityType.SCHEMA + ":" + identifier;
    String tableKey = Entity.EntityType.TABLE + ":" + identifier;
    String filesetKey = Entity.EntityType.FILESET + ":" + identifier;
    String topicKey = Entity.EntityType.TOPIC + ":" + identifier;
    Mockito.when(
            mockJedis.eval(
                mockService.deleteRecursivelyLuaScript(),
                Lists.newArrayList(
                    metalakeKey, catalogKey, schemaKey, tableKey, filesetKey, topicKey),
                Lists.newArrayList()))
        .thenReturn(1L);

    Mockito.doCallRealMethod().when(mockService).deleteRecursively(Mockito.any(), Mockito.any());
    long deleteCount = mockService.deleteRecursively(Entity.EntityType.METALAKE, identifier);
    Assertions.assertEquals(1, deleteCount);
  }
}
