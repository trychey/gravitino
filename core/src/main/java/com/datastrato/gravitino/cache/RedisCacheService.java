/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache;

import com.beust.jcommander.internal.Lists;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.UnsupportedEntityTypeException;
import com.datastrato.gravitino.cipher.CipherUtils;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisCacheService implements CacheService {
  private static final Logger LOG = LoggerFactory.getLogger(RedisCacheService.class);
  private static final int timeout = 2000;
  private static final int maxTotal = 50;
  private static final int maxIdle = 20;
  private static final int minIdle = 10;
  private static final int maxWaitMillis = 1000;
  private static final int minEvictIdleConnectionMills = 30000;
  private static final int timeBetweenEvictionRunsMillis = 30000;
  @VisibleForTesting JedisPool jedisPool;

  public RedisCacheService(Config config) {
    String redisHost = config.get(Configs.SERVER_REDIS_CACHE_HOST);
    Preconditions.checkArgument(StringUtils.isNotBlank(redisHost), "Redis host is not set");

    String redisPortString = config.get(Configs.SERVER_REDIS_CACHE_PORT);
    Preconditions.checkArgument(StringUtils.isNotBlank(redisPortString), "Redis port is not set");
    int redisPort = Integer.parseInt(redisPortString);

    String encryptedRedisPassword = config.get(Configs.SERVER_REDIS_CACHE_PASSWORD);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(encryptedRedisPassword), "Redis password is not set");
    String redisPassword = CipherUtils.decryptStringWithoutCompress(encryptedRedisPassword);

    // All parameters is list at
    // https://commons.apache.org/proper/commons-pool/apidocs/src-html/org/apache/commons/pool2/impl/BaseObjectPoolConfig.html
    GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
    // The maximum number of connections that are supported by the pool
    jedisPoolConfig.setMaxTotal(maxTotal);
    // The maximum number of idle connections in the pool
    jedisPoolConfig.setMaxIdle(maxIdle);
    // The minimum number of idle connections in the pool
    jedisPoolConfig.setMinIdle(minIdle);
    // Specifies whether the client must wait when the resource pool is exhausted.
    // The following maxWaitMillis parameter takes effect only when this parameter is set to true
    jedisPoolConfig.setBlockWhenExhausted(true);
    // The maximum number of milliseconds that the client needs to wait
    // when no connection is available
    // default is -1, specifies that the connection will never time out.
    jedisPoolConfig.setMaxWait(Duration.ofMillis(maxWaitMillis));
    // Specifies whether to enable JMX monitoring
    // jedisPoolConfig.setJmxEnabled(true);
    // Specifies whether to enable the idle resource detection
    jedisPoolConfig.setTestWhileIdle(true);
    // Specifies the cycle of idle resources detection
    // default is -1 that specifies that no idle resource are detected
    jedisPoolConfig.setMinEvictableIdleDuration(Duration.ofMillis(minEvictIdleConnectionMills));
    // The minimum idle time of a resource in the resource pool. When the upper limit is reached,
    // the idle resource will be evicted
    jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(timeBetweenEvictionRunsMillis));
    // The number of resources to be detected at each idle resource detection
    jedisPoolConfig.setNumTestsPerEvictionRun(10);

    // The timeout parameter specifies both the connection timeout and the read/write timeout.
    this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout, redisPassword);
  }

  @Override
  public void insert(NameIdentifier identifier, Entity e) {
    try (Jedis jedis = jedisPool.getResource()) {
      String key = generateKey(e.type(), identifier);
      String value = JsonUtils.anyFieldMapper().writeValueAsString(e);
      // Set the expiration time to 15 minutes
      jedis.setex(key, 900, value);
    } catch (Exception ex) {
      throw new GravitinoRuntimeException(
          ex, "Failed to insert the entity: %s into the redis cache.", identifier);
    }
  }

  @Override
  public long delete(Entity.EntityType type, NameIdentifier identifier) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (type != Entity.EntityType.METALAKE) {
        String key = generateKey(type, identifier);
        long deletedKeysCount = jedis.del(key);
        LOG.info(
            "Deleted {} keys with prefixes from the redis cache: {}.",
            deletedKeysCount,
            identifier);
        return deletedKeysCount;
      } else {
        String deleteScript = deleteRecursivelyLuaScript();
        List<String> deleteKeys = Lists.newArrayList(Entity.EntityType.METALAKE + ":" + identifier);
        long deletedKeysCount = (Long) jedis.eval(deleteScript, deleteKeys, Lists.newArrayList());
        LOG.info(
            "Deleted {} keys with prefixes from the redis cache: {}.",
            deletedKeysCount,
            identifier);
        return deletedKeysCount;
      }
    } catch (Exception ex) {
      throw new GravitinoRuntimeException(
          ex, "Failed to delete the entity: %s from the redis cache.", identifier);
    }
  }

  @Override
  public long deleteRecursively(Entity.EntityType type, NameIdentifier identifier) {
    try (Jedis jedis = jedisPool.getResource()) {
      List<String> deleteKeys = Lists.newArrayList();
      String deleteScript = deleteRecursivelyLuaScript();
      switch (type) {
        case METALAKE:
          deleteKeys.add(generateKey(Entity.EntityType.METALAKE, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.CATALOG, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.SCHEMA, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.TABLE, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.FILESET, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.TOPIC, identifier));
          break;
        case CATALOG:
          deleteKeys.add(generateKey(Entity.EntityType.CATALOG, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.SCHEMA, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.TABLE, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.FILESET, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.TOPIC, identifier));
          break;
        case SCHEMA:
          deleteKeys.add(generateKey(Entity.EntityType.SCHEMA, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.TABLE, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.FILESET, identifier));
          deleteKeys.add(generateKey(Entity.EntityType.TOPIC, identifier));
          break;
        case TABLE:
          deleteKeys.add(generateKey(Entity.EntityType.TABLE, identifier));
          break;
        case FILESET:
          deleteKeys.add(generateKey(Entity.EntityType.FILESET, identifier));
          break;
        case TOPIC:
          deleteKeys.add(generateKey(Entity.EntityType.TOPIC, identifier));
          break;
        default:
          throw new UnsupportedEntityTypeException(
              "Unsupported entity type: %s for deleting the redis cache recursively.", type);
      }
      long deletedKeysCount = (Long) jedis.eval(deleteScript, deleteKeys, Lists.newArrayList());
      LOG.info(
          "Deleted {} keys with prefixes from the redis cache: {}.", deletedKeysCount, deleteKeys);
      return deletedKeysCount;
    } catch (Exception ex) {
      throw new GravitinoRuntimeException(
          ex, "Failed to delete the entity: %s from the redis cache recursively.", identifier);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      Entity.EntityType type, NameIdentifier identifier) {
    try (Jedis jedis = jedisPool.getResource()) {
      String key = generateKey(type, identifier);
      String value = jedis.get(key);
      if (value == null) {
        return null;
      }
      switch (type) {
        case METALAKE:
          return (E) JsonUtils.anyFieldMapper().readValue(value, BaseMetalake.class);
        case CATALOG:
          return (E) JsonUtils.anyFieldMapper().readValue(value, CatalogEntity.class);
        case SCHEMA:
          return (E) JsonUtils.anyFieldMapper().readValue(value, SchemaEntity.class);
        case TABLE:
          return (E) JsonUtils.anyFieldMapper().readValue(value, TableEntity.class);
        case FILESET:
          return (E) JsonUtils.anyFieldMapper().readValue(value, FilesetEntity.class);
        case TOPIC:
          return (E) JsonUtils.anyFieldMapper().readValue(value, TopicEntity.class);
        default:
          throw new UnsupportedEntityTypeException(
              "Unsupported entity type: %s for getting from the redis cache.", type);
      }
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          e, "Failed to get the entity: %s from the redis cache.", identifier);
    }
  }

  private static String generateKey(Entity.EntityType type, NameIdentifier identifier) {
    return type.toString() + ":" + identifier.toString();
  }

  @VisibleForTesting
  String deleteRecursivelyLuaScript() {
    return "redis.replicate_commands()\n"
        + "local deletedKeysCount = 0\n"
        + "-- Traverse all keys with the given prefixes\n"
        + "for i, prefix in ipairs(KEYS) do\n"
        + "    local cursor = '0'\n"
        + "    repeat\n"
        + "        -- Scan keys with the given prefix\n"
        + "        local scanResult = redis.call('SCAN', cursor, 'MATCH', prefix .. '*', 'COUNT', 100)\n"
        + "        cursor = scanResult[1]\n"
        + "        local keys = scanResult[2]\n"
        + "        -- Delete matching keys\n"
        + "        for j, key in ipairs(keys) do\n"
        + "            redis.call('DEL', key)\n"
        + "            deletedKeysCount = deletedKeysCount + 1\n"
        + "        end\n"
        + "    until cursor == '0'\n"
        + "end\n"
        + "return deletedKeysCount";
  }

  @Override
  public synchronized void close() throws IOException {
    jedisPool.close();
  }
}
