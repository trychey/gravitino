/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import static com.datastrato.gravitino.secret.SecretsManager.KERBEROS_SECRET_TYPE;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.auth.DataWorkshopUser;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class KerberosSecretPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosSecretPlugin.class);
  public static final String KEYTAB_FORMAT = System.getProperty("user.dir") + "/kerberos/%s.keytab";
  public static final String TICKET_FORMAT = System.getProperty("user.dir") + "/kerberos/%s.krb5cc";
  private static final String EXPIRE_TIME_PROPERTIES = "expireTime";
  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final Config config;
  private final KerberosHelper kerberosHelper;
  private final Cache<String, KerberosSecret> kerberosSecretCache;

  public KerberosSecretPlugin(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.config = config;
    this.kerberosHelper = new KerberosHelper(config);
    this.kerberosSecretCache =
        Caffeine.newBuilder()
            .maximumSize(3000)
            .expireAfterWrite(22, TimeUnit.HOURS)
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("kerberos-secret-cleaner-%d")
                            .build())))
            .build();
  }

  public Secret getSecret(String metalake) {
    String kerberos =
        String.format(config.get(KerberosSecretConfig.KERBEROS_FORMAT), extractWorkspace());
    String ticketCachePath = String.format(TICKET_FORMAT, kerberos);

    return kerberosSecretCache.get(
        ticketCachePath,
        i -> {
          if (isTicketCacheExpired(ticketCachePath)) {
            String keytabPath = getKeytabPath(kerberos);
            kerberosHelper.kinit(kerberos, keytabPath, ticketCachePath);
          }

          HashMap<String, String> properties = Maps.newHashMap();
          LocalDateTime ticketCacheExpireTime =
              kerberosHelper.getTicketCacheExpireTime(ticketCachePath);
          properties.put(
              EXPIRE_TIME_PROPERTIES,
              String.valueOf(
                  ticketCacheExpireTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli()));

          byte[] ticketBytes;
          try {
            ticketBytes = FileUtils.readFileToByteArray(new File(ticketCachePath));
          } catch (IOException e) {
            throw new GravitinoRuntimeException(
                e, "Failed to read ticket cache %s", ticketCachePath);
          }

          return KerberosSecret.builder()
              .withName(kerberos)
              .withValue(Base64.getEncoder().encodeToString(ticketBytes))
              .withType(KERBEROS_SECRET_TYPE)
              .withProperties(properties)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreateTime(Instant.now())
                      .withCreator(PrincipalUtils.getCurrentUserName())
                      .build())
              .build();
        });
  }

  private boolean isTicketCacheExpired(String ticketCachePath) {
    try {
      LocalDateTime expireTime = kerberosHelper.getTicketCacheExpireTime(ticketCachePath);
      return LocalDateTime.now().isAfter(expireTime.minusHours(3));
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
    return true;
  }

  // Caffeine ensures the operation is atomic
  private String getKeytabPath(String kerberos) {
    File keytabFile = new File(String.format(KEYTAB_FORMAT, kerberos));
    if (!keytabFile.exists()) {
      byte[] keytab = kerberosHelper.fetchKeytab(kerberos);
      try {
        LOG.info("Writing keytab {} to file: {}", kerberos, keytabFile.getAbsolutePath());
        FileUtils.writeByteArrayToFile(keytabFile, keytab);
      } catch (IOException e) {
        throw new GravitinoRuntimeException(
            e, "Failed to write keytab to file: %s", keytabFile.getAbsolutePath());
      }
    }
    return keytabFile.getAbsolutePath();
  }

  private long extractWorkspace() {
    String currentUserName = PrincipalUtils.getCurrentUserName();
    DataWorkshopUser dataWorkshopUser = DataWorkshopUser.fromUserPrincipal(currentUserName);
    return dataWorkshopUser.workspaceId();
  }
}
