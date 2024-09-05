/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.secret;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.cipher.CipherUtils;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosHelper {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosHelper.class);

  private final Config config;
  private final CloseableHttpClient httpClient;

  public KerberosHelper(Config config) {
    this.config = config;
    this.httpClient = HttpClients.createDefault();
  }

  public byte[] fetchKeytab(String kerberos) {
    String host = config.get(KerberosSecretConfig.KERBEROS_HOST);
    String user = config.get(KerberosSecretConfig.KERBEROS_USER);
    String password =
        CipherUtils.decryptStringWithoutCompress(
            config.get(KerberosSecretConfig.KERBEROS_PASSWORD));

    String url =
        host
            + String.format(
                "/kerberos/keytab?user=%s&auth_token=%s&account=%s", user, password, kerberos);
    HttpGet httpGet = new HttpGet(url);
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        throw new GravitinoRuntimeException(
            "Failed to fetch keytab %s, status code: %s", kerberos, statusCode);
      }
      return EntityUtils.toByteArray(response.getEntity());
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to fetch keytab %s", kerberos);
    }
  }

  public void kinit(String kerberos, String keytabPath, String ticketCachePath) {
    String command = String.format("kinit -c %s -kt %s %s", ticketCachePath, keytabPath, kerberos);
    try {
      LOG.info("Executing kinit command: {}", command);
      Process process = Runtime.getRuntime().exec(command);
      process.waitFor(10, TimeUnit.SECONDS);
      if (process.exitValue() != 0) {
        throw new GravitinoRuntimeException(
            "Failed to exec command '%s', exit code: %s", command, process.exitValue());
      }
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to exec command '%s'", command);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public String klist(String ticketCachePath) {
    StringBuilder result = new StringBuilder();
    String command = String.format("klist -c %s", ticketCachePath);

    try {
      LOG.info("Executing klist command: {}", command);
      Process process = Runtime.getRuntime().exec(command);
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));

      String line;
      while ((line = reader.readLine()) != null) {
        result.append(line).append("\n");
      }
      process.waitFor(10, TimeUnit.SECONDS);
      if (process.exitValue() != 0) {
        throw new GravitinoRuntimeException(
            String.format(
                "Failed to exec command '%s', exit code: %s", command, process.exitValue()));
      }
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to exec command '%s'", command);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return result.toString();
  }

  public LocalDateTime getTicketCacheExpireTime(String ticketCachePath) {
    if (!new File(ticketCachePath).exists()) {

      // If the ticket cache file does not exist, return an expired time directly.
      return LocalDateTime.now().minusDays(1);
    }

    String klist = klist(ticketCachePath);

    // Valid starting       Expires              Service principal
    // 08/26/2024 12:00:01  08/27/2024 12:00:01  krbtgt/XIAOMI.HADOOP@XIAOMI.HADOOP
    Pattern pattern =
        Pattern.compile(
            "(?<valid>\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2})\\s+(?<expires>\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2})");

    for (String line : klist.split("\n")) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        return LocalDateTime.parse(
            matcher.group("expires"), DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
      }
    }

    // If the expiration time cannot be found, return an expired time directly.
    return LocalDateTime.now().minusDays(1);
  }
}
