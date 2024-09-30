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

package org.apache.gravitino.audit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultFileAuditWriter is the default implementation of AuditLogWriter, which writes audit logs
 * to a file.
 */
public class DefaultFileAuditWriter implements AuditLogWriter {
  private static final Logger Log = LoggerFactory.getLogger(DefaultFileAuditWriter.class);

  private Formatter formatter;
  private Writer outWriter;
  @VisibleForTesting String fileName;

  boolean immediateFlush;

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
    fileName = properties.getOrDefault("file", Configs.AUDIT_LOG_FILE_WRITER_DEFAULT_FILE_NAME);
    immediateFlush = Boolean.parseBoolean(properties.getOrDefault("immediateFlush", "false"));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileName), "FileAuditWriter: fileName is not set in configuration.");
    try {
      OutputStream outputStream = new FileOutputStream(fileName, true);
      outWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    } catch (FileNotFoundException e) {
      throw new GravitinoRuntimeException(
          String.format("Audit log file: %s is not exists", fileName));
    }
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    String log = auditLog.toString();
    try {
      outWriter.write(log);
      if (immediateFlush) {
        outWriter.flush();
      }
    } catch (Exception e) {
      Log.warn("Failed to write audit log: {}", log, e);
    }
  }

  @Override
  public void close() {
    if (outWriter != null) {
      try {
        outWriter.close();
      } catch (Exception e) {
        Log.warn("Failed to close writer", e);
      }
    }
  }
}
