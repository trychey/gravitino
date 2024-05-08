/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.gc.utils;

import static com.datastrato.gravitino.shaded.com.google.common.base.Preconditions.checkNotNull;

import com.datastrato.gravitino.gc.constants.CliConstants;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CliParser {
  private String gravitinoServerUri;
  private String gravitinoMetalake;
  private String filesetCatalog;
  private String date;
  private String principalFilePath;
  private Boolean skipTrash;
  private String superUser;

  public CliParser(String[] args) {
    Options options = new Options();
    options.addOption(CliConstants.GRAVITINO_SERVER_URI, true, "gravitino server uri");
    options.addOption(CliConstants.GRAVITINO_METALAKE, true, "gravitino metalake");
    options.addOption(CliConstants.FILESET_CATALOG, true, "gravitino fileset catalog");
    options.addOption(CliConstants.DATE, true, "date");
    options.addOption(CliConstants.PRINCIPAL_FILE_PATH, true, "principal file path");
    options.addOption(CliConstants.SKIP_TRASH, true, "skip clean files to trash");
    options.addOption(CliConstants.SUPER_USER, true, "super user information");
    try {
      BasicParser basicParser = new BasicParser();
      CommandLine commandLine = basicParser.parse(options, args);
      this.gravitinoServerUri =
          checkNotNull(
              commandLine.getOptionValue(CliConstants.GRAVITINO_SERVER_URI),
              "Gravitino server uri should not be null.");
      this.gravitinoMetalake =
          checkNotNull(
              commandLine.getOptionValue(CliConstants.GRAVITINO_METALAKE),
              "Gravitino metalake should not be null.");
      this.filesetCatalog =
          checkNotNull(
              commandLine.getOptionValue(CliConstants.FILESET_CATALOG),
              "Gravitino fileset catalog should not be null.");
      this.date =
          checkNotNull(commandLine.getOptionValue(CliConstants.DATE), "Date should not be null.");
      this.principalFilePath =
          checkNotNull(
              commandLine.getOptionValue(CliConstants.PRINCIPAL_FILE_PATH),
              "Principal file path should not be null");
      this.skipTrash =
          Boolean.valueOf(commandLine.getOptionValue(CliConstants.SKIP_TRASH, "false"));
      this.superUser =
          checkNotNull(
              commandLine.getOptionValue(CliConstants.SUPER_USER), "Super user should not be null");
    } catch (ParseException e) {
      throw new RuntimeException("Failed to parse arguments, exception:", e);
    }
  }

  public String getGravitinoServerUri() {
    return gravitinoServerUri;
  }

  public String getGravitinoMetalake() {
    return gravitinoMetalake;
  }

  public String getFilesetCatalog() {
    return filesetCatalog;
  }

  public String getDate() {
    return date;
  }

  public String getPrincipalFilePath() {
    return principalFilePath;
  }

  public Boolean skipTrash() {
    return skipTrash;
  }

  public String getSuperUser() {
    return superUser;
  }
}
