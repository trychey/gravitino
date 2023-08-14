/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// zeppelin-integration/src/test/java/org/apache/zeppelin/CommandExecutor.java
public class CommandExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

  public enum IGNORE_ERRORS {
    TRUE,
    FALSE
  }

  public static int NORMAL_EXIT = 0;

  private static IGNORE_ERRORS DEFAULT_BEHAVIOUR_ON_ERRORS = IGNORE_ERRORS.TRUE;

  public static Object executeCommandLocalHost(
      String[] command,
      boolean printToConsole,
      ProcessData.TypesOfData type,
      IGNORE_ERRORS ignore_errors) {
    List<String> subCommandsAsList = new ArrayList<>(Arrays.asList(command));
    String mergedCommand = StringUtils.join(subCommandsAsList, " ");

    LOG.info("Sending command \"" + mergedCommand + "\" to localhost");

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    Process process = null;
    try {
      process = processBuilder.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ProcessData dataOfProcess = new ProcessData(process, printToConsole);
    Object outputOfProcess = dataOfProcess.getData(type);
    int exit_code = dataOfProcess.getExitCodeValue();

    if (!printToConsole) LOG.trace(outputOfProcess.toString());
    else LOG.debug(outputOfProcess.toString());
    if (ignore_errors == IGNORE_ERRORS.FALSE && exit_code != NORMAL_EXIT) {
      LOG.error(String.format("Command '%s' failed with exit code %s", mergedCommand, exit_code));
    }
    return outputOfProcess;
  }

  public static Object executeCommandLocalHost(
      String command, boolean printToConsole, ProcessData.TypesOfData type) {
    return executeCommandLocalHost(
        new String[] {"bash", "-c", command}, printToConsole, type, DEFAULT_BEHAVIOUR_ON_ERRORS);
  }
}
