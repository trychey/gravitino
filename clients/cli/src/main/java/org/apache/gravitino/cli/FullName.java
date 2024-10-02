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

package org.apache.gravitino.cli;

import org.apache.commons.cli.CommandLine;

/**
 * Extracts different arts of a full name (dot seperated) from the command line input. This includes
 * metalake, catalog, schema, and table names.
 */
public class FullName {
  CommandLine line;

  /**
   * Constructor for the {@code FullName} class.
   *
   * @param line The parsed command line arguments.
   */
  public FullName(CommandLine line) {
    this.line = line;
  }

  /**
   * Retrieves the metalake name from the command line options, environment variables, or the first
   * part of the full name.
   *
   * @return The metalake name, or null if not found.
   */
  public String getMetalakeName() {
    String metalakeEnv = System.getenv("GRAVITINO_METALAKE");

    // Check if the metalake name is specified as a command line option
    if (line.hasOption(GravitinoOptions.METALAKE)) {
      return line.getOptionValue(GravitinoOptions.METALAKE);
      // Check if the metalake name is set as an environment variable
    } else if (metalakeEnv != null) {
      return metalakeEnv;
      // Extract the metalake name from the full name option
    } else if (line.hasOption(GravitinoOptions.NAME)) {
      return line.getOptionValue(GravitinoOptions.NAME).split("\\.")[0];
    }

    return null;
  }

  /**
   * Retrieves the catalog name from the command line or the second part of the full name option.
   *
   * @return The catalog name, or null if not found.
   */
  public String getCatalogName() {
    return getNamePart(GravitinoOptions.CATALOG, 1);
  }

  /**
   * Retrieves the schema name from the command line or the third part of the full name option.
   *
   * @return The schema name, or null if not found.
   */
  public String getSchemaName() {
    return getNamePart(GravitinoOptions.SCHEMA, 2);
  }

  /**
   * Retrieves the table name from the command line or the fourth part of the full name option.
   *
   * @return The table name, or null if not found.
   */
  public String getTableName() {
    return getNamePart(GravitinoOptions.TABLE, 3);
  }

  /**
   * Helper method to retrieve a specific part of the full name based on the position of the part.
   *
   * @param entity The part of the name to obtain.
   * @param position The position of the name part in the full name string.
   * @return The extracted part of the name, or {@code null} if the name part is missing or
   *     malformed.
   */
  public String getNamePart(String entity, int position) {
    /* Check if the name is specified as a command line option. */
    if (line.hasOption(entity)) {
      return line.getOptionValue(entity);
      /* Extract the name part from the full name if available. */
    } else if (line.hasOption(GravitinoOptions.NAME)) {
      String[] names = line.getOptionValue(GravitinoOptions.NAME).split("\\.");

      /* Adjust position if metalake is part of the full name. */
      String metalakeEnv = System.getenv("GRAVITINO_METALAKE");
      if (metalakeEnv != null) {
        position = position - 1;
      }

      if (names.length < position) {
        System.err.println(ErrorMessages.MALFORMED_NAME);
        return null;
      }

      return names[position];
    }

    System.err.println(ErrorMessages.MISSING_NAME);
    return null;
  }
}
