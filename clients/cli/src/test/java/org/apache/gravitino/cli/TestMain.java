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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMain {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  public void setUpStreams() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void withTwoArgs() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"metalake", "details"};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertEquals(CommandActions.DETAILS, command);
    String entity = Main.resolveEntity(line);
    assertEquals(CommandEntities.METALAKE, entity);
  }

  @Test
  public void defaultToDetailsOneArg() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"metalake"};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertEquals(CommandActions.DETAILS, command);
    String entity = Main.resolveEntity(line);
    assertEquals(CommandEntities.METALAKE, entity);
  }

  @Test
  public void withNoArgs() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertNull(command);
    String entity = Main.resolveEntity(line);
    assertNull(entity);
  }

  @Test
  public void withNoArgsAndOptions() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"--name", "metalake_demo"};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertNull(command);
    String entity = Main.resolveEntity(line);
    assertNull(entity);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void withHelpOption() throws ParseException, UnsupportedEncodingException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"--help"};
    CommandLine line = parser.parse(options, args);

    GravitinoCommandLine commandLine = new GravitinoCommandLine(line, options, null, "help");
    commandLine.handleSimpleLine();

    assertTrue(outContent.toString().contains("usage:")); // Expected help output
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void parseError() throws UnsupportedEncodingException {
    String[] args = {"--invalidOption"};

    Main.main(args);

    assertTrue(errContent.toString().contains("Error parsing command line")); // Expect error
    assertTrue(outContent.toString().contains("usage:")); // Expect help output
  }

  @Test
  public void tagWithMultiArgs() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();

    // gcli tag create --tag tagA tagB
    String[] args = {"tag", "create", "--tag", "tagA", "tagB"};
    CommandLine line = parser.parse(options, args);
    String[] tags = line.getOptionValues("tag");
    assertArrayEquals(tags, new Object[] {"tagA", "tagB"});

    // gcli tag delete --tag tagA tagB
    String[] deleteArgs = {"tag", "delete", "--tag", "tagA", "tagB"};
    CommandLine deleteLine = parser.parse(options, deleteArgs);
    String[] deleteTags = deleteLine.getOptionValues("tag");
    assertArrayEquals(deleteTags, new Object[] {"tagA", "tagB"});

    // gcli tag set --name catalog_postgres.hr --tag tagA tagB
    String[] setArgs = {"tag", "set", "--name", "catalog_postgres.hr", "--tag", "tagA", "tagB"};
    CommandLine setLine = parser.parse(options, setArgs);
    String[] setTags = setLine.getOptionValues("tag");
    assertArrayEquals(setTags, new Object[] {"tagA", "tagB"});

    // gcli tag remove --name catalog_postgres.hr --tag tagA tagB
    String[] removeArgs = {
      "tag", "remove", "--name", "catalog_postgres.hr", "--tag", "tagA", "tagB"
    };
    CommandLine removeLine = parser.parse(options, removeArgs);
    String[] removeTags = removeLine.getOptionValues("tag");
    assertArrayEquals(removeTags, new Object[] {"tagA", "tagB"});
  }

  @Test
  public void tagWithMultiArgsError() throws UnsupportedEncodingException {
    String[] args = {"tag", "properties", "--tag", "tagA", "tagB"};
    assertThrows(
        IllegalArgumentException.class,
        () -> Main.main(args),
        "Error: The current command only supports one --tag option.");

    String[] setArgs = {
      "tag",
      "set",
      "--name",
      "catalog_postgres.hr",
      "--tag",
      "tagA",
      "tagB",
      "--property",
      "test",
      "--value",
      "value"
    };

    assertThrows(
        IllegalArgumentException.class,
        () -> Main.main(setArgs),
        "Error: The current command only supports one --tag option.");

    String[] removeArgs = {
      "tag", "properties", "--tag", "tagA", "tagB", "--property", "test",
    };
    assertThrows(
        IllegalArgumentException.class,
        () -> Main.main(removeArgs),
        "Error: The current command only supports one --tag option.");
  }

  @Test
  public void catalogWithOneArg() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"catalog", "--name", "catalog_postgres"};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertEquals(CommandActions.DETAILS, command);
    String entity = Main.resolveEntity(line);
    assertEquals(CommandEntities.CATALOG, entity);
  }
}
