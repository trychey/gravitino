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

package org.apache.gravitino.cli.commands;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.ParseType;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Type;

/** Update the data type of a column. */
public class UpdateColumnDatatype extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String table;
  protected final String column;
  protected final String datatype;

  /**
   * Update the data type of a column.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param datatype The new data type name.
   */
  public UpdateColumnDatatype(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String datatype) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.column = column;
    this.datatype = datatype;
  }

  /** Update the data type of a column. */
  public void handle() {
    String[] columns = {column};
    Type convertedDatatype = ParseType.toType(datatype);

    try {
      NameIdentifier columnName = NameIdentifier.of(schema, table);
      GravitinoClient client = buildClient(metalake);
      TableChange change = TableChange.updateColumnType(columns, convertedDatatype);

      client.loadCatalog(catalog).asTableCatalog().alterTable(columnName, change);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchSchemaException err) {
      System.err.println(ErrorMessages.UNKNOWN_SCHEMA);
      return;
    } catch (NoSuchTableException err) {
      System.err.println(ErrorMessages.UNKNOWN_TABLE);
      return;
    } catch (NoSuchColumnException err) {
      System.err.println(ErrorMessages.UNKNOWN_COLUMN);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(column + " datatype changed to " + datatype + ".");
  }
}
