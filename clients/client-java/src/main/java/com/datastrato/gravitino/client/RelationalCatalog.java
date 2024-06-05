/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static com.datastrato.gravitino.dto.util.DTOConverters.toDTO;
import static com.datastrato.gravitino.dto.util.DTOConverters.toDTOs;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.TableCreateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.TableResponse;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Relational catalog is a catalog implementation that supports relational database like metadata
 * operations, for example, schemas and tables list, creation, update and deletion. A Relational
 * catalog is under the metalake.
 */
public class RelationalCatalog extends BaseSchemaCatalog implements TableCatalog {

  RelationalCatalog(
      Namespace namespace,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(namespace, name, type, provider, comment, properties, auditDTO, restClient);
  }

  @Override
  public TableCatalog asTableCatalog() {
    return this;
  }

  /**
   * List all the tables under the given Schema namespace.
   *
   * @param namespace The namespace to list the tables under it. This namespace should have 1 level,
   *     which is the schema name;
   * @return A list of {@link NameIdentifier} of the tables under the given namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    checkTableNamespace(namespace);

    Namespace fullNamespace = getTableFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Load the table with specified identifier.
   *
   * @param ident The identifier of the table to load.
   * @return The {@link Table} with specified identifier.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    checkTableNameIdentifer(ident);

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(ident.namespace(), resp.getTable(), restClient);
  }

  /**
   * Create a new table with specified identifier, columns, comment and properties.
   *
   * @param ident The identifier of the table.
   * @param columns The columns of the table.
   * @param comment The comment of the table.
   * @param properties The properties of the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   * @return The created {@link Table}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws TableAlreadyExistsException if the table with specified identifier already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    checkTableNameIdentifer(ident);

    TableCreateRequest req =
        new TableCreateRequest(
            RESTUtils.encodeString(ident.name()),
            comment,
            toDTOs(columns),
            properties,
            toDTOs(sortOrders),
            toDTO(distribution),
            toDTOs(partitioning),
            toDTOs(indexes));
    req.validate();

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.post(
            formatTableRequestPath(fullNamespace),
            req,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(ident.namespace(), resp.getTable(), restClient);
  }

  /**
   * Alter the table with specified identifier by applying the changes.
   *
   * @param ident The identifier of the table.
   * @param changes Table changes to apply to the table.
   * @return The altered {@link Table}.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    checkTableNameIdentifer(ident);

    List<TableUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toTableUpdateRequest)
            .collect(Collectors.toList());
    TableUpdatesRequest updatesRequest = new TableUpdatesRequest(reqs);
    updatesRequest.validate();

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.put(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            updatesRequest,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(ident.namespace(), resp.getTable(), restClient);
  }

  /**
   * Drop the table with specified identifier.
   *
   * @param ident The identifier of the table.
   * @return true if the table is dropped successfully, false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    checkTableNameIdentifer(ident);

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * Purge the table with specified identifier.
   *
   * @param ident The identifier of the table.
   * @return true if the table is purged successfully, false otherwise.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    checkTableNameIdentifer(ident);

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    Map<String, String> params = new HashMap<>();
    params.put("purge", "true");
    try {
      DropResponse resp =
          restClient.delete(
              formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
              params,
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.tableErrorHandler());
      resp.validate();
      return resp.dropped();
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      return false;
    }
  }

  @VisibleForTesting
  static String formatTableRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(ns.level(2)))
        .append("/tables")
        .toString();
  }

  /**
   * Check whether the namespace of a table is valid
   *
   * @param namespace The namespace to check
   */
  static void checkTableNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Table namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a table is valid
   *
   * @param ident The NameIdentifier to check
   */
  static void checkTableNameIdentifer(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifer must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifer name must not be empty");
    checkTableNamespace(ident.namespace());
  }

  Namespace getTableFullNamespace(Namespace tableNamespace) {
    return Namespace.of(this.namespace().level(0), this.name(), tableNamespace.level(0));
  }
  /**
   * Create a new builder for the relational catalog.
   *
   * @return A new builder for the relational catalog.
   */
  public static Builder builder() {
    return new Builder();
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    /** The REST client to send the requests. */
    private RESTClient restClient;
    /** The namespace of the catalog */
    private Namespace namespace;

    protected Builder() {}

    Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public RelationalCatalog build() {
      Namespace.check(
          namespace != null && namespace.length() == 1,
          "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
          namespace);
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new RelationalCatalog(
          namespace, name, type, provider, comment, properties, audit, restClient);
    }
  }
}
