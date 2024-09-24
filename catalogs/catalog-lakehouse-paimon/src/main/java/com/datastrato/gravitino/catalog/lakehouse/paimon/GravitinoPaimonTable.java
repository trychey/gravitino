/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonTablePropertiesMetadata.COMMENT;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonTablePropertiesMetadata.TABLE_MODIFY_TIMESTAMP_MS;
import static com.datastrato.gravitino.dto.rel.partitioning.Partitioning.EMPTY_PARTITIONING;
import static com.datastrato.gravitino.meta.AuditInfo.EMPTY;
import static com.datastrato.gravitino.rel.indexes.Indexes.primary;

import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

/** Implementation of {@link Table} that represents a Paimon Table entity in the Paimon table. */
@ToString
@Getter
public class GravitinoPaimonTable extends BaseTable {

  public static final String PAIMON_PRIMARY_KEY_INDEX_NAME = "PAIMON_PRIMARY_KEY_INDEX";

  private GravitinoPaimonTable() {}

  @Override
  protected TableOperations newOps() {
    // TODO: Implement this interface when we have the Paimon table operations.
    throw new UnsupportedOperationException("PaimonTable does not support TableOperations.");
  }

  /**
   * Converts {@link GravitinoPaimonTable} instance to Paimon table.
   *
   * @return The converted Paimon table.
   */
  public Schema toPaimonTableSchema() {
    Schema.Builder builder = Schema.newBuilder().comment(comment);
    if (properties == null) {
      properties = Maps.newHashMap();
    }
    if (partitioning == null) {
      partitioning = EMPTY_PARTITIONING;
    }

    Map<String, String> normalizedProperties = new HashMap<>(properties);
    normalizedProperties.remove(COMMENT);

    List<String> partitionKeys = getPartitionKeys(partitioning);
    List<String> primaryKeys = getPrimaryKeysFromIndexes(indexes);

    validate(primaryKeys, partitionKeys);

    builder.options(normalizedProperties).primaryKey(primaryKeys).partitionKeys(partitionKeys);
    for (int index = 0; index < columns.length; index++) {
      DataField dataField = GravitinoPaimonColumn.toPaimonColumn(index, columns[index]);
      builder.column(dataField.name(), dataField.type(), dataField.description());
    }
    return builder.build();
  }

  /**
   * Creates a new {@link GravitinoPaimonTable} instance from Paimon table.
   *
   * @param table The {@link Table} instance of Paimon table.
   * @return A new {@link GravitinoPaimonTable} instance.
   */
  public static GravitinoPaimonTable fromPaimonTable(Table table) {
    Map<String, String> properties = new HashMap<>(table.options());

    if (table instanceof FileStoreTable) {
      FileStoreTable fileStoreTable = (FileStoreTable) table;
      properties.put(
          TABLE_MODIFY_TIMESTAMP_MS, String.valueOf(fileStoreTable.schema().timeMillis()));
    }

    return builder()
        .withName(table.name())
        .withColumns(
            GravitinoPaimonColumn.fromPaimonRowType(table.rowType())
                .toArray(new GravitinoPaimonColumn[0]))
        .withPartitioning(toGravitinoPartitioning(table.partitionKeys()))
        .withComment(table.comment().orElse(null))
        .withProperties(properties)
        .withIndexes(constructIndexesFromPrimaryKeys(table))
        .withAuditInfo(EMPTY)
        .build();
  }

  private static Transform[] toGravitinoPartitioning(List<String> partitionKeys) {
    return partitionKeys.stream().map(Transforms::identity).toArray(Transform[]::new);
  }

  private static List<String> getPartitionKeys(Transform[] partitioning) {
    if (partitioning == null) {
      return Collections.emptyList();
    }

    return Arrays.stream(partitioning)
        .map(
            partition -> {
              NamedReference[] references = partition.references();
              Preconditions.checkArgument(
                  references.length == 1, "Partitioning column must be single-column, like 'a'.");
              return references[0].toString();
            })
        .collect(Collectors.toList());
  }

  private List<String> getPrimaryKeysFromIndexes(Index[] indexes) {
    if (indexes == null || indexes.length == 0) {
      return Collections.emptyList();
    }

    Preconditions.checkArgument(
        indexes.length == 1, "Paimon only supports no more than one Index.");

    Index primaryKeyIndex = indexes[0];
    Arrays.stream(primaryKeyIndex.fieldNames())
        .forEach(
            filedName ->
                Preconditions.checkArgument(
                    filedName != null && filedName.length == 1,
                    "The primary key columns should not be nested."));

    return Arrays.stream(primaryKeyIndex.fieldNames())
        .map(fieldName -> fieldName[0])
        .collect(Collectors.toList());
  }

  private static Index[] constructIndexesFromPrimaryKeys(Table table) {
    Index[] indexes = new Index[0];
    if (table.primaryKeys() != null && !table.primaryKeys().isEmpty()) {
      String[][] filedNames = constructIndexFiledNames(table.primaryKeys());
      indexes =
          Collections.singletonList(primary(PAIMON_PRIMARY_KEY_INDEX_NAME, filedNames))
              .toArray(new Index[0]);
    }
    return indexes;
  }

  private static String[][] constructIndexFiledNames(List<String> primaryKeys) {
    return primaryKeys.stream()
        .map(pk -> new String[] {pk})
        .collect(Collectors.toList())
        .toArray(new String[0][0]);
  }

  private static void validate(List<String> primaryKeys, List<String> partitionKeys) {
    if (!primaryKeys.isEmpty()) {
      List<String> adjusted =
          primaryKeys.stream()
              .filter(pk -> !partitionKeys.contains(pk))
              .collect(Collectors.toList());

      Preconditions.checkState(
          !adjusted.isEmpty(),
          String.format(
              "Paimon Table Primary key constraint %s should not be same with partition fields %s,"
                  + " this will result in only one record in a partition.",
              primaryKeys, partitionKeys));
    }
  }

  /** A builder class for constructing {@link GravitinoPaimonTable} instance. */
  public static class Builder extends BaseTableBuilder<Builder, GravitinoPaimonTable> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a {@link GravitinoPaimonTable} instance using the provided values.
     *
     * @return A new {@link GravitinoPaimonTable} instance with the configured values.
     */
    @Override
    protected GravitinoPaimonTable internalBuild() {
      GravitinoPaimonTable paimonTable = new GravitinoPaimonTable();
      paimonTable.name = name;
      paimonTable.comment = comment;
      paimonTable.columns = columns;
      paimonTable.partitioning = partitioning;
      paimonTable.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
      paimonTable.indexes = indexes;
      paimonTable.auditInfo = auditInfo;
      return paimonTable;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
