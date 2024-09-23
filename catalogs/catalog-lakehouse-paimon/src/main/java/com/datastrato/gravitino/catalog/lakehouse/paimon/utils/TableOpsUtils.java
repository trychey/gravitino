/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
import static org.apache.paimon.schema.SchemaChange.addColumn;
import static org.apache.paimon.schema.SchemaChange.dropColumn;
import static org.apache.paimon.schema.SchemaChange.removeOption;
import static org.apache.paimon.schema.SchemaChange.renameColumn;
import static org.apache.paimon.schema.SchemaChange.setOption;
import static org.apache.paimon.schema.SchemaChange.updateColumnComment;
import static org.apache.paimon.schema.SchemaChange.updateColumnNullability;
import static org.apache.paimon.schema.SchemaChange.updateColumnPosition;
import static org.apache.paimon.schema.SchemaChange.updateColumnType;
import static org.apache.paimon.schema.SchemaChange.updateComment;

import com.datastrato.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.TableChange.AddColumn;
import com.datastrato.gravitino.rel.TableChange.After;
import com.datastrato.gravitino.rel.TableChange.ColumnChange;
import com.datastrato.gravitino.rel.TableChange.ColumnPosition;
import com.datastrato.gravitino.rel.TableChange.Default;
import com.datastrato.gravitino.rel.TableChange.DeleteColumn;
import com.datastrato.gravitino.rel.TableChange.First;
import com.datastrato.gravitino.rel.TableChange.RemoveProperty;
import com.datastrato.gravitino.rel.TableChange.RenameColumn;
import com.datastrato.gravitino.rel.TableChange.SetProperty;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnComment;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnNullability;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnType;
import com.datastrato.gravitino.rel.TableChange.UpdateComment;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaChange.Move;

/** Utilities of {@link PaimonCatalogOps} to support table operation. */
public class TableOpsUtils {

  public static final Joiner DOT = Joiner.on(".");

  public static void checkColumnCapability(
      String fieldName, Expression defaultValue, boolean autoIncrement) {
    checkColumnDefaultValue(fieldName, defaultValue);
    checkColumnAutoIncrement(fieldName, autoIncrement);
  }

  public static List<SchemaChange> buildSchemaChanges(TableChange... tableChanges)
      throws UnsupportedOperationException {
    List<SchemaChange> schemaChanges = new ArrayList<>();
    for (TableChange tableChange : tableChanges) {
      schemaChanges.add(buildSchemaChange(tableChange));
    }
    return schemaChanges;
  }

  public static SchemaChange buildSchemaChange(TableChange tableChange)
      throws UnsupportedOperationException {
    if (tableChange instanceof ColumnChange) {
      if (tableChange instanceof AddColumn) {
        AddColumn addColumn = (AddColumn) tableChange;
        String fieldName = getfieldName(addColumn);
        checkColumnCapability(fieldName, addColumn.getDefaultValue(), addColumn.isAutoIncrement());
        return addColumn(
            fieldName,
            toPaimonType(addColumn.getDataType()).copy(addColumn.isNullable()),
            addColumn.getComment(),
            move(fieldName, addColumn.getPosition()));
      } else if (tableChange instanceof DeleteColumn) {
        return dropColumn(getfieldName((DeleteColumn) tableChange));
      } else if (tableChange instanceof RenameColumn) {
        RenameColumn renameColumn = ((RenameColumn) tableChange);
        return renameColumn(getfieldName(renameColumn), renameColumn.getNewName());
      } else if (tableChange instanceof UpdateColumnComment) {
        UpdateColumnComment updateColumnComment = (UpdateColumnComment) tableChange;
        return updateColumnComment(
            getfieldName(updateColumnComment), updateColumnComment.getNewComment());
      } else if (tableChange instanceof UpdateColumnNullability) {
        UpdateColumnNullability updateColumnNullability = (UpdateColumnNullability) tableChange;
        return updateColumnNullability(
            getfieldName(updateColumnNullability), updateColumnNullability.nullable());
      } else if (tableChange instanceof UpdateColumnPosition) {
        UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) tableChange;
        Preconditions.checkArgument(
            !(updateColumnPosition.getPosition() instanceof Default),
            "Default position is not supported for Paimon update column position.");
        return updateColumnPosition(
            move(getfieldName(updateColumnPosition), updateColumnPosition.getPosition()));
      } else if (tableChange instanceof UpdateColumnType) {
        UpdateColumnType updateColumnType = (UpdateColumnType) tableChange;
        return updateColumnType(
            getfieldName(updateColumnType), toPaimonType(updateColumnType.getNewDataType()));
      }
    } else if (tableChange instanceof UpdateComment) {
      return updateComment(((UpdateComment) tableChange).getNewComment());
    } else if (tableChange instanceof SetProperty) {
      SetProperty setProperty = ((SetProperty) tableChange);
      return setOption(setProperty.getProperty(), setProperty.getValue());
    } else if (tableChange instanceof RemoveProperty) {
      RemoveProperty removeProperty = (RemoveProperty) tableChange;
      return removeOption(removeProperty.getProperty());
    }
    throw new UnsupportedOperationException(
        String.format(
            "Paimon does not support %s table change.", tableChange.getClass().getSimpleName()));
  }

  private static void checkColumnDefaultValue(String fieldName, Expression defaultValue) {
    Preconditions.checkArgument(
        defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET),
        String.format(
            "Paimon set column default value through table properties instead of column info. Illegal column: %s.",
            fieldName));
  }

  private static void checkColumnAutoIncrement(String fieldName, boolean autoIncrement) {
    Preconditions.checkArgument(
        !autoIncrement,
        String.format(
            "Paimon does not support auto increment column. Illegal column: %s.", fieldName));
  }

  private static void checkNestedColumn(String[] fieldNames) {
    Preconditions.checkArgument(
        fieldNames.length == 1,
        String.format(
            "Paimon does not support update nested column. Illegal column: %s.",
            getfieldName(fieldNames)));
  }

  public static String[] getfieldName(String fieldName) {
    return new String[] {fieldName};
  }

  public static String getfieldName(String[] fieldName) {
    return DOT.join(fieldName);
  }

  private static String getfieldName(ColumnChange columnChange) {
    return getfieldName(columnChange.fieldName());
  }

  private static Move move(String fieldName, ColumnPosition columnPosition)
      throws UnsupportedOperationException {
    if (columnPosition instanceof After) {
      return Move.after(fieldName, ((After) columnPosition).getColumn());
    } else if (columnPosition instanceof Default) {
      return null;
    } else if (columnPosition instanceof First) {
      return Move.first(fieldName);
    }
    throw new UnsupportedOperationException(
        String.format(
            "Paimon does not support %s column position.",
            columnPosition.getClass().getSimpleName()));
  }
}
