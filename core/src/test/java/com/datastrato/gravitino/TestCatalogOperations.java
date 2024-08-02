/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.catalog.EntityCombinedFileset;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;
import com.datastrato.gravitino.messaging.DataLayout;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicCatalog;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.utils.FilesetPrefixPatternUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class TestCatalogOperations
    implements CatalogOperations, TableCatalog, FilesetCatalog, TopicCatalog, SupportsSchemas {

  private final Map<NameIdentifier, TestTable> tables;

  private final Map<NameIdentifier, TestSchema> schemas;

  private final Map<NameIdentifier, TestFileset> filesets;

  private final Map<NameIdentifier, TestTopic> topics;

  private final BasePropertiesMetadata tablePropertiesMetadata;

  private final BasePropertiesMetadata schemaPropertiesMetadata;

  private final BasePropertiesMetadata filesetPropertiesMetadata;

  private final BasePropertiesMetadata topicPropertiesMetadata;

  private Map<String, String> config;

  public static final String FAIL_CREATE = "fail-create";

  private static final String SLASH = "/";

  private static final String UNDER_SCORE = "_";

  private static final String DOT = ".";

  public TestCatalogOperations(Map<String, String> config) {
    tables = Maps.newHashMap();
    schemas = Maps.newHashMap();
    filesets = Maps.newHashMap();
    topics = Maps.newHashMap();
    tablePropertiesMetadata = new TestBasePropertiesMetadata();
    schemaPropertiesMetadata = new TestBasePropertiesMetadata();
    filesetPropertiesMetadata = new TestFilesetPropertiesMetadata();
    topicPropertiesMetadata = new TestBasePropertiesMetadata();
    this.config = config;
  }

  @Override
  public void initialize(Map<String, String> config, CatalogInfo info) throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return tables.keySet().stream()
        .filter(testTable -> testTable.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    if (tables.containsKey(ident)) {
      return tables.get(ident);
    } else {
      throw new NoSuchTableException("Table %s does not exist", ident);
    }
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestTable table =
        TestTable.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(new HashMap<>(properties))
            .withAuditInfo(auditInfo)
            .withColumns(columns)
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withPartitioning(partitions)
            .withIndexes(indexes)
            .build();

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException("Table %s already exists", ident);
    } else {
      tables.put(ident, table);
    }

    return TestTable.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(new HashMap<>(properties))
        .withAuditInfo(auditInfo)
        .withColumns(columns)
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withPartitioning(partitions)
        .withIndexes(indexes)
        .build();
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    if (!tables.containsKey(ident)) {
      throw new NoSuchTableException("Table %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestTable table = tables.get(ident);
    Map<String, String> newProps =
        table.properties() != null ? Maps.newHashMap(table.properties()) : Maps.newHashMap();

    NameIdentifier newIdent = ident;
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        newProps.put(
            ((TableChange.SetProperty) change).getProperty(),
            ((TableChange.SetProperty) change).getValue());
      } else if (change instanceof TableChange.RemoveProperty) {
        newProps.remove(((TableChange.RemoveProperty) change).getProperty());
      } else if (change instanceof TableChange.RenameTable) {
        String newName = ((TableChange.RenameTable) change).getNewName();
        newIdent = NameIdentifier.of(ident.namespace(), newName);
        if (tables.containsKey(newIdent)) {
          throw new TableAlreadyExistsException("Table %s already exists", ident);
        }
      } else {
        throw new IllegalArgumentException("Unsupported table change: " + change);
      }
    }

    TestTable updatedTable =
        TestTable.builder()
            .withName(newIdent.name())
            .withComment(table.comment())
            .withProperties(new HashMap<>(newProps))
            .withAuditInfo(updatedAuditInfo)
            .withColumns(table.columns())
            .withPartitioning(table.partitioning())
            .withDistribution(table.distribution())
            .withSortOrders(table.sortOrder())
            .withIndexes(table.index())
            .build();

    tables.put(ident, updatedTable);
    return updatedTable;
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    if (tables.containsKey(ident)) {
      tables.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return schemas.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestSchema schema =
        TestSchema.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    if (schemas.containsKey(ident)) {
      throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
    } else {
      schemas.put(ident, schema);
    }

    return schema;
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    if (schemas.containsKey(ident)) {
      return schemas.get(ident);
    } else {
      throw new NoSuchSchemaException("Schema %s does not exist", ident);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    if (!schemas.containsKey(ident)) {
      throw new NoSuchSchemaException("Schema %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestSchema schema = schemas.get(ident);
    Map<String, String> newProps =
        schema.properties() != null ? Maps.newHashMap(schema.properties()) : Maps.newHashMap();

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        newProps.put(
            ((SchemaChange.SetProperty) change).getProperty(),
            ((SchemaChange.SetProperty) change).getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        newProps.remove(((SchemaChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported schema change: " + change);
      }
    }

    TestSchema updatedSchema =
        TestSchema.builder()
            .withName(ident.name())
            .withComment(schema.comment())
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .build();

    schemas.put(ident, updatedSchema);
    return updatedSchema;
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (!schemas.containsKey(ident)) {
      return false;
    }

    schemas.remove(ident);
    if (cascade) {
      tables.keySet().stream()
          .filter(table -> table.namespace().toString().equals(ident.toString()))
          .forEach(tables::remove);
    }

    return true;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return tablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return schemaPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    if (config.containsKey("mock")) {
      return new BasePropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return ImmutableMap.<String, PropertyEntry<?>>builder()
              .put(
                  "key1",
                  PropertyEntry.stringPropertyEntry(
                      "key1", "value1", true, true, null, false, false))
              .put(
                  "key2",
                  PropertyEntry.stringPropertyEntry(
                      "key2", "value2", true, false, null, false, false))
              .put(
                  "key3",
                  new PropertyEntry.Builder<Integer>()
                      .withDecoder(Integer::parseInt)
                      .withEncoder(Object::toString)
                      .withDefaultValue(1)
                      .withDescription("key3")
                      .withHidden(false)
                      .withReserved(false)
                      .withImmutable(true)
                      .withJavaType(Integer.class)
                      .withRequired(false)
                      .withName("key3")
                      .build())
              .put(
                  "key4",
                  PropertyEntry.stringPropertyEntry(
                      "key4", "value4", false, false, "value4", false, false))
              .put(
                  "reserved_key",
                  PropertyEntry.stringPropertyEntry(
                      "reserved_key", "reserved_key", false, true, "reserved_value", false, true))
              .put(
                  "hidden_key",
                  PropertyEntry.stringPropertyEntry(
                      "hidden_key", "hidden_key", false, false, "hidden_value", true, false))
              .put(
                  FAIL_CREATE,
                  PropertyEntry.booleanPropertyEntry(
                      FAIL_CREATE,
                      "Whether an exception needs to be thrown on creation",
                      false,
                      false,
                      false,
                      false,
                      false))
              .build();
        }
      };
    } else if (config.containsKey("hive")) {
      return new BasePropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return ImmutableMap.<String, PropertyEntry<?>>builder()
              .put(
                  "hive.metastore.uris",
                  PropertyEntry.stringPropertyEntry(
                      "hive.metastore.uris",
                      "The Hive metastore URIs",
                      true,
                      true,
                      null,
                      false,
                      false))
              .build();
        }
      };
    }
    return Maps::newHashMap;
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    return filesetPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    return topicPropertiesMetadata;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return filesets.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    if (filesets.containsKey(ident)) {
      return filesets.get(ident);
    } else {
      throw new NoSuchFilesetException("Fileset %s does not exist", ident);
    }
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    Map<String, String> copyedProperties = Maps.newHashMap(properties);
    checkAndFillFilesetProperties(ident, copyedProperties);

    TestFileset fileset =
        TestFileset.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(copyedProperties)
            .withAuditInfo(auditInfo)
            .withType(type)
            .withStorageLocation(storageLocation)
            .build();

    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (filesets.containsKey(ident)) {
      throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
    } else if (!schemas.containsKey(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    } else {
      filesets.put(ident, fileset);
    }

    return fileset;
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    if (!filesets.containsKey(ident)) {
      throw new NoSuchFilesetException("Fileset %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestFileset fileset = filesets.get(ident);
    Map<String, String> newProps =
        fileset.properties() != null ? Maps.newHashMap(fileset.properties()) : Maps.newHashMap();
    NameIdentifier newIdent = ident;
    String newComment = fileset.comment();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        newProps.put(
            ((FilesetChange.SetProperty) change).getProperty(),
            ((FilesetChange.SetProperty) change).getValue());
      } else if (change instanceof FilesetChange.RemoveProperty) {
        newProps.remove(((FilesetChange.RemoveProperty) change).getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        String newName = ((FilesetChange.RenameFileset) change).getNewName();
        newIdent = NameIdentifier.of(ident.namespace(), newName);
        if (filesets.containsKey(newIdent)) {
          throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
        }
        filesets.remove(ident);
      } else if (change instanceof FilesetChange.UpdateFilesetComment) {
        newComment = ((FilesetChange.UpdateFilesetComment) change).getNewComment();
      } else if (change instanceof FilesetChange.RemoveComment) {
        newComment = null;
      } else {
        throw new IllegalArgumentException("Unsupported fileset change: " + change);
      }
    }

    checkAndFillFilesetProperties(ident, newProps);

    TestFileset updatedFileset =
        TestFileset.builder()
            .withName(newIdent.name())
            .withComment(newComment)
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .withType(fileset.type())
            .withStorageLocation(fileset.storageLocation())
            .build();
    filesets.put(newIdent, updatedFileset);
    return updatedFileset;
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    if (filesets.containsKey(ident)) {
      filesets.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public FilesetContext getFilesetContext(NameIdentifier ident, FilesetDataOperationCtx ctx)
      throws NoSuchFilesetException {
    if (filesets.containsKey(ident)) {
      String subPath;
      if (!ctx.subPath().trim().startsWith(SLASH)) {
        subPath = SLASH + ctx.subPath().trim();
      } else {
        subPath = ctx.subPath().trim();
      }

      Fileset fileset = loadFileset(ident);
      Preconditions.checkArgument(
          fileset.properties().containsKey(FilesetProperties.PREFIX_PATTERN_KEY),
          "Fileset: `%s` does not contain the property: `%s`, please set this property."
              + " Following options are supported: `%s`.",
          ident,
          FilesetProperties.PREFIX_PATTERN_KEY,
          Arrays.asList(FilesetPrefixPattern.values()));
      FilesetPrefixPattern prefixPattern =
          FilesetPrefixPattern.valueOf(
              fileset.properties().get(FilesetProperties.PREFIX_PATTERN_KEY));

      Preconditions.checkArgument(
          fileset.properties().containsKey(FilesetProperties.DIR_MAX_LEVEL_KEY),
          "Fileset: `%s` does not contain the property: `%s`, please set this property.",
          ident,
          FilesetProperties.DIR_MAX_LEVEL_KEY);
      int maxLevel =
          Integer.parseInt(fileset.properties().get(FilesetProperties.DIR_MAX_LEVEL_KEY));
      Preconditions.checkArgument(
          maxLevel > 0, "Fileset: `%s`'s max level should be greater than 0.", ident);

      boolean isMountFile = checkMountsSingleFile(fileset);
      Preconditions.checkArgument(ctx.operation() != null, "operation must not be null.");
      switch (ctx.operation()) {
        case CREATE:
        case APPEND:
        case MKDIRS:
          Preconditions.checkArgument(
              checkSubDirValid(subPath, prefixPattern, maxLevel, isMountFile),
              prefixErrorMessage(subPath, prefixPattern, maxLevel));
          break;
        case RENAME:
          Preconditions.checkArgument(
              subPath.startsWith(SLASH) && subPath.length() > 1,
              "subPath cannot be blank when need to rename a file or a directory.");
          Preconditions.checkArgument(
              !isMountFile,
              String.format(
                  "Cannot rename the fileset: %s which only mounts to a single file.", ident));
          Preconditions.checkArgument(
              checkSubDirValid(subPath, prefixPattern, maxLevel, false),
              prefixErrorMessage(subPath, prefixPattern, maxLevel));
          break;
        default:
          break;
      }

      String actualPath;
      // subPath cannot be null, so we only need check if it is blank
      if (subPath.startsWith(SLASH) && subPath.length() == 1) {
        actualPath = fileset.storageLocation();
      } else {
        actualPath = fileset.storageLocation() + subPath;
      }

      return TestFilesetContext.builder()
          .withFileset(
              EntityCombinedFileset.of(fileset)
                  .withHiddenPropertiesSet(
                      fileset.properties().keySet().stream()
                          .filter(filesetPropertiesMetadata::isHiddenProperty)
                          .collect(Collectors.toSet())))
          .withActualPaths(new String[] {actualPath})
          .build();
    } else {
      throw new NoSuchFilesetException("Fileset %s does not exist", ident);
    }
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    return topics.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    if (topics.containsKey(ident)) {
      return topics.get(ident);
    } else {
      throw new NoSuchTopicException("Topic %s does not exist", ident);
    }
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    TestTopic topic =
        TestTopic.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    if (topics.containsKey(ident)) {
      throw new TopicAlreadyExistsException("Topic %s already exists", ident);
    } else {
      topics.put(ident, topic);
    }

    return topic;
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    if (!topics.containsKey(ident)) {
      throw new NoSuchTopicException("Topic %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestTopic topic = topics.get(ident);
    Map<String, String> newProps =
        topic.properties() != null ? Maps.newHashMap(topic.properties()) : Maps.newHashMap();
    String newComment = topic.comment();

    for (TopicChange change : changes) {
      if (change instanceof TopicChange.SetProperty) {
        newProps.put(
            ((TopicChange.SetProperty) change).getProperty(),
            ((TopicChange.SetProperty) change).getValue());
      } else if (change instanceof TopicChange.RemoveProperty) {
        newProps.remove(((TopicChange.RemoveProperty) change).getProperty());
      } else if (change instanceof TopicChange.UpdateTopicComment) {
        newComment = ((TopicChange.UpdateTopicComment) change).getNewComment();
      } else {
        throw new IllegalArgumentException("Unsupported topic change: " + change);
      }
    }

    TestTopic updatedTopic =
        TestTopic.builder()
            .withName(ident.name())
            .withComment(newComment)
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .build();

    topics.put(ident, updatedTopic);
    return updatedTopic;
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) throws NoSuchTopicException {
    if (topics.containsKey(ident)) {
      topics.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  private void checkAndFillFilesetProperties(
      NameIdentifier identifier, Map<String, String> properties) {
    // Get properties with default values
    FilesetPrefixPattern pattern =
        FilesetPrefixPattern.valueOf(
            properties.computeIfAbsent(
                FilesetProperties.PREFIX_PATTERN_KEY, value -> FilesetPrefixPattern.ANY.name()));
    int timeNum =
        Integer.parseInt(
            properties.computeIfAbsent(
                FilesetProperties.LIFECYCLE_TIME_NUM_KEY, value -> String.valueOf(-1)));
    Preconditions.checkArgument(
        timeNum != 0,
        "The fileset: `%s`'s property: %s should be a negative or positive integer,"
            + " but cannot be 0.",
        identifier,
        FilesetProperties.LIFECYCLE_TIME_NUM_KEY);
    FilesetLifecycleUnit timeUnit =
        FilesetLifecycleUnit.valueOf(
            properties.computeIfAbsent(
                FilesetProperties.LIFECYCLE_TIME_UNIT_KEY,
                value -> FilesetLifecycleUnit.RETENTION_DAY.name()));
    Preconditions.checkArgument(
        timeUnit == FilesetLifecycleUnit.RETENTION_DAY,
        "Only supported: `%s` for the property: `%s` now.",
        FilesetLifecycleUnit.RETENTION_DAY.name(),
        FilesetProperties.LIFECYCLE_TIME_NUM_KEY);

    int dirMaxLevel;
    switch (pattern) {
      case ANY:
        Preconditions.checkArgument(
            timeNum < 0,
            "Lifecycle time number should be permanent"
                + " because fileset's dir prefix pattern type is: `%s`.",
            pattern.name());
        dirMaxLevel =
            Integer.parseInt(
                properties.computeIfAbsent(
                    FilesetProperties.DIR_MAX_LEVEL_KEY, value -> String.valueOf(3)));
        Preconditions.checkArgument(
            dirMaxLevel > 0,
            "`%s` should be greater than 0 for the dir prefix: `%s`",
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            pattern.name());
        break;
      case DATE:
      case DATE_HOUR:
      case DATE_WITH_STRING:
      case DATE_US_HOUR:
      case DATE_US_HOUR_US_MINUTE:
        dirMaxLevel =
            Integer.parseInt(
                properties.computeIfAbsent(
                    FilesetProperties.DIR_MAX_LEVEL_KEY, value -> String.valueOf(3)));
        Preconditions.checkArgument(
            dirMaxLevel > 0,
            "`%s` should be grater than 0 for the dir prefix: `%s`",
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            pattern.name());
        break;
      case YEAR_MONTH_DAY:
        dirMaxLevel =
            Integer.parseInt(
                properties.computeIfAbsent(
                    FilesetProperties.DIR_MAX_LEVEL_KEY, value -> String.valueOf(5)));
        Preconditions.checkArgument(
            dirMaxLevel >= 5,
            "`%s` should be greater and equal than 5 for the dir prefix: `%s`",
            FilesetProperties.DIR_MAX_LEVEL_KEY,
            pattern.name());
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported fileset directory prefix pattern: `%s`.", pattern.name()));
    }
  }

  private static boolean checkSubDirValid(
      String subPath, FilesetPrefixPattern prefixPattern, int maxLevel, boolean isFile) {
    // match sub dir like `/xxx/yyy`
    if (StringUtils.isNotBlank(subPath)) {
      // If the prefix is not match, return false immediately
      if (!FilesetPrefixPatternUtils.checkPrefixValid(subPath, prefixPattern)) {
        return false;
      }
      // If the max level is not match, try to check if there is a temporary directory
      if (!FilesetPrefixPatternUtils.checkLevelValid(subPath, maxLevel, isFile)) {
        // In this case, the sub dir level is greater than the dir max level
        String[] dirNames =
            subPath.startsWith(SLASH) ? subPath.substring(1).split(SLASH) : subPath.split(SLASH);
        // Try to check subdirectories before max level + 1 having temporary directory,
        // if so, we pass the check
        for (int index = 0; index < maxLevel + 1 && index < dirNames.length; index++) {
          if (dirNames[index].startsWith(UNDER_SCORE) || dirNames[index].startsWith(DOT)) {
            return true;
          }
        }
        return false;
      }
    }
    return true;
  }

  private boolean checkMountsSingleFile(Fileset fileset) {
    try {
      File locationPath = new File(fileset.storageLocation());
      return locationPath.isFile();
    } catch (Exception e) {
      return false;
    }
  }

  private static String prefixErrorMessage(
      String subPath, FilesetPrefixPattern pattern, Integer maxLevel) {
    return String.format(
        "The sub Path: %s is not valid, the whole path should like `%s`,"
            + " and max sub directory level after fileset identifier should be less than %d.",
        subPath, pattern.getExample(), maxLevel);
  }
}
