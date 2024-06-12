/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.storage.relational.RelationalEntityStore;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCatalogOperations implements CatalogOperations, SupportsSchemas, FilesetCatalog {

  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema %s does not exist";
  private static final String FILESET_DOES_NOT_EXIST_MSG = "Fileset %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(HadoopCatalogOperations.class);

  private static final HadoopCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new HadoopCatalogPropertiesMetadata();

  private static final HadoopSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new HadoopSchemaPropertiesMetadata();

  private static final HadoopFilesetPropertiesMetadata FILESET_PROPERTIES_METADATA =
      new HadoopFilesetPropertiesMetadata();

  private final EntityStore store;

  @VisibleForTesting Configuration hadoopConf;

  @VisibleForTesting Optional<Path> catalogStorageLocation;

  private List<String> validManagedPaths;
  private List<String> validExternalPaths;

  // For testing only.
  HadoopCatalogOperations(EntityStore store) {
    this.store = store;
  }

  public HadoopCatalogOperations() {
    this(GravitinoEnv.getInstance().entityStore());
  }

  @Override
  public void initialize(Map<String, String> config, CatalogInfo info) throws RuntimeException {
    // Initialize Hadoop Configuration.
    this.hadoopConf = new Configuration();
    Map<String, String> bypassConfigs =
        config.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CATALOG_BYPASS_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CATALOG_BYPASS_PREFIX.length()),
                    Map.Entry::getValue));
    bypassConfigs.forEach(hadoopConf::set);

    String catalogLocation =
        (String)
            CATALOG_PROPERTIES_METADATA.getOrDefault(
                config, HadoopCatalogPropertiesMetadata.LOCATION);

    String validManagedPathString =
        hadoopConf.get(HadoopCatalogPropertiesMetadata.VALID_MANAGED_PATHS, "");
    if (StringUtils.isNotBlank(validManagedPathString)) {
      this.validManagedPaths = Splitter.on(',').splitToList(validManagedPathString);
    } else {
      this.validManagedPaths = ImmutableList.of();
    }

    String validExternalPathString =
        hadoopConf.get(HadoopCatalogPropertiesMetadata.VALID_EXTERNAL_PATHS, "");
    if (StringUtils.isNotBlank(validExternalPathString)) {
      this.validExternalPaths = Splitter.on(',').splitToList(validExternalPathString);
    } else {
      this.validExternalPaths = ImmutableList.of();
    }

    this.catalogStorageLocation = Optional.ofNullable(catalogLocation).map(Path::new);
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    try {
      NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
      if (!store.exists(schemaIdent, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException(SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
      }

      List<FilesetEntity> filesets =
          store.list(namespace, FilesetEntity.class, Entity.EntityType.FILESET);
      return filesets.stream()
          .map(f -> NameIdentifier.of(namespace, f.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list filesets under namespace " + namespace, e);
    }
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    try {
      FilesetEntity filesetEntity =
          store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);

      return HadoopFileset.builder()
          .withName(ident.name())
          .withType(filesetEntity.filesetType())
          .withComment(filesetEntity.comment())
          .withStorageLocation(filesetEntity.storageLocation())
          .withProperties(filesetEntity.properties())
          .withAuditInfo(filesetEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchFilesetException(exception, FILESET_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load fileset %s" + ident, ioe);
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
    try {
      if (store.exists(ident, Entity.EntityType.FILESET)) {
        throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if fileset " + ident + " exists", ioe);
    }

    /*
    SchemaEntity schemaEntity;
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      schemaEntity = store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class);
    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + schemaIdent, ioe);
    }
    */

    // For external fileset, the storageLocation must be set.
    if (type == Fileset.Type.EXTERNAL && StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException(
          "Storage location must be set for external fileset " + ident);
    }

    /*
    // Either catalog property "location", or schema property "location", or storageLocation must be
    // set for managed fileset.
    Path schemaPath = getSchemaPath(schemaIdent.name(), schemaEntity.properties());
    if (schemaPath == null && StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException(
          "Storage location must be set for fileset "
              + ident
              + " when it's catalog and schema location are not set");
    }

    // The specified storageLocation will take precedence over the calculated one.
    Path filesetPath =
        StringUtils.isNotBlank(storageLocation)
            ? new Path(storageLocation)
            : new Path(schemaPath, ident.name());

    try {
      // formalize the path to avoid path without scheme, uri, authority, etc.
      filesetPath = formalizePath(filesetPath, hadoopConf);
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to formalize fileset " + ident + " location " + filesetPath, ioe);
    }
    */

    // Change the immutable map to a mutable map
    Map<String, String> props =
        properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
    // Check properties before creating the directory
    checkAndFillFilesetProperties(ident, props);

    // Always need specify storage location for all filests
    if (StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException("Storage location must be set for fileset: " + ident);
    }

    Path filesetPath = new Path(storageLocation);
    if (type == Fileset.Type.MANAGED) {
      checkAndCreateManagedStorageLocation(ident, filesetPath);
    } else {
      checkExternalStorageLocation(ident, filesetPath);
    }

    StringIdentifier stringId = StringIdentifier.fromProperties(props);
    Preconditions.checkArgument(stringId != null, "Property String identifier should not be null");

    FilesetEntity filesetEntity =
        FilesetEntity.builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withFilesetType(type)
            // Store the storageLocation to the store. If the "storageLocation" is null for
            // managed fileset, Gravitino will get and store the location based on the
            // catalog/schema's location and store it to the store.
            .withStorageLocation(filesetPath.toString())
            .withProperties(props)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(filesetEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident, ioe);
    }

    return HadoopFileset.builder()
        .withName(ident.name())
        .withComment(comment)
        .withType(type)
        .withStorageLocation(filesetPath.toString())
        .withProperties(filesetEntity.properties())
        .withAuditInfo(filesetEntity.auditInfo())
        .build();
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    try {
      if (!store.exists(ident, Entity.EntityType.FILESET)) {
        throw new NoSuchFilesetException(FILESET_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load fileset " + ident, ioe);
    }

    try {
      FilesetEntity updatedFilesetEntity =
          store.update(
              ident,
              FilesetEntity.class,
              Entity.EntityType.FILESET,
              e -> updateFilesetEntity(ident, e, changes));

      return HadoopFileset.builder()
          .withName(updatedFilesetEntity.name())
          .withComment(updatedFilesetEntity.comment())
          .withType(updatedFilesetEntity.filesetType())
          .withStorageLocation(updatedFilesetEntity.storageLocation())
          .withProperties(updatedFilesetEntity.properties())
          .withAuditInfo(updatedFilesetEntity.auditInfo())
          .build();

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to update fileset " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchFilesetException(nsee, FILESET_DOES_NOT_EXIST_MSG, ident);
    } catch (AlreadyExistsException aee) {
      // This is happened when renaming a fileset to an existing fileset name.
      throw new RuntimeException(
          "Fileset with the same name " + ident.name() + " already exists", aee);
    }
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    try {
      FilesetEntity filesetEntity =
          store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);
      Path filesetPath = new Path(filesetEntity.storageLocation());

      // For managed fileset, we should delete the related files.
      if (filesetEntity.filesetType() == Fileset.Type.MANAGED) {
        FileSystem fs = filesetPath.getFileSystem(hadoopConf);
        if (fs.exists(filesetPath)) {
          if (!fs.delete(filesetPath, true)) {
            LOG.warn("Failed to delete fileset {} location {}", ident, filesetPath);
            return false;
          }

        } else {
          LOG.warn("Fileset {} location {} does not exist", ident, filesetPath);
        }
      }

      return store.delete(ident, Entity.EntityType.FILESET);
    } catch (NoSuchEntityException ne) {
      LOG.warn("Fileset {} does not exist", ident);
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete fileset " + ident, ioe);
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      List<SchemaEntity> schemas =
          store.list(namespace, SchemaEntity.class, Entity.EntityType.SCHEMA);
      return schemas.stream()
          .map(s -> NameIdentifier.of(namespace, s.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list schemas under namespace " + namespace, e);
    }
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      if (store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident + " exists", ioe);
    }

    Path schemaPath = getSchemaPath(ident.name(), properties);
    if (schemaPath != null) {
      try {
        FileSystem fs = schemaPath.getFileSystem(hadoopConf);
        if (!fs.exists(schemaPath)) {
          if (!fs.mkdirs(schemaPath)) {
            // Fail the operation when failed to create the schema path.
            throw new RuntimeException(
                "Failed to create schema " + ident + " location " + schemaPath);
          }
          LOG.info("Created schema {} location {}", ident, schemaPath);
        } else {
          LOG.info("Schema {} manages the existing location {}", ident, schemaPath);
        }

      } catch (IOException ioe) {
        throw new RuntimeException(
            "Failed to create schema " + ident + " location " + schemaPath, ioe);
      }
    }

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkNotNull(stringId, "Property String identifier should not be null");

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(schemaEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create schema " + ident, ioe);
    }

    return HadoopSchema.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(schemaEntity.properties())
        .withAuditInfo(schemaEntity.auditInfo())
        .build();
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      SchemaEntity schemaEntity = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);

      return HadoopSchema.builder()
          .withName(ident.name())
          .withComment(schemaEntity.comment())
          .withProperties(schemaEntity.properties())
          .withAuditInfo(schemaEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + ident, ioe);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      if (!store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException(SCHEMA_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident + " exists", ioe);
    }

    try {
      SchemaEntity entity =
          store.update(
              ident,
              SchemaEntity.class,
              Entity.EntityType.SCHEMA,
              schemaEntity -> updateSchemaEntity(ident, schemaEntity, changes));

      return HadoopSchema.builder()
          .withName(ident.name())
          .withComment(entity.comment())
          .withProperties(entity.properties())
          .withAuditInfo(entity.auditInfo())
          .build();

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to update schema " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchSchemaException(nsee, SCHEMA_DOES_NOT_EXIST_MSG, ident);
    } catch (AlreadyExistsException aee) {
      throw new RuntimeException(
          "Schema with the same name "
              + ident.name()
              + " already exists, this is unexpected because schema doesn't support rename",
          aee);
    }
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      SchemaEntity schemaEntity = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());

      Path schemaPath = getSchemaPath(ident.name(), properties);
      // Nothing to delete if the schema path is not set.
      if (schemaPath == null) {
        return false;
      }

      FileSystem fs = schemaPath.getFileSystem(hadoopConf);
      // Nothing to delete if the schema path does not exist.
      if (!fs.exists(schemaPath)) {
        return false;
      }

      if (fs.listStatus(schemaPath).length > 0 && !cascade) {
        throw new NonEmptySchemaException(
            "Schema %s with location %s is not empty", ident, schemaPath);
      } else {
        fs.delete(schemaPath, true);
      }

      LOG.info("Deleted schema {} location {}", ident, schemaPath);
      return true;

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident + " location", ioe);
    }
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Hadoop fileset catalog doesn't support table related operations");
  }

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Hadoop fileset catalog doesn't support topic related operations");
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    return FILESET_PROPERTIES_METADATA;
  }

  @Override
  public void close() throws IOException {}

  private SchemaEntity updateSchemaEntity(
      NameIdentifier ident, SchemaEntity schemaEntity, SchemaChange... changes) {
    Map<String, String> props =
        schemaEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(schemaEntity.properties());

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty setProperty = (SchemaChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        SchemaChange.RemoveProperty removeProperty = (SchemaChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else {
        throw new IllegalArgumentException(
            "Unsupported schema change: " + change.getClass().getSimpleName());
      }
    }

    return SchemaEntity.builder()
        .withName(schemaEntity.name())
        .withNamespace(ident.namespace())
        .withId(schemaEntity.id())
        .withComment(schemaEntity.comment())
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(schemaEntity.auditInfo().creator())
                .withCreateTime(schemaEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private FilesetEntity updateFilesetEntity(
      NameIdentifier ident, FilesetEntity filesetEntity, FilesetChange... changes) {
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());
    String newName = ident.name();
    String newComment = filesetEntity.comment();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty removeProperty = (FilesetChange.RemoveProperty) change;
        // If the `owner` property is set, cannot remove anymore
        if (removeProperty.getProperty().equalsIgnoreCase(FilesetProperties.OWNER_KEY)) {
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot remove the `%s` property for the fileset: `%s`.",
                  FilesetProperties.OWNER_KEY, ident));
        }
        props.remove(removeProperty.getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        newName = ((FilesetChange.RenameFileset) change).getNewName();
      } else if (change instanceof FilesetChange.UpdateFilesetComment) {
        newComment = ((FilesetChange.UpdateFilesetComment) change).getNewComment();
      } else {
        throw new IllegalArgumentException(
            "Unsupported fileset change: " + change.getClass().getSimpleName());
      }
    }

    // Check properties after applying all changes
    checkAndFillFilesetProperties(ident, props);

    return FilesetEntity.builder()
        .withName(newName)
        .withNamespace(ident.namespace())
        .withId(filesetEntity.id())
        .withComment(newComment)
        .withFilesetType(filesetEntity.filesetType())
        .withStorageLocation(filesetEntity.storageLocation())
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(filesetEntity.auditInfo().creator())
                .withCreateTime(filesetEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private Path getSchemaPath(String name, Map<String, String> properties) {
    String schemaLocation =
        (String)
            SCHEMA_PROPERTIES_METADATA.getOrDefault(
                properties, HadoopSchemaPropertiesMetadata.LOCATION);

    return Optional.ofNullable(schemaLocation)
        .map(Path::new)
        .orElse(catalogStorageLocation.map(p -> new Path(p, name)).orElse(null));
  }

  @VisibleForTesting
  static Path formalizePath(Path path, Configuration configuration) throws IOException {
    FileSystem defaultFs = FileSystem.get(configuration);
    return path.makeQualified(defaultFs.getUri(), defaultFs.getWorkingDirectory());
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

  private void checkAndCreateManagedStorageLocation(NameIdentifier ident, Path filesetPath) {
    try {
      String checkedPathString = filesetPath.toString();
      if (!this.validManagedPaths.isEmpty()) {
        // Check if the storage location is set in the valid managed locations
        boolean isValidManagedPath =
            this.validManagedPaths.stream()
                .anyMatch(
                    patternString -> {
                      Pattern pattern =
                          Pattern.compile(
                              String.format(
                                  patternString,
                                  ident.namespace().level(1),
                                  ident.namespace().level(2),
                                  ident.name()));
                      Matcher matcher = pattern.matcher(checkedPathString);
                      return matcher.matches();
                    });
        if (!isValidManagedPath) {
          throw new RuntimeException(
              String.format(
                  "The storage location: %s is not valid for managed fileset: %s.%s.%s.",
                  checkedPathString,
                  ident.namespace().level(1),
                  ident.namespace().level(2),
                  ident.name()));
        }
      }

      FileSystem fs = filesetPath.getFileSystem(hadoopConf);
      if (!fs.exists(filesetPath)) {
        if (!fs.mkdirs(filesetPath)) {
          throw new RuntimeException(
              "Failed to create fileset " + ident + " location " + filesetPath);
        }
      } else {
        LOG.warn(
            "Using existing managed storage location: `{}` for fileset: `{}`", filesetPath, ident);
      }
      setPathPermission(fs, filesetPath);

      LOG.info("Created managed fileset {} location {}", ident, filesetPath);
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to create managed fileset " + ident + " location " + filesetPath, ioe);
    }
  }

  private void setPathPermission(FileSystem fs, Path filesetPath) {
    if (fs instanceof LocalFileSystem) {
      // skip unit test
      return;
    }

    try {
      FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE);
      fs.setPermission(filesetPath, permission);

      String platformAcl = "user:h_data_platform:rwx,default:user:h_data_platform:rwx";
      String sqlPrcAcl = "user:sql_prc:rwx,default:user:sql_prc:rwx";
      String otherAcl = "other::---,default:other::r-x";
      List<AclEntry> aclEntries =
          AclEntry.parseAclSpec(String.join(",", platformAcl, sqlPrcAcl, otherAcl), true);

      fs.modifyAclEntries(filesetPath, aclEntries);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to set permission for fileset " + filesetPath, ioe);
    }
  }

  private void checkExternalStorageLocation(NameIdentifier ident, Path filesetPath) {
    try {
      String checkedPathString = filesetPath.toString();
      if (!this.validManagedPaths.isEmpty()) {
        // Check if the storage location is in the valid managed locations
        boolean isValidManagedPath =
            this.validManagedPaths.stream()
                .anyMatch(
                    patternString -> {
                      Pattern pattern =
                          Pattern.compile(
                              String.format(
                                  patternString,
                                  ident.namespace().level(1),
                                  ident.namespace().level(2),
                                  ident.name()));
                      Matcher matcher = pattern.matcher(checkedPathString);
                      return matcher.matches();
                    });
        // Cannot set the managed storage location for external filesets
        if (isValidManagedPath) {
          throw new RuntimeException(
              String.format(
                  "The managed storage location: %s cannot set for external filesets.",
                  checkedPathString));
        }
      }

      if (!this.validExternalPaths.isEmpty()) {
        // Check if the storage location is in the valid external locations
        boolean isValidExternalPath =
            this.validExternalPaths.stream()
                .anyMatch(
                    patternString -> {
                      Pattern pattern = Pattern.compile(patternString);
                      Matcher matcher = pattern.matcher(checkedPathString);
                      return matcher.matches();
                    });
        if (!isValidExternalPath) {
          throw new RuntimeException(
              String.format(
                  "The external storage location: %s cannot set for external filesets.",
                  checkedPathString));
        }
      }

      // Fetch an external fileset name the storage location is already mounted,
      // if it's find one, we throw an exception
      // only supports in relational entity storage
      if (store instanceof RelationalEntityStore) {
        String filesetName = store.fetchExternalFilesetName(checkedPathString);
        if (StringUtils.isNotBlank(filesetName)) {
          throw new RuntimeException(
              String.format(
                  "The storage location: %s is already mounted with an external fileset which named: %s.",
                  checkedPathString, filesetName));
        }
      }

      FileSystem fs = filesetPath.getFileSystem(hadoopConf);
      // Throw an exception if the storage location is not exist for external filesets
      if (!fs.exists(filesetPath)) {
        throw new RuntimeException(
            String.format("The storage location: %s does not exists.", filesetPath));
      }

      // Check if the external fileset mounts a single file
      FileStatus fileStatus = fs.getFileStatus(filesetPath);
      if (fileStatus != null && fileStatus.isFile()) {
        throw new RuntimeException("Cannot mount a single file with an external fileset.");
      }
      LOG.info("External fileset {} manages the existing location {}", ident, filesetPath);

    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to create external fileset " + ident + " location " + filesetPath, ioe);
    }
  }
}
