/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import static com.datastrato.gravitino.Configs.GRAVITINO_TESTING_ENABLE;
import static com.datastrato.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.CHECK_UNIQUE_STORAGE_LOCATION_SCHEME;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.EntityCombinedFileset;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.enums.FilesetBackupLocKeyPattern;
import com.datastrato.gravitino.enums.FilesetLifecycleUnit;
import com.datastrato.gravitino.enums.FilesetPrefixPattern;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.storage.relational.RelationalEntityStore;
import com.datastrato.gravitino.utils.FilesetPrefixPatternUtils;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
  private static boolean checkUniqueStorageLocationSchemeEnabled = true;

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

  private static final String SLASH = "/";

  private static final String UNDER_SCORE = "_";

  private static final String DOT = ".";

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

    checkUniqueStorageLocationSchemeEnabled =
        Boolean.parseBoolean(
            bypassConfigs.getOrDefault(CHECK_UNIQUE_STORAGE_LOCATION_SCHEME, "true"));

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

    // Check the key pattern of Fileset backup storage location
    List<String> allStorageLocations = checkStorageLocationPattern(ident, storageLocation, props);

    if (type == Fileset.Type.MANAGED) {
      checkAndCreateManagedStorageLocations(ident, allStorageLocations, props);
    } else {
      validateExternalStorageLocations(ident, storageLocation, allStorageLocations, props);
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
            .withStorageLocation(storageLocation)
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
        .withStorageLocation(storageLocation)
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

      // For managed fileset, we should delete the related files.
      if (filesetEntity.filesetType() == Fileset.Type.MANAGED) {
        try {
          deleteStorageLocations(filesetEntity);
        } catch (RuntimeException e) {
          LOG.warn("Failed to delete Fileset {} locations.", filesetEntity.nameIdentifier());
          return false;
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
  public FilesetContext getFilesetContext(NameIdentifier ident, FilesetDataOperationCtx ctx)
      throws NoSuchFilesetException {
    Preconditions.checkArgument(ctx.subPath() != null, "subPath must not be null");
    // fill the sub path with a leading slash if it does not have one
    String subPath;
    if (!ctx.subPath().trim().isEmpty() && !ctx.subPath().trim().startsWith(SLASH)) {
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
    int maxLevel = Integer.parseInt(fileset.properties().get(FilesetProperties.DIR_MAX_LEVEL_KEY));
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
      case COPY_FILE:
        Preconditions.checkArgument(
            subPath.startsWith(SLASH) && subPath.length() > 1,
            String.format(
                "subPath cannot be blank when need to operate a file or a directory with operation: %s.",
                ctx.operation()));
        Preconditions.checkArgument(
            !isMountFile,
            String.format(
                "Cannot operate the fileset: %s with operation: %s which only mounts to a single file.",
                ident, ctx.operation()));
        Preconditions.checkArgument(
            checkSubDirValid(subPath, prefixPattern, maxLevel, false),
            prefixErrorMessage(subPath, prefixPattern, maxLevel));
        break;
      default:
        break;
    }

    List<String> actualPaths;
    // subPath cannot be null, so we only need check if it is blank
    if (StringUtils.isBlank(subPath)) {
      actualPaths = getStorageLocations(fileset);
    } else {
      actualPaths =
          getStorageLocations(fileset).stream()
              .map(
                  location -> {
                    String storageLocation =
                        location.endsWith(SLASH)
                            ? location.substring(0, fileset.storageLocation().length() - 1)
                            : location;
                    return String.format("%s%s", storageLocation, subPath);
                  })
              .collect(Collectors.toList());
    }
    return HadoopFilesetContext.builder()
        .withFileset(
            EntityCombinedFileset.of(fileset)
                .withHiddenPropertiesSet(
                    fileset.properties().keySet().stream()
                        .filter(FILESET_PROPERTIES_METADATA::isHiddenProperty)
                        .collect(Collectors.toSet())))
        .withActualPaths(actualPaths.toArray(new String[0]))
        .build();
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
        FileSystem fs =
            FileSystemCache.getInstance().getFileSystem(schemaPath, hadoopConf, properties);
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
      FileSystem fs =
          FileSystemCache.getInstance().getFileSystem(schemaPath, hadoopConf, properties);
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
    String newPrimaryStorageLocation = filesetEntity.storageLocation();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) change;
        if (setProperty.getProperty().startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY)) {
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot add backup storage location property: `%s` for the fileset: `%s` through the SetProperty FilesetChange.",
                  setProperty.getProperty(), ident));
        }
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
        if (removeProperty
            .getProperty()
            .startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY)) {
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot remove backup storage location property: `%s` for the fileset: `%s` through the SetProperty FilesetChange.",
                  removeProperty.getProperty(), ident));
        }
        props.remove(removeProperty.getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        newName = ((FilesetChange.RenameFileset) change).getNewName();
      } else if (change instanceof FilesetChange.UpdateFilesetComment) {
        newComment = ((FilesetChange.UpdateFilesetComment) change).getNewComment();
      } else if (change instanceof FilesetChange.RemoveComment) {
        newComment = null;
      } else if (change instanceof FilesetChange.AddBackupStorageLocation) {
        FilesetChange.AddBackupStorageLocation addBackupStorageLocation =
            (FilesetChange.AddBackupStorageLocation) change;
        Map<String, String> newProps =
            addBackupStorageLocation(filesetEntity, addBackupStorageLocation);
        props.putAll(newProps);
      } else if (change instanceof FilesetChange.RemoveBackupStorageLocation) {
        FilesetChange.RemoveBackupStorageLocation removeBackupStorageLocation =
            (FilesetChange.RemoveBackupStorageLocation) change;
        removeBackupStorageLocation(filesetEntity, props, removeBackupStorageLocation);
      } else if (change instanceof FilesetChange.UpdateBackupStorageLocation) {
        FilesetChange.UpdateBackupStorageLocation updateBackupStorageLocation =
            (FilesetChange.UpdateBackupStorageLocation) change;
        Map<String, String> newProps =
            updateBackupStorageLocation(filesetEntity, updateBackupStorageLocation);
        props.putAll(newProps);
      } else if (change instanceof FilesetChange.UpdatePrimaryStorageLocation) {
        FilesetChange.UpdatePrimaryStorageLocation updatePrimaryStorageLocation =
            (FilesetChange.UpdatePrimaryStorageLocation) change;
        newPrimaryStorageLocation =
            updatePrimaryStorageLocation(filesetEntity, updatePrimaryStorageLocation);
      } else if (change instanceof FilesetChange.SwitchBackupStorageLocation) {
        FilesetChange.SwitchBackupStorageLocation switchBackupStorageLocation =
            (FilesetChange.SwitchBackupStorageLocation) change;
        Map<String, String> newProps =
            switchBackupStorageLocation(filesetEntity, switchBackupStorageLocation);
        props.putAll(newProps);
      } else if (change instanceof FilesetChange.SwitchPrimaryAndBackupStorageLocation) {
        FilesetChange.SwitchPrimaryAndBackupStorageLocation switchPrimaryAndBackupStorageLocation =
            (FilesetChange.SwitchPrimaryAndBackupStorageLocation) change;
        Pair<String, Map<String, String>> primaryAndBackupStorageLocation =
            switchPrimaryAndBackupStorageLocation(
                filesetEntity, switchPrimaryAndBackupStorageLocation);
        newPrimaryStorageLocation = primaryAndBackupStorageLocation.getLeft();
        props.putAll(primaryAndBackupStorageLocation.getRight());
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
        .withStorageLocation(newPrimaryStorageLocation)
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

  private List<String> getBackupStorageLocations(Map<String, String> props) {
    return props.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  private List<String> checkStorageLocationPattern(
      NameIdentifier identifier, String primaryStorageLocation, Map<String, String> properties) {
    String backupLocKeyRegex = FilesetBackupLocKeyPattern.NUMERIC_SUFFIX.getBackupLocKeyRegex();
    Pattern backupLocKeyPattern = Pattern.compile(backupLocKeyRegex);
    List<String> backupStorageLocations =
        properties.entrySet().stream()
            .filter(
                entry -> entry.getKey().startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY))
            .map(
                entry -> {
                  String backupStorageLocationKey = entry.getKey();
                  String backupStorageLocation = entry.getValue();
                  Preconditions.checkArgument(
                      StringUtils.isNotBlank(backupStorageLocation),
                      "The new added backup storage location can not be null or empty.");
                  if (!backupLocKeyPattern.matcher(backupStorageLocationKey).matches()) {
                    throw new IllegalArgumentException(
                        String.format(
                            "Invalid backup storage location key: %s for Fileset: %s, its pattern should be `%s`.",
                            backupStorageLocationKey, identifier, backupLocKeyRegex));
                  }
                  return backupStorageLocation;
                })
            .collect(Collectors.toList());

    // Check if the storage location scheme is unique by default, and you can set it to false in UT.
    if (checkUniqueStorageLocationSchemeEnabled) {
      Set<String> storageLocationSchemes =
          backupStorageLocations.stream()
              .map(
                  location -> {
                    Path path = new Path(location);
                    return path.toUri().getScheme();
                  })
              .collect(Collectors.toSet());

      String primaryStorageLocationSchema = new Path(primaryStorageLocation).toUri().getScheme();
      storageLocationSchemes.add(primaryStorageLocationSchema);

      Preconditions.checkArgument(
          backupStorageLocations.size() + 1 == storageLocationSchemes.size(),
          "The scheme of each storage location must be unique.");
    }

    List<String> allStorageLocations = new ArrayList<>();
    allStorageLocations.add(primaryStorageLocation);
    allStorageLocations.addAll(backupStorageLocations);
    return allStorageLocations;
  }

  private void checkAndCreateManagedStorageLocations(
      NameIdentifier ident, List<String> allStorageLocations, Map<String, String> properties) {
    try {
      checkValidManagedPaths(ident, allStorageLocations);
      createManagedStorageLocations(ident, allStorageLocations, properties);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create managed fileset: %s.", ident), e);
    }
  }

  private void setPathPermission(FileSystem fs, Path filesetPath) {
    if (fs instanceof LocalFileSystem) {
      // skip unit test
      return;
    }

    if (GravitinoEnv.getInstance().config().get(GRAVITINO_TESTING_ENABLE)) {
      // skip integration test
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
    } catch (UnsupportedOperationException uoe) {
      LOG.warn(
          "The filesystem: {} doesn't support modifyAclEntries for path: {}",
          fs.getClass(),
          filesetPath,
          uoe);
    }
  }

  private void validateExternalStorageLocations(
      NameIdentifier ident,
      String primaryStorageLocation,
      List<String> allStorageLocations,
      Map<String, String> properties) {
    checkValidExternalPaths(ident, allStorageLocations);
    checkExternalStorageLocations(ident, primaryStorageLocation, allStorageLocations, properties);
  }

  private boolean validateStorageLocationPattern(NameIdentifier ident, String storageLocation) {
    return this.validManagedPaths.stream()
        .anyMatch(
            patternString -> {
              Pattern pattern =
                  Pattern.compile(
                      String.format(
                          patternString,
                          ident.namespace().level(1),
                          ident.namespace().level(2),
                          ident.name()));
              Matcher matcher = pattern.matcher(storageLocation);
              return matcher.matches();
            });
  }

  private void checkValidManagedPaths(NameIdentifier ident, List<String> allStorageLocations) {
    if (!this.validManagedPaths.isEmpty()) {
      // Check if the primary and backup storage locations are set in the valid managed locations
      allStorageLocations.forEach(
          location -> {
            boolean isValidManagedPath = validateStorageLocationPattern(ident, location);
            if (!isValidManagedPath) {
              throw new RuntimeException(
                  String.format(
                      "The storage location: %s is not valid for managed fileset: %s.%s.%s.",
                      location,
                      ident.namespace().level(1),
                      ident.namespace().level(2),
                      ident.name()));
            }
          });
    }
  }

  private void checkValidExternalPaths(NameIdentifier ident, List<String> allStorageLocations) {
    if (!this.validManagedPaths.isEmpty()) {
      // Check if the primary and backup storage locations are in the valid managed locations
      allStorageLocations.forEach(
          location -> {
            boolean isValidManagedPath = validateStorageLocationPattern(ident, location);
            // Cannot set the managed storage location for external filesets
            if (isValidManagedPath) {
              throw new RuntimeException(
                  String.format(
                      "The managed storage location: %s cannot be set for external filesets.",
                      location));
            }
          });
    }

    if (!this.validExternalPaths.isEmpty()) {
      // Check if the primary and backup storage locations are in the valid external locations
      allStorageLocations.forEach(
          location -> {
            boolean isValidExternalPath =
                this.validExternalPaths.stream()
                    .anyMatch(
                        patternString -> {
                          Pattern pattern = Pattern.compile(patternString);
                          Matcher matcher = pattern.matcher(location);
                          return matcher.matches();
                        });
            if (!isValidExternalPath) {
              throw new RuntimeException(
                  String.format(
                      "The external storage location: %s cannot set for external filesets.",
                      location));
            }
          });
    }
  }

  private void createManagedStorageLocations(
      NameIdentifier ident, List<String> allStorageLocations, Map<String, String> properties) {
    allStorageLocations.forEach(
        location -> {
          Path locationPath = new Path(location);
          try {
            FileSystem fs =
                FileSystemCache.getInstance().getFileSystem(locationPath, hadoopConf, properties);
            if (!fs.exists(locationPath)) {
              if (!fs.mkdirs(locationPath)) {
                throw new RuntimeException(
                    String.format(
                        "Failed to create location: %s for Fileset: %s.", ident, location));
              }
            } else {
              LOG.warn("Using existing managed location: `{}` for Fileset: `{}`", location, ident);
            }
            LOG.info("Created managed location {} for Fileset {}.", ident, location);

            setPathPermission(fs, locationPath);
          } catch (IOException e) {
            throw new RuntimeException(
                String.format(
                    "Failed to create managed location: %s for Fileset: %s.", location, ident),
                e);
          }
        });
  }

  private void checkExternalStorageLocations(
      NameIdentifier ident,
      String primaryStorageLocation,
      List<String> allStorageLocations,
      Map<String, String> properties) {

    // Fetch an external fileset name the storage location is already mounted,
    // if it's find one, we throw an exception
    // only supports in relational entity storage
    allStorageLocations.forEach(
        location -> {
          if (store instanceof RelationalEntityStore) {
            String filesetName = store.fetchExternalFilesetName(location);
            if (StringUtils.isNotBlank(filesetName)) {
              throw new RuntimeException(
                  String.format(
                      "The storage location: %s is already mounted with an external fileset which named: %s.",
                      location, filesetName));
            }
          }

          try {
            Path locationPath = new Path(location);
            FileSystem fs =
                FileSystemCache.getInstance().getFileSystem(locationPath, hadoopConf, properties);
            // Throw an exception if the storage location is not exist for external filesets
            if (!fs.exists(locationPath)) {
              if (!primaryStorageLocation.equals(location)) {
                LOG.info(
                    String.format(
                        "The backup storage location: %s of external Fileset does not exists, to create it.",
                        locationPath));
                if (fs.mkdirs(locationPath)) {
                  setPathPermission(fs, locationPath);
                  LOG.info(
                      String.format(
                          "The backup storage location: %s of external Fileset is created.",
                          locationPath));
                } else {
                  throw new RuntimeException(
                      String.format(
                          "The backup storage location: %s of external Fileset cannot be created.",
                          locationPath));
                }
              } else {
                throw new RuntimeException(
                    String.format(
                        "The primary storage location: %s of external Fileset does not exists.",
                        locationPath));
              }
            }

            // Check if the external fileset mounts a single file
            FileStatus fileStatus = fs.getFileStatus(locationPath);
            if (fileStatus != null && fileStatus.isFile()) {
              throw new RuntimeException("Cannot mount a single file with an external fileset.");
            }
            LOG.info("External fileset {} manages the existing location {}", ident, locationPath);
          } catch (IOException e) {
            throw new RuntimeException("Check external Fileset storage location failed.");
          }
        });
  }

  private Map<String, String> addBackupStorageLocation(
      FilesetEntity filesetEntity,
      FilesetChange.AddBackupStorageLocation addBackupStorageLocation) {
    NameIdentifier ident = filesetEntity.nameIdentifier();
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());

    String backupStorageLocationKey = addBackupStorageLocation.getBackupStorageLocationKey();
    String backupStorageLocationValue = addBackupStorageLocation.getBackupStorageLocationValue();

    HashMap<String, String> toCheckedProperties = Maps.newHashMap();
    toCheckedProperties.put(backupStorageLocationKey, backupStorageLocationValue);
    List<String> allStorageLocations =
        checkStorageLocationPattern(ident, filesetEntity.storageLocation(), toCheckedProperties);

    Preconditions.checkArgument(
        allStorageLocations.size() == 2
            && filesetEntity.storageLocation().equals(allStorageLocations.get(0)),
        "The pattern of the new backup storage location key or value is incorrect.");
    // remove the primary storage location
    allStorageLocations.remove(filesetEntity.storageLocation());

    if (props.containsKey(backupStorageLocationKey)) {
      throw new IllegalArgumentException(
          String.format(
              "Can not add an existing backup storage location key: %s for Fileset: %s",
              backupStorageLocationKey, ident));
    }

    if (filesetEntity.filesetType() == Fileset.Type.MANAGED) {
      checkAndCreateManagedStorageLocations(ident, allStorageLocations, toCheckedProperties);
    } else {
      validateExternalStorageLocations(
          ident, filesetEntity.storageLocation(), allStorageLocations, toCheckedProperties);
    }

    props.put(backupStorageLocationKey, backupStorageLocationValue);
    LOG.info(
        "Add backup storage location key {} value {} for Fileset {} successfully.",
        backupStorageLocationKey,
        backupStorageLocationValue,
        ident);
    return props;
  }

  private void removeBackupStorageLocation(
      FilesetEntity filesetEntity,
      Map<String, String> props,
      FilesetChange.RemoveBackupStorageLocation removeBackupStorageLocation) {
    NameIdentifier ident = filesetEntity.nameIdentifier();

    String backupStorageLocationKey = removeBackupStorageLocation.getBackupStorageLocationKey();

    if (!props.containsKey(backupStorageLocationKey)) {
      throw new IllegalArgumentException(
          String.format(
              "Attempt to remove a non-existing backup storage location key: %s for Fileset: %s",
              backupStorageLocationKey, ident));
    }

    String backupStorageLocation = props.get(backupStorageLocationKey);
    try {
      Path backupStorageLocationPath = new Path(backupStorageLocation);
      FileSystem fs =
          FileSystemCache.getInstance().getFileSystem(backupStorageLocationPath, hadoopConf, props);
      if (!fs.exists(backupStorageLocationPath)) {
        LOG.warn(
            "The backup storage location of the key to be removed {} for Fileset {} does not exist.",
            backupStorageLocationKey,
            ident);
      } else if (fs.listStatus(backupStorageLocationPath).length > 0) {
        throw new UnsupportedOperationException(
            String.format(
                "Can not remove a backup storage location key: %s where data exists for Fileset: %s.",
                backupStorageLocationKey, ident));
      }

      props.remove(backupStorageLocationKey);
      LOG.info(
          "Remove backup storage location key {} for Fileset {} successfully.",
          backupStorageLocationKey,
          ident);
    } catch (IOException ioe) {
      throw new RuntimeException(
          String.format(
              "Failed to remove backup storage location key: %s for Fileset: %s",
              backupStorageLocationKey, ident),
          ioe);
    }
  }

  private Map<String, String> updateBackupStorageLocation(
      FilesetEntity filesetEntity,
      FilesetChange.UpdateBackupStorageLocation updateBackupStorageLocation) {
    NameIdentifier ident = filesetEntity.nameIdentifier();
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());

    String backupStorageLocationKey = updateBackupStorageLocation.getBackupStorageLocationKey();

    if (!props.containsKey(backupStorageLocationKey)) {
      throw new IllegalArgumentException(
          String.format(
              "Attempt to update a non-existing backup storage location key: %s for Fileset: %s",
              backupStorageLocationKey, ident));
    }

    String backupStorageLocation = props.get(backupStorageLocationKey);
    try {
      Path backupStorageLocationPath = new Path(backupStorageLocation);
      FileSystem fs =
          FileSystemCache.getInstance().getFileSystem(backupStorageLocationPath, hadoopConf, props);
      if (!fs.exists(backupStorageLocationPath)) {
        LOG.warn(
            "The backup storage location of the key to be updated {} for Fileset {} does not exist.",
            backupStorageLocationKey,
            ident);
      } else if (fs.listStatus(backupStorageLocationPath).length > 0) {
        throw new UnsupportedOperationException(
            String.format(
                "Can not update a backup storage location key: %s where data exists for Fileset: %s.",
                backupStorageLocationKey, ident));
      }
    } catch (IOException ioe) {
      throw new RuntimeException(
          String.format(
              "Failed to check backup storage location key: %s for Fileset: %s",
              backupStorageLocationKey, ident),
          ioe);
    }

    String backupStorageLocationNewValue =
        updateBackupStorageLocation.getBackupStorageLocationNewValue();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(backupStorageLocationNewValue),
        "The backup storage location can not be null or empty.");
    Preconditions.checkArgument(
        !backupStorageLocationNewValue.equalsIgnoreCase(backupStorageLocation),
        String.format(
            "The new backup storage location should be different from the old one, new: %s, old: %s.",
            backupStorageLocationNewValue, backupStorageLocation));
    List<String> backupStorageLocations = Collections.singletonList(backupStorageLocationNewValue);

    if (filesetEntity.filesetType() == Fileset.Type.MANAGED) {
      checkAndCreateManagedStorageLocations(ident, backupStorageLocations, props);
    } else {
      validateExternalStorageLocations(
          ident, filesetEntity.storageLocation(), backupStorageLocations, props);
    }

    props.put(
        backupStorageLocationKey, updateBackupStorageLocation.getBackupStorageLocationNewValue());
    LOG.info(
        "Update backup storage location key {} with new value {} for Fileset {} successfully.",
        backupStorageLocationKey,
        updateBackupStorageLocation.getBackupStorageLocationNewValue(),
        ident);
    return props;
  }

  private String updatePrimaryStorageLocation(
      FilesetEntity filesetEntity,
      FilesetChange.UpdatePrimaryStorageLocation updatePrimaryStorageLocation) {
    NameIdentifier ident = filesetEntity.nameIdentifier();
    String oldPrimaryStorageLocation = filesetEntity.storageLocation();
    String newPrimaryStorageLocation = updatePrimaryStorageLocation.getNewPrimaryStorageLocation();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(newPrimaryStorageLocation),
        "The new primary storage location can not be null or empty.");
    Preconditions.checkArgument(
        !oldPrimaryStorageLocation.equalsIgnoreCase(newPrimaryStorageLocation),
        String.format(
            "The new primary storage location should be different from the old one, new: %s, old: %s.",
            newPrimaryStorageLocation, oldPrimaryStorageLocation));

    try {
      Path oldPrimaryStorageLocationPath = new Path(oldPrimaryStorageLocation);
      FileSystem fs =
          FileSystemCache.getInstance()
              .getFileSystem(oldPrimaryStorageLocationPath, hadoopConf, filesetEntity.properties());
      if (!fs.exists(oldPrimaryStorageLocationPath)) {
        LOG.warn(
            "The old primary storage location to be updated {} for Fileset {} does not exist.",
            oldPrimaryStorageLocation,
            ident);
      } else if (fs.listStatus(oldPrimaryStorageLocationPath).length > 0) {
        throw new UnsupportedOperationException(
            String.format(
                "Can not update a primary storage location: %s where data exists for Fileset: %s.",
                oldPrimaryStorageLocation, ident));
      }
    } catch (IOException ioe) {
      throw new RuntimeException(
          String.format(
              "Failed to check the old primary storage location: %s for Fileset: %s",
              oldPrimaryStorageLocation, ident),
          ioe);
    }

    List<String> newPrimaryStorageLocations = Collections.singletonList(newPrimaryStorageLocation);

    if (filesetEntity.filesetType() == Fileset.Type.MANAGED) {
      checkAndCreateManagedStorageLocations(
          ident, newPrimaryStorageLocations, filesetEntity.properties());
    } else {
      validateExternalStorageLocations(
          ident, newPrimaryStorageLocation, newPrimaryStorageLocations, filesetEntity.properties());
    }
    LOG.info(
        "Update the old primary storage location {} with new value {} for Fileset {} successfully.",
        oldPrimaryStorageLocation,
        newPrimaryStorageLocation,
        ident);
    return newPrimaryStorageLocation;
  }

  private Map<String, String> switchBackupStorageLocation(
      FilesetEntity filesetEntity,
      FilesetChange.SwitchBackupStorageLocation switchBackupStorageLocation) {
    NameIdentifier ident = filesetEntity.nameIdentifier();
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());

    String firstBackupStorageLocationKey =
        switchBackupStorageLocation.getFirstBackupStorageLocationKey();
    String secondBackupStorageLocationKey =
        switchBackupStorageLocation.getSecondBackupStorageLocationKey();
    Preconditions.checkArgument(
        props.containsKey(firstBackupStorageLocationKey)
            && props.containsKey(secondBackupStorageLocationKey),
        String.format(
            "The backup storage location keys %s or %s is not found in the properties of Fileset: %s",
            firstBackupStorageLocationKey, secondBackupStorageLocationKey, ident));
    String firstBackupStorageLocationValue = props.get(firstBackupStorageLocationKey);
    String secondBackupStorageLocationValue = props.get(secondBackupStorageLocationKey);

    props.put(firstBackupStorageLocationKey, secondBackupStorageLocationValue);
    props.put(secondBackupStorageLocationKey, firstBackupStorageLocationValue);
    LOG.info(
        "Switch the backup storage locations between first key {} and the second key {} for Fileset {} successfully.",
        firstBackupStorageLocationKey,
        secondBackupStorageLocationKey,
        ident);
    return props;
  }

  private Pair<String, Map<String, String>> switchPrimaryAndBackupStorageLocation(
      FilesetEntity filesetEntity,
      FilesetChange.SwitchPrimaryAndBackupStorageLocation switchPrimaryAndBackupStorageLocation) {
    NameIdentifier ident = filesetEntity.nameIdentifier();
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());

    String primaryStorageLocation = filesetEntity.storageLocation();
    String backupStorageLocationKey =
        switchPrimaryAndBackupStorageLocation.getBackupStorageLocationKey();

    Preconditions.checkArgument(
        props.containsKey(backupStorageLocationKey),
        String.format(
            "The backup storage location keys %s is not found in the properties of Fileset: %s",
            backupStorageLocationKey, ident));
    String backupStorageLocation = props.get(backupStorageLocationKey);
    props.put(backupStorageLocationKey, primaryStorageLocation);
    LOG.info(
        "Switch the primary and backup storage location key {} for Fileset {} successfully.",
        backupStorageLocationKey,
        ident);
    return Pair.of(backupStorageLocation, props);
  }

  private void deleteStorageLocations(FilesetEntity filesetEntity) {
    Preconditions.checkArgument(
        filesetEntity.filesetType() == Fileset.Type.MANAGED,
        "Only the locations of managed filesets can be deleted.");

    List<String> backupStorageLocations = getBackupStorageLocations(filesetEntity.properties());
    backupStorageLocations.add(filesetEntity.storageLocation());

    backupStorageLocations.forEach(
        location -> {
          try {
            Path storageLocationPath = new Path(location);
            FileSystem fs =
                FileSystemCache.getInstance()
                    .getFileSystem(storageLocationPath, hadoopConf, filesetEntity.properties());
            if (fs.exists(storageLocationPath)) {
              fs.delete(storageLocationPath, true);
            } else {
              LOG.warn(
                  "Fileset {} location {} does not exist",
                  filesetEntity.nameIdentifier(),
                  location);
            }
          } catch (IOException e) {
            throw new RuntimeException("Failed to delete storage location " + location, e);
          }
        });
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
      Path locationPath = new Path(fileset.storageLocation());
      FileSystem fs =
          FileSystemCache.getInstance()
              .getFileSystem(locationPath, hadoopConf, fileset.properties());
      return fs.getFileStatus(locationPath).isFile();
    } catch (FileNotFoundException e) {
      // We should always return false here, same with the logic in `FileSystem.isFile(Path f)`.
      return false;
    } catch (IOException e) {
      throw new GravitinoRuntimeException(
          "Cannot check whether the fileset: %s mounts a single file, exception: %s",
          fileset.name(), e);
    }
  }

  private static String prefixErrorMessage(
      String subPath, FilesetPrefixPattern pattern, Integer maxLevel) {
    return String.format(
        "The sub Path: %s is not valid, the whole path should like `%s`,"
            + " and max sub directory level after fileset identifier should be less than %d.",
        subPath, pattern.getExample(), maxLevel);
  }

  @VisibleForTesting
  static List<String> getStorageLocations(Fileset fileset) {
    return Stream.concat(
            Stream.of(fileset.storageLocation()),
            fileset.properties().entrySet().stream()
                .filter(
                    entry ->
                        entry.getKey().startsWith(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY))
                .sorted(
                    (loc1, loc2) -> {
                      String priority1 =
                          loc1.getKey().replace(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY, "");
                      String priority2 =
                          loc2.getKey().replace(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY, "");
                      return Integer.parseInt(priority1) - Integer.parseInt(priority2);
                    })
                .map(Map.Entry::getValue))
        .collect(Collectors.toList());
  }
}
