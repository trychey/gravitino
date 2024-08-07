/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.file.FilesetContextDTO;
import com.datastrato.gravitino.dto.file.FilesetDTO;
import com.datastrato.gravitino.dto.requests.CatalogCreateRequest;
import com.datastrato.gravitino.dto.requests.FilesetCreateRequest;
import com.datastrato.gravitino.dto.requests.FilesetUpdateRequest;
import com.datastrato.gravitino.dto.requests.FilesetUpdatesRequest;
import com.datastrato.gravitino.dto.requests.GetFilesetContextRequest;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.FilesetContextResponse;
import com.datastrato.gravitino.dto.responses.FilesetResponse;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestFilesetCatalog extends TestBase {

  protected static Catalog catalog;

  private static GravitinoMetalake metalake;

  protected static final String metalakeName = "testMetalake";

  protected static final String catalogName = "testCatalog";

  private static final String provider = "test";

  private static final String BACKUP_STORAGE_LOCATION_KEY = "backup.storage.location.";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    metalake = TestGravitinoMetalake.createMetalake(client, metalakeName);

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(catalogName)
            .withType(CatalogDTO.Type.FILESET)
            .withProvider(provider)
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "k2"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    CatalogCreateRequest catalogCreateRequest =
        new CatalogCreateRequest(
            catalogName, CatalogDTO.Type.FILESET, provider, "comment", ImmutableMap.of("k1", "k2"));
    CatalogResponse catalogResponse = new CatalogResponse(mockCatalog);
    buildMockResource(
        Method.POST,
        "/api/metalakes/" + metalakeName + "/catalogs",
        catalogCreateRequest,
        catalogResponse,
        SC_OK);

    catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            CatalogDTO.Type.FILESET,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
  }

  @Test
  public void testListFileset() throws JsonProcessingException {
    NameIdentifier fileset1 = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset1");
    NameIdentifier fileset2 = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset2");
    String filesetPath = withSlash(FilesetCatalog.formatFilesetRequestPath(fileset1.namespace()));

    EntityListResponse resp = new EntityListResponse(new NameIdentifier[] {fileset1, fileset2});
    buildMockResource(Method.GET, filesetPath, null, resp, SC_OK);
    NameIdentifier[] filesets = catalog.asFilesetCatalog().listFilesets(fileset1.namespace());

    Assertions.assertEquals(2, filesets.length);
    Assertions.assertEquals(fileset1, filesets[0]);
    Assertions.assertEquals(fileset2, filesets[1]);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, filesetPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asFilesetCatalog().listFilesets(fileset1.namespace()),
        "schema not found");

    // Throw fileset not found exception
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchFileException.class.getSimpleName(), "fileset not found");
    buildMockResource(Method.GET, filesetPath, null, errResp1, SC_NOT_FOUND);
    Assertions.assertThrows(
        NotFoundException.class,
        () -> catalog.asFilesetCatalog().listFilesets(fileset1.namespace()),
        "fileset not found");

    // Throw Runtime exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, filesetPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asFilesetCatalog().listFilesets(fileset1.namespace()),
        "internal error");
  }

  @Test
  public void testLoadFileset() throws JsonProcessingException {
    NameIdentifier fileset = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset1");
    String filesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(fileset.namespace()) + "/fileset1");

    FilesetDTO mockFileset =
        mockFilesetDTO(
            fileset.name(),
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    FilesetResponse resp = new FilesetResponse(mockFileset);
    buildMockResource(Method.GET, filesetPath, null, resp, SC_OK);
    Fileset loadedFileset = catalog.asFilesetCatalog().loadFileset(fileset);
    Assertions.assertNotNull(loadedFileset);
    assertFileset(mockFileset, loadedFileset);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, filesetPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asFilesetCatalog().loadFileset(fileset),
        "schema not found");

    ErrorResponse errResp1 =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "fileset not found");
    buildMockResource(Method.GET, filesetPath, null, errResp1, SC_NOT_FOUND);
    Assertions.assertThrows(
        NotFoundException.class,
        () -> catalog.asFilesetCatalog().loadFileset(fileset),
        "fileset not found");

    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, filesetPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asFilesetCatalog().loadFileset(fileset),
        "internal error");
  }

  @Test
  public void testCreateFileset() throws JsonProcessingException {
    NameIdentifier fileset = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset1");
    String filesetPath = withSlash(FilesetCatalog.formatFilesetRequestPath(fileset.namespace()));

    FilesetDTO mockFileset =
        mockFilesetDTO(
            fileset.name(),
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name(fileset.name())
            .type(Fileset.Type.MANAGED)
            .comment("mock comment")
            .storageLocation("mock location")
            .properties(ImmutableMap.of("k1", "v1"))
            .build();
    FilesetResponse resp = new FilesetResponse(mockFileset);
    buildMockResource(Method.POST, filesetPath, req, resp, SC_OK);
    Fileset loadedFileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                fileset,
                "mock comment",
                Fileset.Type.MANAGED,
                "mock location",
                ImmutableMap.of("k1", "v1"));
    Assertions.assertNotNull(loadedFileset);
    assertFileset(mockFileset, loadedFileset);

    // Test FilesetAlreadyExistsException
    ErrorResponse errResp =
        ErrorResponse.alreadyExists(
            FilesetAlreadyExistsException.class.getSimpleName(), "fileset already exists");
    buildMockResource(Method.POST, filesetPath, req, errResp, SC_CONFLICT);
    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(
                    fileset,
                    "mock comment",
                    Fileset.Type.MANAGED,
                    "mock location",
                    ImmutableMap.of("k1", "v1")),
        "fileset already exists");

    // Test RuntimeException
    ErrorResponse errResp1 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, filesetPath, req, errResp1, SC_CONFLICT);
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(
                    fileset,
                    "mock comment",
                    Fileset.Type.MANAGED,
                    "mock location",
                    ImmutableMap.of("k1", "v1")),
        "internal error");
  }

  @Test
  public void testDropFileset() throws JsonProcessingException {
    NameIdentifier fileset = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset1");
    String filesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(fileset.namespace()) + "/fileset1");

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, filesetPath, null, resp, SC_OK);
    boolean dropped = catalog.asFilesetCatalog().dropFileset(fileset);
    Assertions.assertTrue(dropped);

    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, filesetPath, null, resp1, SC_OK);
    boolean dropped1 = catalog.asFilesetCatalog().dropFileset(fileset);
    Assertions.assertFalse(dropped1);

    // Test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, filesetPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asFilesetCatalog().dropFileset(fileset),
        "internal error");
  }

  @Test
  public void testAlterFileset() throws JsonProcessingException {
    NameIdentifier fileset = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset1");
    String filesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(fileset.namespace()) + "/fileset1");

    // Test alter fileset name
    FilesetUpdateRequest req = new FilesetUpdateRequest.RenameFilesetRequest("new name");
    FilesetDTO mockFileset =
        mockFilesetDTO(
            "new name",
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    FilesetResponse resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(req)), resp, SC_OK);
    Fileset res = catalog.asFilesetCatalog().alterFileset(fileset, req.filesetChange());
    assertFileset(mockFileset, res);

    // Test alter fileset comment
    FilesetUpdateRequest req1 = new FilesetUpdateRequest.UpdateFilesetCommentRequest("new comment");
    FilesetDTO mockFileset1 =
        mockFilesetDTO(
            "new name",
            Fileset.Type.MANAGED,
            "new comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    FilesetResponse resp1 = new FilesetResponse(mockFileset1);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(req1)), resp1, SC_OK);
    Fileset res1 = catalog.asFilesetCatalog().alterFileset(fileset, req1.filesetChange());
    assertFileset(mockFileset1, res1);

    // Test set fileset properties
    FilesetUpdateRequest req2 = new FilesetUpdateRequest.SetFilesetPropertiesRequest("k2", "v2");
    FilesetDTO mockFileset2 =
        mockFilesetDTO(
            "new name",
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1", "k2", "v2"));
    FilesetResponse resp2 = new FilesetResponse(mockFileset2);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(req2)), resp2, SC_OK);
    Fileset res2 = catalog.asFilesetCatalog().alterFileset(fileset, req2.filesetChange());
    assertFileset(mockFileset2, res2);

    // Test remove fileset properties
    FilesetUpdateRequest req3 = new FilesetUpdateRequest.RemoveFilesetPropertiesRequest("k1");
    FilesetDTO mockFileset3 =
        mockFilesetDTO(
            "new name", Fileset.Type.MANAGED, "mock comment", "mock location", ImmutableMap.of());
    FilesetResponse resp3 = new FilesetResponse(mockFileset3);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(req3)), resp3, SC_OK);
    Fileset res3 = catalog.asFilesetCatalog().alterFileset(fileset, req3.filesetChange());
    assertFileset(mockFileset3, res3);

    // Test remove fileset comment
    FilesetUpdateRequest req4 = new FilesetUpdateRequest.RemoveFilesetCommentRequest();
    FilesetDTO mockFileset4 =
        mockFilesetDTO("new name", Fileset.Type.MANAGED, null, "mock location", ImmutableMap.of());
    FilesetResponse resp4 = new FilesetResponse(mockFileset4);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(req4)), resp4, SC_OK);
    Fileset res4 = catalog.asFilesetCatalog().alterFileset(fileset, req4.filesetChange());
    assertFileset(mockFileset4, res4);

    // Test NoSuchFilesetException
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchFilesetException.class.getSimpleName(), "fileset not found");
    buildMockResource(
        Method.PUT,
        filesetPath,
        new FilesetUpdatesRequest(ImmutableList.of(req)),
        errResp,
        SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchFilesetException.class,
        () -> catalog.asFilesetCatalog().alterFileset(fileset, req.filesetChange()),
        "fileset not found");

    // Test RuntimeException
    ErrorResponse errResp1 = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.PUT,
        filesetPath,
        new FilesetUpdatesRequest(ImmutableList.of(req)),
        errResp1,
        SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asFilesetCatalog().alterFileset(fileset, req.filesetChange()),
        "internal error");
  }

  @Test
  public void testGetFilesetContext() throws JsonProcessingException {
    NameIdentifier fileset = NameIdentifier.of(metalakeName, catalogName, "schema1", "fileset1");
    String filesetPath =
        withSlash(
            FilesetCatalog.formatFilesetRequestPath(fileset.namespace()) + "/fileset1/context");

    FilesetDTO mockFileset =
        mockFilesetDTO(
            fileset.name(),
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    String[] mockActualPaths = new String[] {"mock location/test"};
    FilesetContextDTO mockContext = mockFilesetContextDTO(mockFileset, mockActualPaths);
    FilesetContextResponse resp = new FilesetContextResponse(mockContext);
    Map<String, String> extraInfo = Maps.newHashMap();
    extraInfo.put("mock key", "mock value");
    GetFilesetContextRequest req =
        GetFilesetContextRequest.builder()
            .subPath("/test")
            .operation(FilesetDataOperation.CREATE)
            .clientType(ClientType.HADOOP_GVFS)
            .ip("127.0.0.1")
            .sourceEngineType(SourceEngineType.SPARK)
            .appId("application_1_1")
            .extraInfo(extraInfo)
            .build();
    buildMockResource(Method.POST, filesetPath, req, resp, SC_OK);
    BaseFilesetDataOperationCtx ctx =
        BaseFilesetDataOperationCtx.builder()
            .withSubPath("/test")
            .withOperation(FilesetDataOperation.CREATE)
            .withClientType(ClientType.HADOOP_GVFS)
            .withIp("127.0.0.1")
            .withSourceEngineType(SourceEngineType.SPARK)
            .withAppId("application_1_1")
            .withExtraInfo(extraInfo)
            .build();
    FilesetContext filesetContext = catalog.asFilesetCatalog().getFilesetContext(fileset, ctx);
    Assertions.assertNotNull(filesetContext);
    assertFileset(mockFileset, filesetContext.fileset());

    Assertions.assertEquals(mockActualPaths[0], filesetContext.actualPaths()[0]);
  }

  @ParameterizedTest
  @MethodSource("filesetTypes")
  public void testCreateLoadAndDropFilesetWithMultipleLocs(Fileset.Type type)
      throws JsonProcessingException {
    String schemaName = "schema1_" + type.name();
    String filesetName = "fileset1_" + type.name();
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    String filesetPath = withSlash(FilesetCatalog.formatFilesetRequestPath(ident.namespace()));

    String comment = "mock comment";
    String storageLocation = "mock location";
    String backStorageLocation = "mock bak location";
    Map<String, String> properties =
        ImmutableMap.of("k1", "v1", BACKUP_STORAGE_LOCATION_KEY + 1, backStorageLocation);

    // Create fileset
    FilesetDTO mockFileset =
        mockFilesetDTO(ident.name(), type, comment, storageLocation, properties);
    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name(ident.name())
            .type(type)
            .comment(comment)
            .storageLocation(storageLocation)
            .properties(properties)
            .build();
    FilesetResponse resp = new FilesetResponse(mockFileset);
    buildMockResource(Method.POST, filesetPath, req, resp, SC_OK);
    Fileset loadedFileset =
        catalog.asFilesetCatalog().createFileset(ident, comment, type, storageLocation, properties);
    Assertions.assertNotNull(loadedFileset);
    assertFileset(mockFileset, loadedFileset);

    // Load fileset
    filesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(ident.namespace()) + "/" + filesetName);

    mockFileset = mockFilesetDTO(ident.name(), type, comment, storageLocation, properties);
    resp = new FilesetResponse(mockFileset);
    buildMockResource(Method.GET, filesetPath, null, resp, SC_OK);
    loadedFileset = catalog.asFilesetCatalog().loadFileset(ident);
    Assertions.assertNotNull(loadedFileset);
    assertFileset(mockFileset, loadedFileset);

    // Drop fileset
    filesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(ident.namespace()) + "/" + filesetName);

    DropResponse dropResponse = new DropResponse(true);
    buildMockResource(Method.DELETE, filesetPath, null, dropResponse, SC_OK);
    boolean dropped = catalog.asFilesetCatalog().dropFileset(ident);
    Assertions.assertTrue(dropped);

    DropResponse dropResponse1 = new DropResponse(false);
    buildMockResource(Method.DELETE, filesetPath, null, dropResponse1, SC_OK);
    boolean dropped1 = catalog.asFilesetCatalog().dropFileset(ident);
    Assertions.assertFalse(dropped1);
  }

  @ParameterizedTest
  @MethodSource("filesetTypes")
  public void testStorageLocationFilesetChanges(Fileset.Type type) throws JsonProcessingException {
    String schemaName = "schema2_" + type.name();
    String filesetName = "fileset1_" + type.name();
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    String filesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(ident.namespace()) + "/" + filesetName);

    String comment = "mock comment";
    String storageLocation = "mock location";
    String backStorageLocation = "mock bak location";
    Map<String, String> properties =
        ImmutableMap.of("k1", "v1", BACKUP_STORAGE_LOCATION_KEY + 1, backStorageLocation);

    // Test add backup storage location
    FilesetUpdateRequest.AddFilesetBackupStorageLocationRequest add =
        new FilesetUpdateRequest.AddFilesetBackupStorageLocationRequest(
            BACKUP_STORAGE_LOCATION_KEY + 1, backStorageLocation);
    FilesetDTO mockFileset =
        mockFilesetDTO(ident.name(), type, comment, storageLocation, properties);
    FilesetResponse resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(add)), resp, SC_OK);
    Fileset res = catalog.asFilesetCatalog().alterFileset(ident, add.filesetChange());
    assertFileset(mockFileset, res);

    // Test remove backup storage location
    FilesetUpdateRequest.RemoveFilesetBackupStorageLocationRequest remove =
        new FilesetUpdateRequest.RemoveFilesetBackupStorageLocationRequest(
            BACKUP_STORAGE_LOCATION_KEY + 1);
    mockFileset =
        mockFilesetDTO(ident.name(), type, comment, storageLocation, ImmutableMap.of("k1", "v1"));
    resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(remove)), resp, SC_OK);
    res = catalog.asFilesetCatalog().alterFileset(ident, remove.filesetChange());
    assertFileset(mockFileset, res);

    // Test update backup storage location
    String newBackStorageLocation = "mock bak new location";
    FilesetUpdateRequest.UpdateFilesetBackupStorageLocationRequest update =
        new FilesetUpdateRequest.UpdateFilesetBackupStorageLocationRequest(
            BACKUP_STORAGE_LOCATION_KEY + 1, newBackStorageLocation);
    mockFileset =
        mockFilesetDTO(
            ident.name(),
            type,
            comment,
            storageLocation,
            ImmutableMap.of("k1", "v1", BACKUP_STORAGE_LOCATION_KEY + 1, newBackStorageLocation));
    resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(update)), resp, SC_OK);
    res = catalog.asFilesetCatalog().alterFileset(ident, update.filesetChange());
    assertFileset(mockFileset, res);

    // Test update primary storage location
    String newStorageLocation = "mock new location";
    FilesetUpdateRequest.UpdateFilesetPrimaryStorageLocationRequest update2 =
        new FilesetUpdateRequest.UpdateFilesetPrimaryStorageLocationRequest(newStorageLocation);
    mockFileset =
        mockFilesetDTO(
            ident.name(),
            type,
            comment,
            newStorageLocation,
            ImmutableMap.of("k1", "v1", BACKUP_STORAGE_LOCATION_KEY + 1, newBackStorageLocation));
    resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(update2)), resp, SC_OK);
    res = catalog.asFilesetCatalog().alterFileset(ident, update2.filesetChange());
    assertFileset(mockFileset, res);

    // Test switch backup storage locations
    String backStorageLocation1 = "mock bak location1";
    String createFilesetPath =
        withSlash(FilesetCatalog.formatFilesetRequestPath(ident.namespace()));
    mockFileset =
        mockFilesetDTO(
            ident.name(),
            type,
            comment,
            storageLocation,
            ImmutableMap.of(
                "k1",
                "v1",
                BACKUP_STORAGE_LOCATION_KEY + 1,
                backStorageLocation,
                BACKUP_STORAGE_LOCATION_KEY + 2,
                backStorageLocation1));
    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name(ident.name())
            .type(type)
            .comment(comment)
            .storageLocation(storageLocation)
            .properties(
                ImmutableMap.of(
                    "k1",
                    "v1",
                    BACKUP_STORAGE_LOCATION_KEY + 1,
                    backStorageLocation,
                    BACKUP_STORAGE_LOCATION_KEY + 2,
                    backStorageLocation1))
            .build();
    resp = new FilesetResponse(mockFileset);
    buildMockResource(Method.POST, createFilesetPath, req, resp, SC_OK);
    Fileset loadedFileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                ident,
                comment,
                type,
                storageLocation,
                ImmutableMap.of(
                    "k1",
                    "v1",
                    BACKUP_STORAGE_LOCATION_KEY + 1,
                    backStorageLocation,
                    BACKUP_STORAGE_LOCATION_KEY + 2,
                    backStorageLocation1));
    Assertions.assertNotNull(loadedFileset);
    assertFileset(mockFileset, loadedFileset);

    FilesetUpdateRequest.SwitchFilesetBackupStorageLocationRequest switch1 =
        new FilesetUpdateRequest.SwitchFilesetBackupStorageLocationRequest(
            BACKUP_STORAGE_LOCATION_KEY + 1, BACKUP_STORAGE_LOCATION_KEY + 2);
    mockFileset =
        mockFilesetDTO(
            ident.name(),
            type,
            comment,
            storageLocation,
            ImmutableMap.of(
                "k1",
                "v1",
                BACKUP_STORAGE_LOCATION_KEY + 1,
                backStorageLocation1,
                BACKUP_STORAGE_LOCATION_KEY + 2,
                backStorageLocation));
    resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(switch1)), resp, SC_OK);
    res = catalog.asFilesetCatalog().alterFileset(ident, switch1.filesetChange());
    assertFileset(mockFileset, res);

    // Test awitch primary and backup storage location
    FilesetUpdateRequest.SwitchFilesetPrimaryAndBackupStorageLocationRequest switch2 =
        new FilesetUpdateRequest.SwitchFilesetPrimaryAndBackupStorageLocationRequest(
            BACKUP_STORAGE_LOCATION_KEY + 1);
    mockFileset =
        mockFilesetDTO(
            ident.name(),
            type,
            comment,
            backStorageLocation1,
            ImmutableMap.of(
                "k1",
                "v1",
                BACKUP_STORAGE_LOCATION_KEY + 1,
                storageLocation,
                BACKUP_STORAGE_LOCATION_KEY + 2,
                backStorageLocation));
    resp = new FilesetResponse(mockFileset);
    buildMockResource(
        Method.PUT, filesetPath, new FilesetUpdatesRequest(ImmutableList.of(switch2)), resp, SC_OK);
    res = catalog.asFilesetCatalog().alterFileset(ident, switch2.filesetChange());
    assertFileset(mockFileset, res);
  }

  private FilesetDTO mockFilesetDTO(
      String name,
      Fileset.Type type,
      String comment,
      String location,
      Map<String, String> properties) {
    return FilesetDTO.builder()
        .name(name)
        .type(type)
        .comment(comment)
        .storageLocation(location)
        .properties(properties)
        .audit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private FilesetContextDTO mockFilesetContextDTO(FilesetDTO fileset, String[] actualPaths) {
    return FilesetContextDTO.builder().fileset(fileset).actualPaths(actualPaths).build();
  }

  private void assertFileset(FilesetDTO expected, Fileset actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.type(), actual.type());
    Assertions.assertEquals(expected.properties(), actual.properties());
  }

  private static Stream<Arguments> filesetTypes() {
    return Stream.of(Arguments.of(Fileset.Type.EXTERNAL), Arguments.of(Fileset.Type.MANAGED));
  }
}
