/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Version;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.client.DefaultOAuth2TokenProvider;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoClientBase;
import com.datastrato.gravitino.client.TokenAuthProvider;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.filesystem.hadoop.context.FileSystemContext;
import com.datastrato.gravitino.properties.FilesetProperties;
import com.datastrato.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import com.datastrato.gravitino.shaded.com.google.common.collect.Lists;
import com.datastrato.gravitino.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.StringUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Gravitino server, and creates an independent file system for it to act as an agent for users to
 * access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger Logger = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private String metalakeName;
  private String authType;
  private Cache<NameIdentifier, Catalog> catalogCache;
  private ScheduledThreadPoolExecutor catalogCleanScheduler;
  private InternalFileSystemManager fileSystemManager;
  private String localAddress;
  private String appId;
  private SourceEngineType sourceType;
  private Map<String, String> extraInfo = ImmutableMap.of();
  private static final ClientType clientType = ClientType.HADOOP_GVFS;
  private static final Version.VersionInfo clientVersion = Version.getCurrentVersion();
  private static boolean isFsGravitinoFilesetWritePrimaryOnly = false;
  private static boolean isCloudMLEnv = false;

  // The pattern is used to match gvfs path. The scheme prefix (gvfs://fileset) is optional.
  // The following path can be match:
  //     gvfs://fileset/fileset_catalog/fileset_schema/fileset1/file.txt
  //     /fileset_catalog/fileset_schema/fileset1/sub_dir/
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$");

  // The prefix is used to match catalog bypass configuration. These configuration from
  // catalog properties and fileset properties will be trim and pass to the
  // hadoop configuration.
  private static final String CATALOG_BYPASS_PREFIX = "gravitino.bypass.";

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }
    String metalakeNameEnv = System.getenv("GRAVITINO_METALAKE");
    if (StringUtils.isNotBlank(metalakeNameEnv)) {
      this.metalakeName = metalakeNameEnv;
    } else {
      this.metalakeName =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    }
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    initializeClient(configuration);
    initializeCatalogCache();
    isCloudMLEnv = isCloudMLEnv();

    this.fileSystemManager = new InternalFileSystemManager(configuration);
    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

    this.localAddress = getLocalAddress();
    this.appId = getAppId();
    this.sourceType = getSourceType();
    this.extraInfo = getExtraInfo();

    isFsGravitinoFilesetWritePrimaryOnly =
        configuration.getBoolean(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_WRITE_PRIMARY_ONLY_DEFAULT);

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  private void initializeCatalogCache() {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.catalogCleanScheduler =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("gvfs-catalog-cache-cleaner-%d")
                .build());
    this.catalogCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .scheduler(Scheduler.forScheduledExecutorService(catalogCleanScheduler))
            .build();
  }

  private void initializeClient(Configuration configuration) {
    // initialize the Gravitino client
    String serverUriEnv = System.getenv("GRAVITINO_SERVER");
    String serverUri;
    if (StringUtils.isNotBlank(serverUriEnv)) {
      serverUri = serverUriEnv;
    } else {
      serverUri =
          configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    }
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    String authTypeEnv = System.getenv("GRAVITINO_CLIENT_AUTH_TYPE");
    if (StringUtils.isNotBlank(authTypeEnv)) {
      this.authType = authTypeEnv;
    } else {
      this.authType =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
              GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
    }
    String isIntegrationTesting =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_TESTING,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_TESTING_DEFAULT);
    if (Boolean.parseBoolean(isIntegrationTesting)) {
      GravitinoClientBase.Builder<GravitinoClient> builder =
          GravitinoClient.builder(serverUri).withMetalake(metalakeName).withSimpleAuth();
      this.client = builder.build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE)) {
      String superUserEnv = System.getenv("GRAVITINO_TOKEN");
      String superUser;
      if (StringUtils.isNotBlank(superUserEnv)) {
        superUser = superUserEnv;
      } else {
        superUser =
            configuration.get(
                GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_SUPER_USER_KEY);
      }
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_SUPER_USER_KEY,
          superUser);

      String proxyUserEnv = System.getenv("GRAVITINO_PROXY_USER");
      String proxyUser;
      if (StringUtils.isNotBlank(proxyUserEnv)) {
        proxyUser = proxyUserEnv;
      } else {
        proxyUser =
            configuration.get(
                GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_PROXY_USER_KEY);
      }
      GravitinoClientBase.Builder<GravitinoClient> builder =
          GravitinoClient.builder(serverUri).withMetalake(metalakeName).withSimpleAuth(superUser);
      if (StringUtils.isNotBlank(proxyUser)) {
        builder.withHeaders(ImmutableMap.of(AuthConstants.PROXY_USER, proxyUser));
      }
      this.client = builder.build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE)) {
      String authServerUri =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
          authServerUri);

      String credential =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
          credential);

      String path =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY,
          path);

      String scope =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY,
          scope);

      DefaultOAuth2TokenProvider authDataProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(authServerUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();

      this.client =
          GravitinoClient.builder(serverUri)
              .withMetalake(metalakeName)
              .withOAuth(authDataProvider)
              .build();
    } else if (authType.equalsIgnoreCase(GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE)) {
      String tokenEnv = System.getenv("GRAVITINO_CLIENT_AUTH_TOKEN");
      String token;
      if (StringUtils.isNotBlank(tokenEnv)) {
        token = tokenEnv;
      } else {
        token =
            configuration.get(
                GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY);
      }
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.TOKEN_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY,
          token);
      TokenAuthProvider tokenAuthProvider = new TokenAuthProvider(token);
      this.client =
          GravitinoClient.builder(serverUri)
              .withMetalake(metalakeName)
              .withTokenAuth(tokenAuthProvider)
              .build();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported authentication type: %s for %s.",
              authType, GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
    }
  }

  @VisibleForTesting
  InternalFileSystemManager getFileSystemManager() {
    return fileSystemManager;
  }

  private static String getLocalAddress() {
    try {
      Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
      InetAddress ip;
      while (allNetInterfaces.hasMoreElements()) {
        NetworkInterface netInterface = allNetInterfaces.nextElement();
        if (!netInterface.isLoopback() && !netInterface.isVirtual() && netInterface.isUp()) {
          Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
          while (addresses.hasMoreElements()) {
            ip = addresses.nextElement();
            if (ip instanceof Inet4Address) {
              return ip.getHostAddress();
            }
          }
        }
      }
    } catch (Exception e) {
      Logger.warn("Cannot get the local IP address: ", e);
    }
    return "unknown";
  }

  private static String getAppId() {
    // get APP_ID for spark / flink / cloudml
    try {
      String appIdVar = System.getenv("APP_ID");
      if (StringUtils.isNotBlank(appIdVar)) {
        return appIdVar;
      }
      if (isCloudMLEnv) {
        appIdVar = System.getenv("CLOUDML_JOB_ID");
        if (StringUtils.isNotBlank(appIdVar)) {
          return appIdVar;
        }
      }
    } catch (Exception e) {
      Logger.warn("Cannot get the app id: ", e);
    }
    return "unknown";
  }

  private static SourceEngineType getSourceType() {
    try {
      String notebookVar = System.getenv("NOTEBOOK_TASK");
      if (StringUtils.isNotBlank(notebookVar) && notebookVar.equals("true")) {
        return SourceEngineType.NOTEBOOK;
      }
      String sparkClasspathVar = System.getenv("SPARK_DIST_CLASSPATH");
      if (StringUtils.isNotBlank(sparkClasspathVar)) {
        return SourceEngineType.SPARK;
      }
      String flinkClasspathVar = System.getenv("_FLINK_CLASSPATH");
      if (StringUtils.isNotBlank(flinkClasspathVar)) {
        return SourceEngineType.FLINK;
      }

      if (isCloudMLEnv) {
        return SourceEngineType.CLOUDML;
      }
    } catch (Exception e) {
      Logger.warn("Cannot get the source type: ", e);
    }
    return SourceEngineType.UNKNOWN;
  }

  private static boolean isCloudMLEnv() {
    try {
      String cloudmlJobId = System.getenv("CLOUDML_JOB_ID");
      if (StringUtils.isNotBlank(cloudmlJobId)) {
        return true;
      }
    } catch (Exception e) {
      Logger.warn("Cannot get the cloudml env: ", e);
    }
    return false;
  }

  private static Map<String, String> getExtraInfo() {
    Map<String, String> extraInfo = Maps.newHashMap();
    if (isCloudMLEnv) {
      extraInfo.computeIfAbsent("CLOUDML_OWNER_NAME", k -> System.getenv("OWNER_NAME"));
      extraInfo.computeIfAbsent("CLOUDML_EXP_JOBNAME", k -> System.getenv("EXP_JOBNAME"));
      extraInfo.computeIfAbsent("CLOUDML_USER", k -> System.getenv("CLOUDML_USER"));
      extraInfo.computeIfAbsent("CLOUDML_XIAOMI_DEV_IMAGE", k -> System.getenv("XIAOMI_DEV_IMAGE"));
      extraInfo.computeIfAbsent(
          "CLOUDML_KRB_ACCOUNT", k -> System.getenv("XIAOMI_HDFS_KRB_ACCOUNT"));
      extraInfo.computeIfAbsent(
          "CLOUDML_XIAOMI_BUILD_IMAGE", k -> System.getenv("XIAOMI_BUILD_IMAGE"));
      extraInfo.computeIfAbsent("CLOUDML_CLUSTER_NAME", k -> System.getenv("CLUSTER_NAME"));
    }
    extraInfo.computeIfAbsent("CLIENT_VERSION", k -> clientVersion.version);
    extraInfo.computeIfAbsent("CLIENT_COMPILE_DATE", k -> clientVersion.compileDate);
    extraInfo.computeIfAbsent("CLIENT_GIT_COMMIT", k -> clientVersion.gitCommit);
    return extraInfo;
  }

  private void checkAuthConfig(String authType, String configKey, String configValue) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue),
        "%s should not be null if %s is set to %s.",
        configKey,
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        authType);
  }

  private String getVirtualLocation(NameIdentifier identifier, boolean withScheme) {
    return String.format(
        "%s/%s/%s/%s",
        withScheme ? GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX : "",
        identifier.namespace().level(1),
        identifier.namespace().level(2),
        identifier.name());
  }

  private FileStatus convertFileStatusPathPrefix(
      FileStatus fileStatus, String actualPrefix, String virtualPrefix) {
    String filePath = fileStatus.getPath().toString();
    Preconditions.checkArgument(
        filePath.startsWith(actualPrefix),
        "Path %s doesn't start with prefix \"%s\".",
        filePath,
        actualPrefix);
    Path path = new Path(filePath.replaceFirst(actualPrefix, virtualPrefix));
    fileStatus.setPath(path);

    return fileStatus;
  }

  @VisibleForTesting
  static String getStorageLocation(Fileset fileset, int index) {
    Preconditions.checkArgument(index >= 0, "Index cannot be negative.");
    Map<String, String> properties = fileset.properties();
    if (index == 0) {
      return fileset.storageLocation().endsWith("/")
          ? fileset.storageLocation().substring(0, fileset.storageLocation().length() - 1)
          : fileset.storageLocation();
    } else {
      String backupStorageLocation =
          properties.get(FilesetProperties.BACKUP_STORAGE_LOCATION_KEY + index);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(backupStorageLocation),
          "Backup storage location cannot be null or empty.");

      return backupStorageLocation.endsWith("/")
          ? backupStorageLocation.substring(0, backupStorageLocation.length() - 1)
          : backupStorageLocation;
    }
  }

  @VisibleForTesting
  NameIdentifier extractIdentifier(URI virtualUri) {
    String virtualPath = virtualUri.toString();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(virtualPath),
        "Uri which need be extracted cannot be null or empty.");

    Matcher matcher = IDENTIFIER_PATTERN.matcher(virtualPath);
    Preconditions.checkArgument(
        matcher.matches() && matcher.groupCount() == 3,
        "URI %s doesn't contains valid identifier",
        virtualPath);

    return NameIdentifier.ofFileset(
        metalakeName, matcher.group(1), matcher.group(2), matcher.group(3));
  }

  private FilesetContextPair getFilesetContext(Path virtualPath, FilesetDataOperation operation) {
    NameIdentifier identifier = extractIdentifier(virtualPath.toUri());
    String virtualPathString = virtualPath.toString();
    String subPath = getSubPath(identifier, virtualPathString);
    BaseFilesetDataOperationCtx requestCtx =
        BaseFilesetDataOperationCtx.builder()
            .withOperation(operation)
            .withSubPath(subPath)
            .withClientType(clientType)
            .withIp(this.localAddress)
            .withSourceEngineType(this.sourceType)
            .withAppId(this.appId)
            .withExtraInfo(this.extraInfo)
            .build();
    NameIdentifier catalogIdent =
        NameIdentifier.ofCatalog(metalakeName, identifier.namespace().level(1));
    Map<String, String> properties = Maps.newHashMap();
    Catalog catalog = catalogCache.get(catalogIdent, ident -> client.loadCatalog(ident));
    Preconditions.checkArgument(
        catalog != null, String.format("Loaded fileset catalog: %s is null.", catalogIdent));
    properties.putAll(catalog.properties());
    FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();
    FilesetContext context = filesetCatalog.getFilesetContext(identifier, requestCtx);
    FileSystem[] fileSystems = new FileSystem[context.actualPaths().length];
    for (int index = 0; index < context.actualPaths().length; index++) {
      String actualPath = context.actualPaths()[index];
      URI uri = new Path(actualPath).toUri();
      FileSystem fileSystem =
          fileSystemManager.getFileSystem(
              uri, subPath, loadConfig(properties, context.fileset().properties()));
      fileSystems[index] = fileSystem;
    }
    return new FilesetContextPair(context, fileSystems);
  }

  private String getSubPath(NameIdentifier identifier, String virtualPath) {
    return virtualPath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)
        ? virtualPath.substring(
            String.format(
                    "%s/%s/%s/%s",
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                    identifier.namespace().level(1),
                    identifier.namespace().level(2),
                    identifier.name())
                .length())
        : virtualPath.substring(
            String.format(
                    "/%s/%s/%s",
                    identifier.namespace().level(1),
                    identifier.namespace().level(2),
                    identifier.name())
                .length());
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public synchronized void setWorkingDirectory(Path newDir) {
    FilesetContextPair pair = getFilesetContext(newDir, FilesetDataOperation.SET_WORKING_DIR);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    for (int i = 0; i < actualPaths.length; i++) {
      fileSystems[i].setWorkingDirectory(new Path(actualPaths[i]));
    }
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.OPEN);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    for (int i = 0; i < actualPaths.length; i++) {
      try {
        return fileSystems[i].open(new Path(actualPaths[i]), bufferSize);
      } catch (FileNotFoundException e) {
        Logger.debug(
            "Cannot find the gvfs file: {} in the underline Filesystem: {}.",
            path,
            fileSystems[i].getUri(),
            e);
      }
    }
    throw fileNotFoundException(path.toString());
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.CREATE);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    if (isFsGravitinoFilesetWritePrimaryOnly) {
      return fileSystems[0].create(
          new Path(actualPaths[0]),
          permission,
          overwrite,
          bufferSize,
          replication,
          blockSize,
          progress);
    }
    List<Integer> validateActualPathIndexes = validateActualPaths(fileSystems, actualPaths);
    if (validateActualPathIndexes.isEmpty()) {
      return fileSystems[0].create(
          new Path(actualPaths[0]),
          permission,
          overwrite,
          bufferSize,
          replication,
          blockSize,
          progress);
    }
    Integer index = validateActualPathIndexes.get(0);
    return fileSystems[index].create(
        new Path(actualPaths[index]),
        permission,
        overwrite,
        bufferSize,
        replication,
        blockSize,
        progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.APPEND);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    List<Integer> validateActualPathIndexes = validateActualPaths(fileSystems, actualPaths);
    if (validateActualPathIndexes.isEmpty()) {
      throw fileNotFoundException(path.toString());
    }
    Integer index = validateActualPathIndexes.get(0);
    return fileSystems[index].append(new Path(actualPaths[index]), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // There are two cases that cannot be renamed:
    // 1. Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    // 2. Fileset only mounts a single file, the storage location of the fileset cannot be renamed;
    // Otherwise the metadata in the Gravitino server may be inconsistent.
    NameIdentifier srcIdentifier = extractIdentifier(src.toUri());
    NameIdentifier dstIdentifier = extractIdentifier(dst.toUri());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);

    FilesetContextPair srcPair = getFilesetContext(src, FilesetDataOperation.RENAME);
    FilesetContextPair dstPair = getFilesetContext(dst, FilesetDataOperation.RENAME);

    FileSystem[] srcFileSystems = srcPair.getFileSystems();
    String[] srcActualPaths = srcPair.getContext().actualPaths();
    String[] dstActualPaths = dstPair.getContext().actualPaths();

    if (isFsGravitinoFilesetWritePrimaryOnly) {
      return srcFileSystems[0].rename(new Path(srcActualPaths[0]), new Path(dstActualPaths[0]));
    }

    List<Integer> validateActualPathIndexes =
        validateActualPaths(srcFileSystems, srcActualPaths, dstActualPaths);
    if (validateActualPathIndexes.isEmpty()) {
      throw fileNotFoundException(src.toString());
    }
    Integer index = validateActualPathIndexes.get(0);
    return srcFileSystems[index].rename(
        new Path(srcActualPaths[index]), new Path(dstActualPaths[index]));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.DELETE);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    if (isFsGravitinoFilesetWritePrimaryOnly) {
      return fileSystems[0].delete(new Path(actualPaths[0]), recursive);
    }
    List<Integer> validateActualPathIndexes = validateActualPaths(fileSystems, actualPaths);
    if (validateActualPathIndexes.isEmpty()) {
      return false;
    }
    Integer index = validateActualPathIndexes.get(0);
    return fileSystems[index].delete(new Path(actualPaths[index]), recursive);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.GET_FILE_STATUS);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    if (isFsGravitinoFilesetWritePrimaryOnly) {
      return fileSystems[0].getFileStatus(new Path(actualPaths[0]));
    }
    NameIdentifier identifier = extractIdentifier(path.toUri());
    for (int i = 0; i < actualPaths.length; i++) {
      try {
        FileStatus fileStatus = fileSystems[i].getFileStatus(new Path(actualPaths[i]));
        return convertFileStatusPathPrefix(
            fileStatus,
            getStorageLocation(pair.getContext().fileset(), i),
            getVirtualLocation(identifier, true));
      } catch (FileNotFoundException e) {
        // Skipping log to avoid print to many logs
      }
    }
    throw fileNotFoundException(path.toString());
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.LIST_STATUS);
    NameIdentifier identifier = extractIdentifier(path.toUri());

    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();

    List<Integer> validateActualPathsForList = validateActualPathsForList(fileSystems, actualPaths);
    if (isFsGravitinoFilesetWritePrimaryOnly) {
      int index = validateActualPathsForList.get(0);
      FileStatus[] fileStatuses = fileSystems[index].listStatus(new Path(actualPaths[index]));
      return Arrays.stream(fileStatuses)
          .map(
              fileStatus ->
                  convertFileStatusPathPrefix(
                      fileStatus,
                      getStorageLocation(pair.getContext().fileset(), index),
                      getVirtualLocation(identifier, true)))
          .toArray(FileStatus[]::new);
    } else {
      List<FileStatus> gvfsFileStatus = new ArrayList<>();
      Set<String> distinctFileStatus = new HashSet<>();
      for (int i = 0; i < validateActualPathsForList.size(); i++) {
        int index = validateActualPathsForList.get(i);
        try {
          FileStatus[] fileStatuses = fileSystems[index].listStatus(new Path(actualPaths[index]));
          Arrays.stream(fileStatuses)
              .map(
                  fileStatus ->
                      convertFileStatusPathPrefix(
                          fileStatus,
                          getStorageLocation(pair.getContext().fileset(), index),
                          getVirtualLocation(identifier, true)))
              .forEach(
                  fileStatus -> {
                    if (distinctFileStatus.add(fileStatus.getPath().toString())) {
                      gvfsFileStatus.add(fileStatus);
                    }
                  });
        } catch (FileNotFoundException e) {
          // Skipping log to avoid print to many logs
        }
      }
      return gvfsFileStatus.toArray(new FileStatus[0]);
    }
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.MKDIRS);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    if (isFsGravitinoFilesetWritePrimaryOnly) {
      return fileSystems[0].mkdirs(new Path(actualPaths[0]), permission);
    }
    for (int i = 0; i < actualPaths.length; i++) {
      if (fileSystems[i].exists(new Path(actualPaths[i]))) {
        return true;
      }
    }
    Path actualPath = new Path(actualPaths[0]);
    return fileSystems[0].mkdirs(actualPath, permission);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
    // only support simple auth type,
    // the token auth type will use the credential vending mechanism
    if (authType.equalsIgnoreCase(GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE)) {
      List<Token<?>> tokenList = Lists.newArrayList();
      for (FileSystemContext context :
          fileSystemManager.getInternalContextCache().asMap().values()) {
        try {
          tokenList.addAll(
              Arrays.asList(context.getFileSystem().addDelegationTokens(renewer, credentials)));
        } catch (IOException e) {
          Logger.warn(
              "Failed to add delegation tokens for filesystem: {}",
              context.getFileSystem().getUri(),
              e);
        }
      }
      return tokenList.stream().distinct().toArray(Token[]::new);
    }
    return null;
  }

  @Override
  public boolean truncate(Path file, long newLength) throws IOException {
    FilesetContextPair pair = getFilesetContext(file, FilesetDataOperation.TRUNCATE);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();

    List<Integer> validateActualPathIndexes = validateActualPaths(fileSystems, actualPaths);
    if (validateActualPathIndexes.isEmpty()) {
      throw fileNotFoundException(file.toString());
    }
    Integer index = validateActualPathIndexes.get(0);
    return fileSystems[index].truncate(new Path(actualPaths[index]), newLength);
  }

  @Override
  public short getDefaultReplication(Path f) {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_REPLICATION);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    for (int i = 0; i < actualPaths.length; i++) {
      try {
        return fileSystems[i].getDefaultReplication(new Path(actualPaths[i]));
      } catch (Exception e) {
        Logger.warn(
            "Cannot find the gvfs file: {} in the underline Filesystem: {}.",
            actualPaths[i],
            fileSystems[i].getClass().getName(),
            e);
      }
    }
    throw new RuntimeException(
        String.format("Failed to getDefaultReplication for gvfs path: %s", f));
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    for (int i = 0; i < actualPaths.length; i++) {
      try {
        return fileSystems[i].getDefaultBlockSize(new Path(actualPaths[i]));
      } catch (Exception e) {
        Logger.warn(
            "Cannot find the gvfs file: {} in the underline Filesystem: {}.",
            actualPaths[i],
            fileSystems[i].getClass().getName(),
            e);
      }
    }
    throw new RuntimeException(String.format("Failed to getDefaultBlockSize for gvfs path: %s", f));
  }

  @Override
  public FileChecksum getFileChecksum(Path file) throws IOException {
    FilesetContextPair pair = getFilesetContext(file, FilesetDataOperation.GET_FILE_CHECKSUM);
    FileSystem[] fileSystems = pair.getFileSystems();
    String[] actualPaths = pair.getContext().actualPaths();
    for (int i = 0; i < actualPaths.length; i++) {
      try {
        return fileSystems[i].getFileChecksum(new Path(actualPaths[i]));
      } catch (FileNotFoundException e) {
        // Skipping log to avoid print to many logs
      }
    }
    throw fileNotFoundException(file.toString());
  }

  @Override
  public String getScheme() {
    return "gvfs";
  }

  @Override
  public synchronized void close() throws IOException {
    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }

    catalogCache.invalidateAll();
    catalogCleanScheduler.shutdownNow();

    fileSystemManager.close();

    super.close();
  }

  @VisibleForTesting
  Configuration loadConfig(
      Map<String, String> catalogProperties, Map<String, String> filesetProperties) {
    Configuration configuration = new Configuration(getConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.putAll(catalogProperties);
    properties.putAll(filesetProperties);
    properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(CATALOG_BYPASS_PREFIX))
        .forEach(
            e -> {
              configuration.set(e.getKey().replaceFirst(CATALOG_BYPASS_PREFIX, ""), e.getValue());
            });

    return configuration;
  }

  private List<Integer> validateActualPaths(FileSystem[] fileSystems, String[] actualPaths)
      throws IOException {
    Preconditions.checkArgument(fileSystems != null, "The file systems cannot be null.");
    Preconditions.checkArgument(actualPaths != null, "The actual paths cannot be null.");
    Preconditions.checkArgument(
        fileSystems.length == actualPaths.length,
        String.format(
            "The number of actual paths is different from the number of file systems, actual paths: %s, file systems: %s.",
            actualPaths.length, fileSystems.length));

    List<Integer> validatedActualPathIndexes = new ArrayList<>();
    for (int i = 0; i < fileSystems.length; i++) {
      if (fileSystems[i].exists(new Path(actualPaths[i]))) {
        validatedActualPathIndexes.add(i);
      }
      Preconditions.checkArgument(
          validatedActualPathIndexes.size() <= 1,
          "Only support no more than one actual path exists.");
    }
    return validatedActualPathIndexes;
  }

  private List<Integer> validateActualPathsForList(FileSystem[] fileSystems, String[] actualPaths)
      throws IOException {
    Preconditions.checkArgument(fileSystems != null, "The file systems cannot be null.");
    Preconditions.checkArgument(actualPaths != null, "The actual paths cannot be null.");
    Preconditions.checkArgument(
        fileSystems.length == actualPaths.length,
        String.format(
            "The number of actual paths is different from the number of file systems, actual paths: %s, file systems: %s.",
            actualPaths.length, fileSystems.length));

    List<Integer> validatedActualPathIndexes = new ArrayList<>();
    for (int i = 0; i < fileSystems.length; i++) {
      if (fileSystems[i].exists(new Path(actualPaths[i]))) {
        validatedActualPathIndexes.add(i);
      }
    }
    Preconditions.checkArgument(
        !validatedActualPathIndexes.isEmpty(), "No actual path exists for list.");
    return validatedActualPathIndexes;
  }

  private List<Integer> validateActualPaths(
      FileSystem[] fileSystems, String[] srcPaths, String[] dstPaths) throws IOException {
    Preconditions.checkArgument(fileSystems != null, "The file systems cannot be null.");
    Preconditions.checkArgument(srcPaths != null, "The src paths cannot be null.");
    Preconditions.checkArgument(dstPaths != null, "The dst paths cannot be null.");
    Preconditions.checkArgument(
        fileSystems.length == srcPaths.length && fileSystems.length == dstPaths.length,
        String.format(
            "The number of src and dst paths is different from the number of file systems, src paths: %s, dst paths: %s, file systems: %s.",
            srcPaths.length, dstPaths.length, fileSystems.length));

    List<Integer> validatedSrcPathIndexes = new ArrayList<>();
    for (int i = 0; i < fileSystems.length; i++) {
      Preconditions.checkArgument(
          !fileSystems[i].exists(new Path(dstPaths[i])),
          String.format("The dst path: %s already exists, cannot rename again.", dstPaths[i]));
      if (fileSystems[i].exists(new Path(srcPaths[i]))) {
        validatedSrcPathIndexes.add(i);
      }
      Preconditions.checkArgument(
          validatedSrcPathIndexes.size() <= 1, "Only support no more than one actual path exists.");
    }
    return validatedSrcPathIndexes;
  }

  private static FileNotFoundException fileNotFoundException(String gvfsPath) {
    return new FileNotFoundException(
        String.format("Cannot find a valid actual path for gvfs path: %s.", gvfsPath));
  }

  private static class FilesetContextPair {
    private final FilesetContext context;
    private final FileSystem[] fileSystems;

    public FilesetContextPair(FilesetContext context, FileSystem[] fileSystems) {
      this.context = context;
      this.fileSystems = fileSystems;
    }

    public FilesetContext getContext() {
      return context;
    }

    public FileSystem[] getFileSystems() {
      return fileSystems;
    }
  }
}
