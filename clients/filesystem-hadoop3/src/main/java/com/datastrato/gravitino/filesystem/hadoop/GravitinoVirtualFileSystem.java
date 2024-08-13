/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.client.DefaultOAuth2TokenProvider;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoClientBase;
import com.datastrato.gravitino.client.TokenAuthProvider;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import com.datastrato.gravitino.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.StringUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
  private Cache<NameIdentifier, FilesetCatalog> catalogCache;
  private ScheduledThreadPoolExecutor catalogCleanScheduler;
  private Cache<String, FileSystem> filesetFSCache;
  private ScheduledThreadPoolExecutor filesetFSCleanScheduler;
  private String localAddress;
  private String appId;
  private SourceEngineType sourceType;
  private static final ClientType clientType = ClientType.HADOOP_GVFS;

  // The pattern is used to match gvfs path. The scheme prefix (gvfs://fileset) is optional.
  // The following path can be match:
  //     gvfs://fileset/fileset_catalog/fileset_schema/fileset1/file.txt
  //     /fileset_catalog/fileset_schema/fileset1/sub_dir/
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$");

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }

    int maxCapacity =
        configuration.getInt(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT);
    Preconditions.checkArgument(
        maxCapacity > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY);

    long evictionMillsAfterAccess =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT);
    Preconditions.checkArgument(
        evictionMillsAfterAccess != 0,
        "'%s' should not be 0",
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY);

    initializeFSCache(maxCapacity, evictionMillsAfterAccess);

    this.metalakeName =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    initializeClient(configuration);
    initializeCatalogCache();

    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

    this.localAddress = getLocalAddress();
    this.appId = getAppId();
    this.sourceType = getSourceType();

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  @VisibleForTesting
  Cache<String, FileSystem> getFilesetFSCache() {
    return filesetFSCache;
  }

  private void initializeFSCache(int maxCapacity, long expireAfterAccess) {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.filesetFSCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-fileset-cache-cleaner"));
    Caffeine<Object, Object> cacheBuilder =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            .scheduler(Scheduler.forScheduledExecutorService(filesetFSCleanScheduler))
            .removalListener(
                (key, value, cause) -> {
                  FileSystem fs = (FileSystem) value;
                  if (fs != null) {
                    try {
                      fs.close();
                    } catch (IOException e) {
                      Logger.error("Cannot close the file system for fileset: {}", key, e);
                    }
                  }
                });
    if (expireAfterAccess > 0) {
      cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
    }
    this.filesetFSCache = cacheBuilder.build();
  }

  private void initializeCatalogCache() {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.catalogCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-catalog-cache-cleaner"));
    this.catalogCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .scheduler(Scheduler.forScheduledExecutorService(catalogCleanScheduler))
            .build();
  }

  private ThreadFactory newDaemonThreadFactory(String name) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build();
  }

  private void initializeClient(Configuration configuration) {
    // initialize the Gravitino client
    String serverUri =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);

    String authType =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
            GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
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
      String superUser =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_SUPER_USER_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_SUPER_USER_KEY,
          superUser);
      String proxyUser =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_SIMPLE_PROXY_USER_KEY);
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
      String token =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TOKEN_KEY);
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
    // get APP_ID for spark / flink app
    try {
      String appIdVar = System.getenv("APP_ID");
      if (StringUtils.isNotBlank(appIdVar)) {
        return appIdVar;
      }
      // TODO need add cloudml app id
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
      // TODO need add cloudml source type
    } catch (Exception e) {
      Logger.warn("Cannot get the source type: ", e);
    }
    return SourceEngineType.UNKNOWN;
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
    String subPath =
        virtualPathString.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)
            ? virtualPathString.substring(
                String.format(
                        "%s/%s/%s/%s",
                        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                        identifier.namespace().level(1),
                        identifier.namespace().level(2),
                        identifier.name())
                    .length())
            : virtualPathString.substring(
                String.format(
                        "/%s/%s/%s",
                        identifier.namespace().level(1),
                        identifier.namespace().level(2),
                        identifier.name())
                    .length());
    BaseFilesetDataOperationCtx requestCtx =
        BaseFilesetDataOperationCtx.builder()
            .withOperation(operation)
            .withSubPath(subPath)
            .withClientType(clientType)
            .withIp(this.localAddress)
            .withSourceEngineType(this.sourceType)
            .withAppId(this.appId)
            .build();
    NameIdentifier catalogIdent =
        NameIdentifier.ofCatalog(metalakeName, identifier.namespace().level(1));
    FilesetCatalog filesetCatalog =
        catalogCache.get(
            catalogIdent, ident -> client.loadCatalog(catalogIdent).asFilesetCatalog());
    Preconditions.checkArgument(
        filesetCatalog != null, String.format("Loaded fileset catalog: %s is null.", catalogIdent));

    FilesetContext context = filesetCatalog.getFilesetContext(identifier, requestCtx);

    FileSystem[] fileSystems = new FileSystem[context.actualPaths().length];
    for (int index = 0; index < context.actualPaths().length; index++) {
      String actualPath = context.actualPaths()[index];
      URI uri = new Path(actualPath).toUri();
      String storageHostPath =
          actualPath.startsWith(GravitinoVirtualFileSystemConfiguration.LOCAL_SCHEME_WITH_SLASH)
              ? GravitinoVirtualFileSystemConfiguration.LOCAL_SCHEME_WITH_SLASH
              : uri.getScheme() + "://" + uri.getHost();
      FileSystem fileSystem =
          filesetFSCache.get(
              storageHostPath,
              str -> {
                try {
                  return FileSystem.newInstance(uri, getConf());
                } catch (IOException ioe) {
                  throw new GravitinoRuntimeException(
                      "Exception occurs when create new FileSystem for actual uri: %s, msg: %s",
                      uri, ioe);
                }
              });
      fileSystems[index] = fileSystem;
    }
    return new FilesetContextPair(context, fileSystems);
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
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    pair.getFileSystems()[0].setWorkingDirectory(actualPath);
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.OPEN);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].open(actualPath, bufferSize);
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
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].create(
        actualPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.APPEND);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].append(actualPath, bufferSize, progress);
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
    Path srcActualPath = new Path(srcPair.getContext().actualPaths()[0]);

    FilesetContextPair dstPair = getFilesetContext(dst, FilesetDataOperation.RENAME);
    Path dstActualPath = new Path(dstPair.getContext().actualPaths()[0]);

    return srcPair.getFileSystems()[0].rename(srcActualPath, dstActualPath);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.DELETE);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].delete(actualPath, recursive);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.GET_FILE_STATUS);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    FileStatus fileStatus = pair.getFileSystems()[0].getFileStatus(actualPath);
    NameIdentifier identifier = extractIdentifier(path.toUri());
    return convertFileStatusPathPrefix(
        fileStatus,
        pair.getContext().fileset().storageLocation(),
        getVirtualLocation(identifier, true));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.LIST_STATUS);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    FileStatus[] fileStatusResults = pair.getFileSystems()[0].listStatus(actualPath);
    NameIdentifier identifier = extractIdentifier(path.toUri());
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                convertFileStatusPathPrefix(
                    fileStatus,
                    new Path(pair.getContext().fileset().storageLocation()).toString(),
                    getVirtualLocation(identifier, true)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.MKDIRS);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].mkdirs(actualPath, permission);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
      throws IOException {
    return null;
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.TRUNCATE);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].truncate(actualPath, newLength);
  }

  @Override
  public short getDefaultReplication(Path f) {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_REPLICATION);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].getDefaultReplication(actualPath);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].getDefaultBlockSize(actualPath);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_FILE_CHECKSUM);
    Path actualPath = new Path(pair.getContext().actualPaths()[0]);
    return pair.getFileSystems()[0].getFileChecksum(actualPath);
  }

  @Override
  public String getScheme() {
    return "gvfs";
  }

  @Override
  public synchronized void close() throws IOException {
    // close all actual FileSystems
    for (FileSystem fileSystem : filesetFSCache.asMap().values()) {
      try {
        fileSystem.close();
      } catch (IOException e) {
        // ignore
      }
    }
    filesetFSCache.invalidateAll();
    catalogCache.invalidateAll();

    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }
    catalogCleanScheduler.shutdownNow();
    filesetFSCleanScheduler.shutdownNow();
    super.close();
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
