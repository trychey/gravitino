"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import datetime
import os
from typing import Dict

from cachetools import LRUCache, TTLCache, TLRUCache
from fsspec import AbstractFileSystem
from readerwriterlock import rwlock

from gravitino.api.secret import Secret
from gravitino.auth.token_auth_provider import TokenAuthProvider
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException
from gravitino.filesystem.delegate_filesystem_context import DelegateFileSystemContext
from gravitino.filesystem.filesystem_context import FileSystemContext
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.simple_filesystem_context import SimpleFileSystemContext
from gravitino.filesystem.storage_type import StorageType

SECRET_EXPIRE_TIME_PROP = "expireTime"
KERBEROS_SECRET_TYPE = "kerberos"
GRAVITINO_BYPASS = "gravitino.bypass."
JUICEFS_MASTER_SERVER_KEY = "juicefs.master"


def cache_ttu_strategy(_key, context, now):
    properties: Dict = context.get_secret().properties()
    expire_time_ms_string = properties.get(SECRET_EXPIRE_TIME_PROP)
    if expire_time_ms_string is None:
        raise GravitinoRuntimeException(
            f"Failed to get the expire time, key: {_key}, value:{context.get_secret().name()} "
        )
    try:
        expire_time_ms = int(expire_time_ms_string)
    except Exception as exc:
        raise GravitinoRuntimeException(
            f"The expire time: {expire_time_ms_string} is not a digit,"
            f" key: {_key}, value:{context.get_secret().name()} "
        ) from exc
    current_timestamp_ms = int(
        datetime.datetime.timestamp(datetime.datetime.now()) * 1000
    )
    if expire_time_ms <= 0:
        return now + 2**63 - 1
    # We reserve 1 hour to avoid the expiration time is too short
    reserved_time_ms = 1000 * 60 * 60
    if expire_time_ms - reserved_time_ms - current_timestamp_ms > 0:
        return now + (expire_time_ms - reserved_time_ms - current_timestamp_ms) / 1000
    return now


class InternalFileSystemManager:
    _auth_type: str
    _gravitino_client: GravitinoClient

    def __init__(
        self, server_uri: str = None, metalake_name: str = None, options: Dict = None
    ):
        self._auth_type = (
            GVFSConfig.DEFAULT_AUTH_TYPE
            if options is None
            else options.get(GVFSConfig.AUTH_TYPE, GVFSConfig.DEFAULT_AUTH_TYPE)
        )
        if self._auth_type == GVFSConfig.TOKEN_AUTH_TYPE:
            self._init_gravitino_client(server_uri, metalake_name, options)

        self._init_context_cache(options)

    def _init_gravitino_client(
        self, server_uri: str = None, metalake_name: str = None, options: Dict = None
    ):
        if metalake_name is not None:
            metalake = metalake_name
        else:
            metalake = os.environ.get("GRAVITINO_METALAKE")
            if metalake is None:
                raise GravitinoRuntimeException(
                    "No metalake name is provided. Please set the environment variable "
                    + "'GRAVITINO_METALAKE' or provide it as a parameter."
                )

        if server_uri is not None:
            uri = server_uri
        else:
            uri = os.environ.get("GRAVITINO_SERVER")
            if uri is None:
                raise GravitinoRuntimeException(
                    "No server URI is provided. Please set the environment variable "
                    + "'GRAVITINO_SERVER' or provide it as a parameter."
                )

        assert (
            self._auth_type == GVFSConfig.TOKEN_AUTH_TYPE
        ), "Token auth type is required."
        token_value = options.get(GVFSConfig.TOKEN_VALUE, None)
        assert (
            token_value is not None
        ), "Token value is not provided when using token auth type."
        token_provider: TokenAuthProvider = TokenAuthProvider(token_value)
        self._gravitino_client = GravitinoClient(
            uri=uri,
            metalake_name=metalake,
            auth_data_provider=token_provider,
        )

    def _init_context_cache(self, options: Dict = None):
        cache_size = (
            GVFSConfig.DEFAULT_CACHE_SIZE
            if options is None
            else options.get(GVFSConfig.CACHE_SIZE, GVFSConfig.DEFAULT_CACHE_SIZE)
        )
        if self._auth_type == GVFSConfig.TOKEN_AUTH_TYPE:
            self._cache = TLRUCache(maxsize=cache_size, ttu=cache_ttu_strategy)
        else:
            cache_expired_time = (
                GVFSConfig.DEFAULT_CACHE_EXPIRED_TIME
                if options is None
                else options.get(
                    GVFSConfig.CACHE_EXPIRED_TIME, GVFSConfig.DEFAULT_CACHE_EXPIRED_TIME
                )
            )
            assert cache_expired_time != 0, "Cache expired time cannot be 0."
            assert cache_size > 0, "Cache size cannot be less than or equal to 0."
            if cache_expired_time < 0:
                self._cache = LRUCache(maxsize=cache_size)
            else:
                self._cache = TTLCache(maxsize=cache_size, ttl=cache_expired_time)

        self._cache_lock = rwlock.RWLockFair()

    @staticmethod
    def _recognize_storage_type(uri: str) -> StorageType:
        if uri.startswith(StorageType.HDFS.value):
            return StorageType.HDFS
        if uri.startswith(StorageType.LAVAFS.value):
            return StorageType.LAVAFS
        if uri.startswith(StorageType.JUICEFS.value):
            return StorageType.JUICEFS
        if uri.startswith(StorageType.LOCAL.value):
            return StorageType.LOCAL
        raise GravitinoRuntimeException(f"Storage type doesn't support now. Path:{uri}")

    @staticmethod
    def _load_conf(
        fileset_properties: Dict[str, str], catalog_properties: Dict[str, str]
    ) -> str:
        props = {}
        props.update(catalog_properties)
        props.update(fileset_properties)
        conf = "&".join(
            [
                f"{k[len(GRAVITINO_BYPASS):]}={v}"
                for k, v in props.items()
                if k.startswith(GRAVITINO_BYPASS)
            ]
        )
        return conf

    def get_filesystem(
        self,
        uri: str,
        sub_path: str,
        fileset_properties: Dict[str, str],
        catalog_properties: Dict[str, str],
    ) -> AbstractFileSystem:
        storage_type = self._recognize_storage_type(uri)
        storage_location = uri.rstrip(sub_path)
        # gen the cache key for different storage type
        if storage_type in (StorageType.HDFS, StorageType.LAVAFS, StorageType.LOCAL):
            cache_key = storage_location
        elif storage_type == StorageType.JUICEFS:
            master_key = fileset_properties.get(
                f"{GRAVITINO_BYPASS}{JUICEFS_MASTER_SERVER_KEY}"
            )
            if master_key is None or len(master_key) == 0:
                raise GravitinoRuntimeException(
                    f"juicefs master key is not set for uri: {uri}"
                )
            cache_key = f"{storage_location}#{master_key}"
        else:
            raise GravitinoRuntimeException(f"unsupported storage type: {storage_type}")
        read_lock = self._cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            context: FileSystemContext = self._cache.get(cache_key)
            if context is not None:
                return context.get_filesystem()
        finally:
            read_lock.release()

        write_lock = self._cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            context: FileSystemContext = self._cache.get(cache_key)
            if context is not None:
                return context.get_filesystem()

            conf = self._load_conf(fileset_properties, catalog_properties)
            if self._auth_type == GVFSConfig.TOKEN_AUTH_TYPE:
                secret: Secret = self._gravitino_client.get_secret(KERBEROS_SECRET_TYPE)
                context: FileSystemContext = DelegateFileSystemContext(
                    secret, uri, conf
                )
            else:
                context: FileSystemContext = SimpleFileSystemContext(uri, conf)
            self._cache[storage_type] = context
            return context.get_filesystem()
        finally:
            write_lock.release()
