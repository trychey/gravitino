"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""


class GVFSConfig:
    CACHE_SIZE = "cache_size"
    DEFAULT_CACHE_SIZE = 20

    CACHE_EXPIRED_TIME = "cache_expired_time"
    DEFAULT_CACHE_EXPIRED_TIME = 300

    AUTH_TYPE = "auth_type"
    DEFAULT_AUTH_TYPE = "simple"
    TOKEN_AUTH_TYPE = "token"

    TOKEN_VALUE = "token_value"

    PROXY_USER = "proxy_user"
