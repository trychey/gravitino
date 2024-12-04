# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import ABC
from typing import Dict

from gravitino.api.credential.credential import Credential


class OSSTokenCredential(Credential, ABC):
    """Represents the audit information of an entity."""

    OSS_TOKEN_CREDENTIAL_TYPE = "oss-token"
    _GRAVITINO_OSS_SESSION_ACCESS_KEY_ID = "oss-access-key-id"
    _GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY = "oss-secret-access-key"
    _GRAVITINO_OSS_TOKEN = "oss-security-token"

    def __init__(self, credential_info: Dict[str, str], expire_time_in_ms: int):
        self._access_key_id = credential_info[self._GRAVITINO_OSS_SESSION_ACCESS_KEY_ID]
        self._secret_access_key = credential_info[
            self._GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY
        ]
        self._security_token = credential_info[self._GRAVITINO_OSS_TOKEN]
        self._expire_time_in_ms = expire_time_in_ms

    def credential_type(self) -> str:
        """The creator of the entity.

        Returns:
             the creator of the entity.
        """
        return self.OSS_TOKEN_CREDENTIAL_TYPE

    def expire_time_in_ms(self) -> int:
        """The creation time of the entity.

        Returns:
             The creation time of the entity.
        """
        return self._expire_time_in_ms

    def credential_info(self) -> Dict[str, str]:
        """
        Returns:
             The last modifier of the entity.
        """
        return {
            self._GRAVITINO_OSS_TOKEN: self._security_token,
            self._GRAVITINO_OSS_SESSION_ACCESS_KEY_ID: self._access_key_id,
            self._GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY: self._secret_access_key,
        }

    def access_key_id(self) -> str:
        return self._access_key_id

    def secret_access_key(self) -> str:
        return self._secret_access_key

    def security_token(self) -> str:
        return self._security_token
