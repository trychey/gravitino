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


class GCSTokenCredential(Credential, ABC):
    """Represents the audit information of an entity."""

    GCS_TOKEN_CREDENTIAL_TYPE = "gcs-token"
    _GCS_TOKEN_NAME = "token"

    _expire_time_in_ms = 0

    def __init__(self, credential_info: Dict[str, str], expire_time_in_ms: int):
        self._token = credential_info[self._GCS_TOKEN_NAME]
        self._expire_time_in_ms = expire_time_in_ms

    def credential_type(self) -> str:
        """The creator of the entity.

        Returns:
             the creator of the entity.
        """
        return self.GCS_TOKEN_CREDENTIAL_TYPE

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
        return {self._GCS_TOKEN_NAME: self._token}

    def token(self) -> str:
        return self._token
