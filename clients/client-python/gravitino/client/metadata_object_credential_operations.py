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

import logging
from typing import List
from gravitino.api.credential.supports_credentials import SupportsCredentials
from gravitino.api.credential.credential import Credential
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.credential_dto import CredentialDTO
from gravitino.dto.responses.credential_response import CredentialResponse
from gravitino.exceptions.handlers.credential_error_handler import (
    CREDENTIAL_ERROR_HANDLER,
)
from gravitino.utils import HTTPClient
from gravitino.utils.credential_utils import CredentialUtils

logger = logging.getLogger(__name__)


class MetadataObjectCredentialOperations(SupportsCredentials):
    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        self._rest_client = rest_client
        t = metadata_object.type().value
        name = metadata_object.name()
        self._request_path = (
            f"api/metalakes/{metalake_name}/objects/{t}/{name}/credentials"
        )
        pass

    def get_credentials(self) -> List[Credential]:
        resp = self._rest_client.get(
            self._request_path,
            error_handler=CREDENTIAL_ERROR_HANDLER,
        )

        credential_resp = CredentialResponse.from_json(resp.body, infer_missing=True)
        credential_resp.validate()
        credential_dtos = credential_resp.credentials()
        return self.to_credentials(credential_dtos)

    def to_credentials(self, credentials: List[CredentialDTO]) -> List[Credential]:
        return [self.to_credential(credential) for credential in credentials]

    def to_credential(self, credentialDTO: CredentialDTO) -> Credential:
        return CredentialUtils.to_credential(
            credentialDTO.credential_type(),
            credentialDTO.credential_info(),
            credentialDTO.expire_time_in_ms(),
        )
