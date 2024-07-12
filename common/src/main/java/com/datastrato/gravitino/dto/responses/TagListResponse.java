/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.tag.TagDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of tags. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class TagListResponse extends BaseResponse {

  @JsonProperty("tags")
  private final TagDTO[] tags;

  /**
   * Creates a new TagListResponse.
   *
   * @param tags The list of tags.
   */
  public TagListResponse(TagDTO[] tags) {
    super(0);
    this.tags = tags;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * TagListResponse.
   */
  public TagListResponse() {
    super();
    this.tags = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(tags != null, "\"tags\" must not be null");
  }
}
