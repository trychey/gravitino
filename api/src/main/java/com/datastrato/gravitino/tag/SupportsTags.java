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

package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchTagException;

/**
 * Interface for supporting getting or associate tags to objects. This interface will be mixed with
 * metadata objects to provide tag operations.
 */
@Evolving
public interface SupportsTags {

  /**
   * List all the tag names for the specific object.
   *
   * @return The list of tag names.
   */
  String[] listTags();

  /**
   * List all the tags with details for the specific object.
   *
   * @return The list of tags.
   */
  Tag[] listTagsInfo();

  /**
   * Get a tag by its name for the specific object.
   *
   * @param name The name of the tag.
   * @return The tag.
   */
  Tag getTag(String name) throws NoSuchTagException;

  /**
   * Associate tags to the specific object. The tagsToAdd will be added to the object, and the
   * tagsToRemove will be removed from the object.
   *
   * @param tagsToAdd The arrays of tag name to be added to the object.
   * @param tagsToRemove The array of tag name to be removed from the object.
   * @return The array of tag names that are associated with the object.
   */
  String[] associateTags(String[] tagsToAdd, String[] tagsToRemove);
}
