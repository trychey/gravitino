/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.security.GroupMappingServiceProvider;

public class DummyGroupsMapping implements GroupMappingServiceProvider {
  @Override
  @SuppressWarnings("unchecked")
  public List<String> getGroups(String user) throws IOException {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {}

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {}
}
