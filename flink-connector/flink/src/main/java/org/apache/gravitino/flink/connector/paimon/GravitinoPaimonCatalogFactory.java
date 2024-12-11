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

package org.apache.gravitino.flink.connector.paimon;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;

/**
 * Factory for creating instances of {@link GravitinoPaimonCatalog}. It will be created by SPI
 * discovery in Flink.
 */
public class GravitinoPaimonCatalogFactory implements CatalogFactory {

  @Override
  public Catalog createCatalog(Context context) {
    FlinkCatalog catalog = new FlinkCatalogFactory().createCatalog(context);
    return new GravitinoPaimonCatalog(context.getName(), catalog);
  }

  @Override
  public String factoryIdentifier() {
    return GravitinoPaimonCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(GravitinoPaimonCatalogFactoryOptions.backendType);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
