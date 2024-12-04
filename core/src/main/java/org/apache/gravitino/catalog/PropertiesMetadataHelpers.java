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
package org.apache.gravitino.catalog;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** This class contains helper methods for properties metadata. */
public class PropertiesMetadataHelpers {

  private PropertiesMetadataHelpers() {}

  public static <T> T checkValueFormat(String key, String value, Function<String, T> decoder) {
    try {
      return decoder.apply(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid value: '%s' for property: '%s'", value, key), e);
    }
  }

  public static void validatePropertyForCreate(
      PropertiesMetadata propertiesMetadata, Map<String, String> properties)
      throws IllegalArgumentException {
    if (properties == null) {
      return;
    }

    List<String> reservedProperties =
        properties.keySet().stream()
            .filter(propertiesMetadata::isReservedProperty)
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        reservedProperties.isEmpty(),
        "Properties are reserved and cannot be set: %s",
        reservedProperties);

    List<String> absentProperties =
        propertiesMetadata.propertyEntries().keySet().stream()
            .filter(propertiesMetadata::isRequiredProperty)
            .filter(k -> !properties.containsKey(k))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        absentProperties.isEmpty(),
        "Properties are required and must be set: %s",
        absentProperties);

    wildcardPropertyChecker(propertiesMetadata, properties);

    // use decode function to validate the property values
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (propertiesMetadata.containsProperty(key)) {
        checkValueFormat(key, value, propertiesMetadata.propertyEntries().get(key)::decode);
      }
    }
  }

  private static void wildcardPropertyChecker(
      PropertiesMetadata propertiesMetadata, Map<String, String> properties)
      throws IllegalArgumentException {
    List<String> wildcardProperties =
        propertiesMetadata.propertyEntries().keySet().stream()
            .filter(propertiesMetadata::isWildcardProperty)
            .collect(Collectors.toList());
    if (wildcardProperties.size() > 0) {
      List<String> wildcardConfigKeys =
          wildcardProperties.stream()
              .filter(key -> !key.contains(AuthorizationPropertiesMeta.getChainPlugsWildcard()))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          wildcardConfigKeys.size() == 1,
          "Wildcard properties `%s` not a valid wildcard config with values: %s",
          wildcardConfigKeys);
      String wildcardConfigKey = wildcardConfigKeys.get(0);
      List<String> wildcardConfigValues =
          Arrays.stream(
                  properties
                      .get(wildcardConfigKey)
                      .split(AuthorizationPropertiesMeta.getChainPluginsSplitter()))
              .map(String::trim)
              .collect(Collectors.toList());

      wildcardConfigValues.stream()
          .filter(v -> v.contains("."))
          .forEach(
              v -> {
                throw new IllegalArgumentException(
                    String.format(
                        "Wildcard property values cannot be set with `.` character in the `%s = %s`.",
                        wildcardConfigKey, properties.get(wildcardConfigKey)));
              });
      Preconditions.checkArgument(
          wildcardConfigValues.size() == wildcardConfigValues.stream().distinct().count(),
          "Duplicate values in wildcard config: %s",
          wildcardConfigValues);

      List<Pattern> patterns =
          wildcardProperties.stream()
              .filter(k -> k.contains(AuthorizationPropertiesMeta.getChainPlugsWildcard()))
              .collect(Collectors.toList())
              .stream()
              .map(
                  wildcard ->
                      wildcard
                          .replace(".", "\\.")
                          .replace(AuthorizationPropertiesMeta.getChainPlugsWildcard(), "([^.]+)"))
              .map(Pattern::compile)
              .collect(Collectors.toList());

      List<String> wildcardPrefix =
          wildcardProperties.stream()
              .filter(s -> s.contains(AuthorizationPropertiesMeta.getChainPlugsWildcard()))
              .map(
                  s ->
                      s.substring(
                          0, s.indexOf(AuthorizationPropertiesMeta.getChainPlugsWildcard())))
              .distinct()
              .collect(Collectors.toList());

      for (String key :
          properties.keySet().stream()
              .filter(
                  k ->
                      !k.equals(wildcardConfigKey)
                          && wildcardPrefix.stream().anyMatch(k::startsWith))
              .collect(Collectors.toList())) {
        boolean matches =
            patterns.stream()
                .anyMatch(
                    pattern -> {
                      Matcher matcher = pattern.matcher(key);
                      if (matcher.find()) {
                        String group = matcher.group(1);
                        return wildcardConfigValues.contains(group);
                      } else {
                        return false;
                      }
                    });
        Preconditions.checkArgument(
            matches,
            "Wildcard properties `%s` not a valid wildcard config with values: %s",
            key,
            wildcardConfigValues);
      }
    }
  }

  public static void validatePropertyForAlter(
      PropertiesMetadata propertiesMetadata,
      Map<String, String> upserts,
      Map<String, String> deletes) {
    for (Map.Entry<String, String> entry : upserts.entrySet()) {
      PropertyEntry<?> propertyEntry = propertiesMetadata.propertyEntries().get(entry.getKey());
      if (Objects.nonNull(propertyEntry)) {
        Preconditions.checkArgument(
            !propertyEntry.isImmutable() && !propertyEntry.isReserved(),
            "Property " + propertyEntry.getName() + " is immutable or reserved, cannot be set");
        checkValueFormat(entry.getKey(), entry.getValue(), propertyEntry::decode);
      }
    }

    for (Map.Entry<String, String> entry : deletes.entrySet()) {
      PropertyEntry<?> propertyEntry = propertiesMetadata.propertyEntries().get(entry.getKey());
      if (Objects.nonNull(propertyEntry)) {
        Preconditions.checkArgument(
            !propertyEntry.isImmutable() && !propertyEntry.isReserved(),
            "Property " + propertyEntry.getName() + " is immutable or reserved, cannot be deleted");
      }
    }
  }
}
