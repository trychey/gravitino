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
package org.apache.gravitino.oss.fs;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.Constants;

public class OSSFileSystemProvider implements FileSystemProvider {

  private static final String OSS_FILESYSTEM_IMPL = "fs.oss.impl";

  public static final Map<String, String> GRAVITINO_KEY_TO_OSS_HADOOP_KEY =
      ImmutableMap.of(
          OSSProperties.GRAVITINO_OSS_ENDPOINT, Constants.ENDPOINT_KEY,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, Constants.ACCESS_KEY_ID,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, Constants.ACCESS_KEY_SECRET);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Configuration configuration = new Configuration();
    config.forEach(
        (k, v) -> {
          if (k.startsWith(GRAVITINO_BYPASS)) {
            configuration.set(k.replace(GRAVITINO_BYPASS, ""), v);
          } else configuration.set(GRAVITINO_KEY_TO_OSS_HADOOP_KEY.getOrDefault(k, k), v);
        });

    // OSS do not use service loader to load the file system, so we need to set the impl class
    if (!config.containsKey(OSS_FILESYSTEM_IMPL)) {
      configuration.set(OSS_FILESYSTEM_IMPL, AliyunOSSFileSystem.class.getCanonicalName());
    }
    return AliyunOSSFileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public String scheme() {
    return "oss";
  }

  @Override
  public String name() {
    return "oss";
  }
}
