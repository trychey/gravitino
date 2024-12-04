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

package org.apache.gravitino.s3.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.VersionInfo;

public class S3FileSystemProvider implements FileSystemProvider {
  private static VersionedClassLoader versionedClassLoader;

  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_S3_HADOOP_KEY =
      ImmutableMap.of(
          S3Properties.GRAVITINO_S3_ENDPOINT, Constants.ENDPOINT,
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, Constants.ACCESS_KEY,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, Constants.SECRET_KEY);

  // We can't use Constants.AWS_CREDENTIALS_PROVIDER as in 2.7, this key does not exist.
  private static final String S3_CREDENTIAL_KEY = "fs.s3a.aws.credentials.provider";
  private static final String S3_SIMPLE_CREDENTIAL =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {

    try {
      String hadoopVersion = VersionInfo.getVersion();
      if (versionedClassLoader == null) {
        versionedClassLoader = VersionedClassLoader.loadVersion(hadoopVersion);
      }

      ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(versionedClassLoader);
        Configuration configuration = new Configuration();
        Map<String, String> hadoopConfMap =
            FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_S3_HADOOP_KEY);

        if (!hadoopConfMap.containsKey(S3_CREDENTIAL_KEY)) {
          hadoopConfMap.put(S3_CREDENTIAL_KEY, S3_SIMPLE_CREDENTIAL);
        }

        hadoopConfMap.forEach(configuration::set);

        // Hadoop-aws 2 does not support IAMInstanceCredentialsProvider
        if (configuration.get(S3_CREDENTIAL_KEY) != null
            && versionedClassLoader.getVersion().startsWith("hadoop2")
            && configuration.get(S3_CREDENTIAL_KEY).contains("IAMInstanceCredentialsProvider")) {
          configuration.set(S3_CREDENTIAL_KEY, S3_SIMPLE_CREDENTIAL);
        }

        return FileSystem.newInstance(path.toUri(), configuration);
      } finally {
        Thread.currentThread().setContextClassLoader(oldClassLoader);
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to load the Hadoop versioned class loader", e);
    } catch (Exception e) {
      throw new IOException("Failed to create the S3AFileSystem instance", e);
    }
  }

  /**
   * Get the scheme of the FileSystem. Attention, for S3 the schema is "s3a", not "s3". Users should
   * use "s3a://..." to access S3.
   */
  @Override
  public String scheme() {
    return "s3a";
  }

  @Override
  public String name() {
    return "s3";
  }
}
