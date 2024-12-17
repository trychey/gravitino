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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":core"))
  compileOnly(project(":catalogs:catalog-common"))
  compileOnly(project(":catalogs:catalog-hadoop"))
  compileOnly(libs.hadoop3.common)
  compileOnly(libs.hadoop3.aws)

  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }

  implementation(libs.guava)
  implementation(libs.aws.iam)
  implementation(libs.aws.policy)
  implementation(libs.aws.sts)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  relocate("org.apache.commons.lang3", "org.apache.gravitino.shaded.org.apache.commons.lang3")
  relocate("com.google.common", "org.apache.gravitino.shaded.com.google.common")
  relocate("com.fasterxml.jackson", "org.apache.gravitino.shaded.com.fasterxml.jackson")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}
