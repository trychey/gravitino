/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  id("java")
}

repositories {
  maven {
    name = "publicVirtual"
    url = uri("https://pkgs.d.xiaomi.net/artifactory/maven-public-virtual/")
  }
  maven {
    name = "releaseVirtual"
    url = uri("https://pkgs.d.xiaomi.net:443/artifactory/maven-release-virtual/")
  }
  maven {
    name = "snapshotVirtual"
    url = uri("https://pkgs.d.xiaomi.net:443/artifactory/maven-snapshot-virtual/")
  }
  maven {
    name = "aliyunMavenCentral"
    url = uri("https://pkgs.d.xiaomi.net:443/artifactory/aliyun-maven-central/")
  }
  mavenLocal()
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.cli)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.commons.io)
  testImplementation(libs.guava)
  testImplementation(libs.httpclient5)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)

  testImplementation(platform("org.junit:junit-bom:5.9.1"))
  testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
  useJUnitPlatform()
}

val testJar by tasks.registering(Jar::class) {
  archiveClassifier.set("tests")
  from(sourceSets["test"].output)
}

configurations {
  create("testArtifacts")
}

artifacts {
  add("testArtifacts", testJar)
}
