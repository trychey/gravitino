/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

dependencies {
  implementation(project(":clients:filesystem-hadoop3"))
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "com.datastrato.gravitino.shaded.com.google")
  relocate("com.github.benmanes.caffeine", "com.datastrato.gravitino.shaded.com.github.benmanes.caffeine")
  relocate("google", "com.datastrato.gravitino.shaded.google")
  relocate("javax.annotation", "com.datastrato.gravitino.shaded.javax.annotation")
  relocate("org.apache.hc", "com.datastrato.gravitino.shaded.org.apache.hc")
  relocate("org.apache.log4j", "com.datastrato.gravitino.shaded.org.apache.log4j")
  relocate("org.apache.logging", "com.datastrato.gravitino.shaded.org.apache.logging")
  relocate("org.checkerframework", "com.datastrato.gravitino.shaded.org.checkerframework")
  relocate("org.slf4j", "com.datastrato.gravitino.shaded.org.slf4j")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
