/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

val sparkVersion = "3.1.2-mdh1.0.0-SNAPSHOT"

dependencies {
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.commons.cli)
  compileOnly("org.apache.spark:spark-sql_2.12:$sparkVersion")

  testImplementation(libs.hadoop3.common)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "com.datastrato.gravitino.shaded.com.google")
  relocate("com.fasterxml", "com.datastrato.gravitino.shaded.com.fasterxml")
  relocate("org.apache.httpcomponents", "com.datastrato.gravitino.shaded.org.apache.httpcomponents")
  relocate("org.apache.commons", "com.datastrato.gravitino.shaded.org.apache.commons") {
    exclude("org.apache.commons.cli")
  }
  relocate("org.antlr", "com.datastrato.gravitino.shaded.org.antlr")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
