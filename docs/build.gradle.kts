/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import com.github.gradle.node.npm.task.NpmTask
import com.github.gradle.node.npm.task.NpxTask

tasks {
  val changeRepository by registering(NpmTask::class) {
    args.set(listOf("config", "set", "registry", "https://pkgs.d.xiaomi.net/artifactory/api/npm/mi-npm/"))
  }

  val lintOpenAPI by registering(NpxTask::class) {
    dependsOn(changeRepository)
    command.set("@redocly/cli@1.5.0")
    args.set(listOf("lint", "--extends=recommended-strict", "${project.projectDir}/open-api/openapi.yaml"))
  }

  build {
    dependsOn(lintOpenAPI)
  }
}
