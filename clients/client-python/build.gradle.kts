/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
import io.github.piyushroshan.python.VenvTask

plugins {
  id("io.github.piyushroshan.python-gradle-miniforge-plugin") version "1.0.0"
}

pythonPlugin {
  pythonVersion.set(project.rootProject.extra["pythonVersion"].toString())
}

fun deleteCacheDir(targetDir: String) {
  project.fileTree(project.projectDir).matching {
    include("**/$targetDir/**")
  }.forEach { file ->
    val targetDirPath = file.path.substring(0, file.path.lastIndexOf(targetDir) + targetDir.length)
    project.file(targetDirPath).deleteRecursively()
  }
}

tasks {
  val pipInstall by registering(VenvTask::class) {
    venvExec = "pip"
    args = listOf("install", "-e", ".")
  }

  val test by registering(VenvTask::class) {
    dependsOn(pipInstall)
    venvExec = "python"
    args = listOf("-m", "unittest")
    workingDir = projectDir.resolve(".")
  }

  val integrationTest by registering(VenvTask::class) {
    dependsOn(pipInstall)
    venvExec = "python"
    args = listOf("-m", "unittest", "tests/test_integration_gravitino_client.py")
    workingDir = projectDir.resolve(".")
    environment = mapOf("PROJECT_VERSION" to project.version,
      "GRAVITINO_HOME" to project.rootDir.path + "/distribution/package")
  }

  val build by registering(VenvTask::class) {
  }

  val clean by registering(Delete::class) {
    delete("build")
    delete("gravitino.egg-info")

    doLast {
      deleteCacheDir(".pytest_cache")
      deleteCacheDir("__pycache__")
    }
  }
}
