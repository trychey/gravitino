"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging
import os
import platform
import shutil
import subprocess
import sys

# pylint: disable=line-too-long
HADOOP_PACKAGE_URI = "https://cnbj1-fds.api.xiaomi.net/gravitino/hadoop-client-pack/hadoop-3.1.0-mdh3.1.1.44-gravitino-20241028.tar.gz"
# For now, the gravitino sdk can only be used in the development machine, so we only support linux environment.
# The krb5.conf file is packaged to ${java.home}/lib/security/krb5.conf,
# such as /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/krb5.conf.
JDK_PACKAGE_URI = {
    "Linux": "https://cnbj1-fds.api.xiaomi.net/gravitino/jdk/openjdk-8u432-b06-linux-x64.tar.gz"
}
HADOOP_PACKAGE_NAME = "hadoop-3.1.0-mdh3.1.1.44-gravitino.tar.gz"

logger = logging.getLogger(__name__)


def _validate_java_env():
    java_home_env = os.environ.get("JAVA_HOME")
    java_path = os.path.join(os.path.expanduser("~"), ".config", "gravitino", "java")
    return java_home_env or os.path.exists(java_path)


def _init_config_directory(sub_dir: str):
    config_dir = os.path.join(os.path.expanduser("~"), ".config", "gravitino", sub_dir)
    if os.path.exists(config_dir):
        logger.info(
            "Directory already exists, delete the package and reinstall: %s",
            config_dir,
        )
        shutil.rmtree(config_dir)
    os.makedirs(config_dir)
    logger.info("Created directory: %s", config_dir)
    return config_dir


def init_hadoop_env():
    is_java_home_exist = _validate_java_env()
    hadoop_home = _init_config_directory("hadoop")

    # download hadoop package and extract it to the hadoop home directory
    gravitino_package = os.path.join(hadoop_home, HADOOP_PACKAGE_NAME)
    try:
        subprocess.run(
            [
                "wget",
                "-O",
                gravitino_package,
                HADOOP_PACKAGE_URI,
            ],
            check=True,
        )
        subprocess.run(
            [
                "tar",
                "-zxvf",
                gravitino_package,
                "-C",
                hadoop_home,
                "--strip-components=1",
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("Failed to download or extract hadoop package: %s", e)
        sys.exit(1)
    os.remove(gravitino_package)
    logger.info("Deleted hadoop package: %s", gravitino_package)
    logger.info("Config the gravitino hadoop environment successfully!")
    if not is_java_home_exist:
        logger.warning(
            "WARN: The Java is not installed. You should install Java first or run `init-java-env` to install it."
        )


def init_java_env():
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        logger.info("JAVA_HOME already exists: %s", java_home)
        return

    system_info = platform.system()
    logger.info("Current platform：%s", system_info)
    jar_url = JDK_PACKAGE_URI.get(system_info)
    if not jar_url:
        logger.info(
            "The platform is not supported: %s. Please install the java manually.",
            system_info,
        )
        return

    # download and extract the jdk package
    java_home = _init_config_directory("java")
    java_package = os.path.join(java_home, f"openjdk8-{system_info}.tar.gz")
    try:
        subprocess.run(
            [
                "wget",
                "-O",
                java_package,
                jar_url,
            ],
            check=True,
        )
        subprocess.run(
            ["tar", "-zxvf", java_package, "-C", java_home, "--strip-components=1"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("Failed to download or extract hadoop: %s", e)
        sys.exit(1)
    os.remove(java_package)
    logger.info("The java is installed successfully: %s", java_home)
