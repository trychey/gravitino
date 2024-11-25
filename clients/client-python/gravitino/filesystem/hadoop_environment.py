"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging
import os
import re
import subprocess

from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException

logger = logging.getLogger(__name__)


class HadoopEnvironment:
    """Initialize hadoop environment, this is a singleton class."""

    instance = None
    init_flag = False

    def __init__(self):
        if HadoopEnvironment.init_flag:
            return

        self._init_java_env()
        self._init_hadoop_env()

        HadoopEnvironment.init_flag = True

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    @staticmethod
    def _init_java_env():
        java_home = os.environ.get("JAVA_HOME")
        default_java_home = os.path.join(
            os.path.expanduser("~"), ".config", "gravitino", "java"
        )
        if java_home is not None:
            logger.info("JAVA_HOME is set to %s", java_home)
        elif os.path.exists(default_java_home):
            os.environ["JAVA_HOME"] = default_java_home
            os.environ["CLASSPATH"] = os.path.join(default_java_home, "lib", "*")
            os.environ["PATH"] = (
                os.path.join(default_java_home, "bin")
                + os.pathsep
                + os.environ.get("PATH", "")
            )
            logger.info("JAVA_HOME is set to %s", os.environ["JAVA_HOME"])
            logger.info("CLASSPATH is set to %s", os.environ["CLASSPATH"])
            logger.info("PATH is set to %s", os.environ["PATH"])
        else:
            raise GravitinoRuntimeException(
                "The JAVA_HOME is not set, you can set it manually or run `init-java-env` to set it."
            )

    # pylint: disable=too-many-branches
    @staticmethod
    def _init_hadoop_env():
        hadoop_home = os.environ.get("HADOOP_HOME")
        hadoop_conf_dir = os.environ.get("HADOOP_CONF_DIR")

        if hadoop_home is None or hadoop_conf_dir is None:
            default_hadoop_home = os.path.join(
                os.path.expanduser("~"), ".config", "gravitino", "hadoop"
            )
            if os.path.exists(default_hadoop_home):
                hadoop_home = default_hadoop_home
                hadoop_conf_dir = os.path.join(hadoop_home, "etc", "hadoop")
                os.environ["HADOOP_HOME"] = hadoop_home
                os.environ["HADOOP_CONF_DIR"] = hadoop_conf_dir

        if hadoop_home is not None and len(hadoop_home) > 0:
            hadoop_shell = f"{hadoop_home}/bin/hadoop"
            if not os.path.exists(hadoop_shell):
                raise GravitinoRuntimeException(
                    f"Hadoop shell:{hadoop_shell} doesn't exist."
                )
            try:
                result = subprocess.run(
                    [hadoop_shell, "classpath", "--glob"],
                    capture_output=True,
                    text=True,
                    # we set check=True to raise exception if the command failed.
                    check=True,
                )
                classpath_str = str(result.stdout)
                origin_classpath = os.environ.get("CLASSPATH")

                # compatible with lavafs in Notebook and PySpark in the cluster
                potential_lavafs_jar_files = []
                current_dir = os.getcwd()
                spark_classpath = os.environ.get("SPARK_DIST_CLASSPATH")
                if spark_classpath is not None and len(spark_classpath) > 0:
                    file_list = os.listdir(current_dir)
                    pattern = re.compile(r"^lavafs.*\.jar$")
                    potential_lavafs_jar_files = [
                        file
                        for file in file_list
                        if pattern.match(file)
                        and os.path.isfile(os.path.join(current_dir, file))
                    ]

                # configure the hadoop conf dir in the classpath
                if hadoop_conf_dir is not None and len(hadoop_conf_dir) > 0:
                    new_classpath = hadoop_conf_dir + ":" + classpath_str
                else:
                    new_classpath = classpath_str

                # if lavafs jar is found, add it to the classpath
                if (
                    potential_lavafs_jar_files is not None
                    and len(potential_lavafs_jar_files) > 0
                ):
                    for lava_jar in potential_lavafs_jar_files:
                        new_classpath = (
                            new_classpath + ":" + os.path.join(current_dir, lava_jar)
                        )

                # set the classpath
                if origin_classpath is None or len(origin_classpath) == 0:
                    os.environ["CLASSPATH"] = new_classpath
                else:
                    os.environ["CLASSPATH"] = origin_classpath + ":" + new_classpath
            except subprocess.CalledProcessError as e:
                raise GravitinoRuntimeException(
                    f"Command failed with return code {e.returncode}, stdout:{e.stdout}, stderr:{e.stderr}"
                ) from e
        else:
            logger.warning("HADOOP_HOME is not set, skip setting CLASSPATH")
