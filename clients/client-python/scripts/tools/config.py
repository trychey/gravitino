"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from setuptools.command.build import build
from setuptools.command.install import install

from scripts.tools import generate_version, env


class PostBuildCommand(build):
    def run(self):
        # Generate version file
        generate_version.main()
        build.run(self)


class PostInstallCommand(install):
    def run(self):
        install.run(self)
        # Init java environment
        env.init_java_env()
        # Init hadoop environment
        env.init_hadoop_env()
