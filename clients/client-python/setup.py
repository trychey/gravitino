"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from setuptools import find_packages, setup

from scripts.tools import config

try:
    with open("README.md") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Gravitino Python client"

setup(
    name="gravitino",
    description="Python lib/client for Gravitino",
    version="0.5.0.3.dev1",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datastrato/gravitino",
    author="datastrato",
    author_email="support@datastrato.com",
    python_requires=">=3.8",
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    install_requires=open("requirements.txt").read(),
    extras_require={
        "dev": open("requirements-dev.txt").read(),
    },
    include_package_data=True,
    package_data={ 'gravitino': ['version.ini'], },
    cmdclass={
        "build": config.PostBuildCommand,
        "install": config.PostInstallCommand
    },
    entry_points={
        'console_scripts': [
            'init-hadoop-env=scripts.tools.env:init_hadoop_env',
            'init-java-env=scripts.tools.env:init_java_env'
        ]
    },
)
