
#bin/bash

#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# build gravitino docker image: ./build-gravitino-image ${cluster} ${image_name}

./gradlew compileDistribution -x test -PjdkVersion=8 -Pcluster=$1 --no-build-cache \
"-Dorg.gradle.jvmargs=-Xmx16g -XX:MaxPermSize=2048m -XX:+HeapDumpOnOutOfMemoryError"

if [ $? -ne 0 ]; then
  echo "Compile gravitino failed!"
  exit 1
else
  docker build --force-rm --no-cache -t $2 -f Dockerfile .
fi


