#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM micr.cloud.mioffice.cn/devops-public/maven-mi-repo:3-jdk-8-slim as builder
ARG BUILD_CLUSTER
RUN echo "Build cluster is: $BUILD_CLUSTER"

WORKDIR /opt
COPY . ./

RUN ./gradlew clean compileDistribution --no-build-cache -Pcluster=$BUILD_CLUSTER -x test

FROM micr.cloud.mioffice.cn/container/xiaomi_centos7:openjdk1.8
ENV TZ "Asia/Shanghai"
ARG HERA_AGENT_PATH=/home/work/app/mitelemetry/agent/opentelemetry-javaagent-all.jar
ARG MYSQL_DRIVER_PATH=/opt/gravitino/libs/mysql-connector-java-5.1.49.jar

COPY --from=builder /opt/entrypoint.sh /opt/entrypoint.sh
COPY --from=builder /opt/distribution/package /opt/gravitino
ADD https://pkgs.d.xiaomi.net/artifactory/aliyun-maven-central/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar $MYSQL_DRIVER_PATH
ADD https://pkgs.d.xiaomi.net/artifactory/releases/io/opentelemetry/javaagent/opentelemetry-javaagent/1.13.1-milatest/opentelemetry-javaagent-1.13.1-milatest.jar $HERA_AGENT_PATH

RUN chmod +x /opt/entrypoint.sh
RUN chmod -R +x /opt/gravitino

ENTRYPOINT ["/opt/entrypoint.sh"]