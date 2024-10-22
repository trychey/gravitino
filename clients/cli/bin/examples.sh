#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# These examples assume you have the Apache Gravitino playground running.

unset GRAVITINO_METALAKE
shopt -s expand_aliases
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'

# display help
gcli --help

# display version
gcli --version

# metalake details
gcli details

# metalake list
gcli list

# list all catalogs in a metalake 
gcli metalake list --metalake metalake_demo

# list catalog schema
gcli catalog list --metalake metalake_demo --name catalog_iceberg
gcli catalog list --metalake metalake_demo --name catalog_mysql
gcli catalog list --metalake metalake_demo --name catalog_postgres
gcli catalog list --metalake metalake_demo --name catalog_hive

# list catalog details
gcli catalog details --metalake metalake_demo --name catalog_iceberg
gcli catalog details --metalake metalake_demo --name catalog_mysql
gcli catalog details --metalake metalake_demo --name catalog_postgres
gcli catalog details --metalake metalake_demo --name catalog_hive

# list schema tables
gcli schema list --metalake metalake_demo --name catalog_postgres.hr
gcli schema list --metalake metalake_demo --name catalog_mysql.db
gcli schema list --metalake metalake_demo --name catalog_hive.sales

# list schema details
gcli schema details --metalake metalake_demo --name catalog_postgres.hr
gcli schema details --metalake metalake_demo --name catalog_mysql.db
gcli schema details --metalake metalake_demo --name catalog_hive.sales

# list table details
gcli table list --metalake metalake_demo --name catalog_postgres.hr.departments
gcli table list --metalake metalake_demo --name catalog_mysql.db.iceberg_tables
gcli table list --metalake metalake_demo --name catalog_hive.sales.products

# Exmaples where metalake is set in an evironment variable
export GRAVITINO_METALAKE=metalake_demo

# metalake details
gcli metalake details

# list all catalogs in a metalake 
gcli metalake list

# list catalog schema
gcli catalog list --name catalog_iceberg
gcli catalog list --name catalog_mysql
gcli catalog list --name catalog_postgres
gcli catalog list --name catalog_hive

# list catalog details
gcli catalog details --name catalog_iceberg
gcli catalog details --name catalog_mysql
gcli catalog details --name catalog_postgres
gcli catalog details --name catalog_hive

# list schema tables
gcli schema list --name catalog_postgres.hr
gcli schema list --name catalog_mysql.db
gcli schema list --name catalog_hive.sales

# list schema details
gcli schema details --name catalog_postgres.hr
gcli schema details --name catalog_mysql.db
gcli schema details --name catalog_hive.sales

# list table details
gcli table list --name catalog_postgres.hr.departments
gcli table list --name catalog_mysql.db.iceberg_tables
gcli table list --name catalog_hive.sales.products

unset GRAVITINO_METALAKE
