[English](./docker.md)|[中文](../zh/deployment/docker.md)

# Quicksql on Docker

[Docker Hub Link](https://hub.docker.com/r/francisdu/quicksql)

## Preparation before use

- Install Docker on Cenots: `yum update -y && yum intall docker -y`

- Install Docker on Ubuntu: `apt update -y && apt-get intall docker -y`

## Start the latest version

- `docker run -it --name quicksql francisdu/quicksql /bin/bash`

- Run example: `quicksql-example.sh --class com.qihoo.qsql.CsvJoinWithEsExample`

## Start version 0.7.1

- `docker run -it --name quicksql francisdu/quicksql:0.7.1 /bin/bash`

- Run example: `quicksql-example.sh --class com.qihoo.qsql.CsvJoinWithEsExample`

## Start version 0.6

- `docker run -it --name quicksql francisdu/quicksql:0.6 /bin/bash`

- Run example: `qsql -e "select 1"`

## Installation location

`QSQL_HOME = /usr/local/qsql`

`Spark_HOME = /usr/local/spark`

## Documents

[Getting started](https://quicksql.readthedocs.io/en/latest/reference/getting-started/)