[English](../../deployment/docker.md)|[中文](./docker.md)

# Docker 上的 Quicksql

[Docker Hub 地址](https://hub.docker.com/r/francisdu/quicksql)

## 使用之前的准备

- 在 Centos 上安装 Docker: `yum update -y && yum intall docker -y`

- 在 Ubuntu 上安装 Docker: `apt update -y && apt-get intall docker -y`

## 体验最新版本

- `docker run -it --name quicksql francisdu/quicksql /bin/bash`

- Run example: `quicksql-example.sh --class com.qihoo.qsql.CsvJoinWithEsExample --runner spark`

## 体验 0.7.1 版本

- `docker run -it --name quicksql francisdu/quicksql:0.7.1 /bin/bash`

- Run example: `quicksql-example.sh --class com.qihoo.qsql.CsvJoinWithEsExample --runner spark`

## 体验 0.6 版本

- `docker run -it --name quicksql francisdu/quicksql:0.6 /bin/bash`

- Run example: `qsql -e "select 1"`

## 安装位置

`QSQL_HOME = /usr/local/qsql`

`Spark_HOME = /usr/local/spark`

## 文档

[入门教程](https://quicksql.readthedocs.io/en/latest/zh/reference/getting-started/)