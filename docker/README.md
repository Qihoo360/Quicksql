# Dockerfile for QuickSQL

[Docker Image](https://hub.docker.com/r/francisdu/quicksql)

## Easy to use:(Based on Centos7)

- Step 1:
Install Docker service : `yum update -y && yum intall docker -y`

- Step 2:
Run Quick SQL image: ` docker run -d --name quicksql francisdu/quicksql`

- Step 3:
Open terminal : `docker exec -it [CONTAINER NAME / ID] /bin/bash`

- Step 4:
Run example : `$QSQL_HOME/bin/run-example com.qihoo.qsql.CsvJoinWithEsExample`

## Installation location:

`QSQL_HOME = /usr/local/qsql`

`Spark_HOME = /usr/local/spark`

## Documents

[架构介绍](../doc/README文档.md) | [部署文档](../doc/BUILD文档.md) | [命令行文档](../doc/CLI文档.md) | [JDBC文档](../doc/JDBC文档.md) | [API文档](../doc/API文档.md)