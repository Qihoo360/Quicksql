# Dockerfile for QuickSQL

### [Dokerhub Address](https://hub.docker.com/r/francisdu/quicksql)

## Usage:(Centos)
#### Step 1:
Install Docker service : `yum update -y && yum intall docker -y`
#### Step 2:
Run QuickSQL : ` docker run -d --name quicksql francisdu/quicksql`
#### Step 3:
Open a terminal : `docker exec -it [CONTAINER NAME / ID] /bin/bash`
#### Step 4:
Run a example : `$QSQL_HOME/bin/run-example com.qihoo.qsql.CsvJoinWithEsExample`

## Installation location:

`QSQL_HOME = /usr/local/qsql-0.6`

`Spark_HOME = /usr/local/spark-2.3.3-bin-hadoop2.7`

## Documents
[架构介绍](https://github.com/Qihoo360/Quicksql/blob/master/doc/README%E6%96%87%E6%A1%A3.md)

[部署文档](https://github.com/Qihoo360/Quicksql/blob/master/doc/BUILD%E6%96%87%E6%A1%A3.md)

[命令行](https://github.com/Qihoo360/Quicksql/blob/master/doc/CLI%E6%96%87%E6%A1%A3.md)

[JDBC](https://github.com/Qihoo360/Quicksql/blob/master/doc/JDBC%E6%96%87%E6%A1%A3.md)

[API](https://github.com/Qihoo360/Quicksql/blob/master/doc/API%E6%96%87%E6%A1%A3.md)