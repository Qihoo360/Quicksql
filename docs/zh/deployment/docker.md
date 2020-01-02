[English](../../deployment/docker.md)|[中文](./docker.md)

# Docker

[Docker镜像地址](https://hub.docker.com/r/francisdu/quicksql)

## 快速启动

- 安装 Docker :
 
```shell 
yum update -y && yum intall docker -y
```

- 运行 Quick SQL: 

```shell
docker run -d --name quicksql francisdu/quicksql
```

- 打开一个终端:
 
```shell
docker exec -it [CONTAINER NAME / ID] /bin/bash
```

- 运行简单的例子 :
 
```shell 
$QSQL_HOME/bin/run-example com.qihoo.qsql.CsvJoinWithEsExample
``` 

## 安装位置

`QSQL_HOME = /usr/local/qsql`

`Spark_HOME = /usr/local/spark`