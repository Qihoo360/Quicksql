[English](./docker.md)|[中文](../zh/deployment/docker.md)

# Docker

[Docker Image Address](https://hub.docker.com/r/francisdu/quicksql)

## Easy to use:

- Install Docker service :
 
```shell 
yum update -y && yum intall docker -y
```

- Run Quick SQL image: 

```shell
docker run -d --name quicksql francisdu/quicksql
```

- Open terminal :
 
```shell
docker exec -it [CONTAINER NAME / ID] /bin/bash
```

- Run example :
 
```shell 
$QSQL_HOME/bin/quicksql-example.sh --class com.qihoo.qsql.CsvJoinWithEsExample --runner spark
``` 

## Installation location:

`QSQL_HOME = /usr/local/qsql`

`Spark_HOME = /usr/local/spark`