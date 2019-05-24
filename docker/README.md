# Dockerfile for QuickSQL

### [Dokerhub Address](https://cloud.docker.com/repository/docker/francisdu/quicksql/)

## Usage:(Centos)
#### Step 1:
Install Docker service : `yum update -y && yum intall docker -y`
#### Step 2:
Run QuickSQL : ` docker run -d --name quicksql francisdu/quicksql`
#### Step 3:
Open a terminal : `docker exec -it [CONTAINER NAME / ID] /bin/bash`
#### Step 4:
Execute a query : `qsql -e "select * from DEPTS"`

## Installation location:


`QSQL_HOME = /usr/local/qsql-0.6`

`Spark_HOME : = /usr/local/spark-2.3.3-bin-hadoop2.7` 



