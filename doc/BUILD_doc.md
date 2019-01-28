## Building Tutorial

[TOC]

### Requirements

- Java >= 1.8
- Scala >= 2.11
- Spark >= 2.2
- [Options] MySQL, Elasticsearch, Hive

### Deployment

Uncompress the package qsql-0.5.tar.gz 

```shell
tar -zxvf ./qsql-0.5.tar.gz
```

Create a soft link

```shell
ln -s qsql-0.5/ qsql
```

The main directory structure after decompression of the release package is：

- bin: included all of scripts for building environment and running sql.
- conf: included all of configures in runtime.
- data: stored data for testing.
- metastore: included a embedded database and create table statements scripts for managing metadata. 

In directory ```$QSQL_HOME/conf```, configure the following files：

- base-env.sh：Included correlated environment variables：
  - JAVA_HOME
  - SPARK_HOME
  - QSQL_CLUSTER_URL
  - QSQL_HDFS_TMP
- qsql-runner.properties：Included serveral runtime properties
- log4j.properties：Included logger level

## Getting Started

### QSQL Shell

```
./bin/qsql -e "select 1"
```

### Query Example

Several sample queries are included with QSQL. To run one of them, use ```./run-example <class> [params]``` 

Example 1: Memory Table Query

```
./bin/run-example com.qihoo.qsql.CsvScanExample
```

Example 2: Hive Join MySQL

```
./bin/run-example com.qihoo.qsql.CsvJoinWithEsExample
```

**Note**:

If you are running a hybrid query, make sure the current machine has deployed Spark, Hive and MySQL environment and inserted the correct connection information of Hive and MySQL into the metastore.

## Properties Configure

### Environment Variables

| Property Name                       | Meaning                 |
| ----------------------------------- | ----------------------- |
| JAVA_HOME                           | Java installation path  |
| SPARK_HOME                          | Spark installation path |
| QSQL_CLUSTER_URL                    | Hadoop cluster url      |
| QSQL_HDFS_TMP (Option)              | Hadoop tmp path         |
| QSQL_DEFAULT_WORKER_NUM (Option)    | Worker number           |
| QSQL_DEFAULT_WORKER_MEMORY (Option) | Worker memory size      |
| QSQL_DEFAULT_DRIVER_MEMORY (Option) | Driver memory size      |
| QSQL_DEFAULT_MASTER (Option)        | Cluster mode in Spark   |
| QSQL_DEFAULT_RUNNER (Option)        | Execution mode          |

### Runtime Variables

#### Application Properties

| Property Name                     | Default            | Meaning                                                      |
| --------------------------------- | ------------------ | ------------------------------------------------------------ |
| spark.sql.hive.metastore.jars     | builtin            | Hive Jars                                                    |
| spark.sql.hive.metastore.version  | 1.2.1              | Hive version                                                 |
| spark.local.dir                   | /tmp               | Temporary file path used by Spark                            |
| spark.driver.userClassPathFirst   | true               | User jars are loaded first during Spark execution            |
| spark.sql.broadcastTimeout        | 300                | Max broadcast waited Time                                    |
| spark.sql.crossJoin.enabled       | true               | Allow Spark SQL execute cross join                           |
| spark.speculation                 | true               | Spark will start task speculation execution                  |
| spark.sql.files.maxPartitionBytes | 134217728（128MB） | The maximum number of bytes of a single partition when Spark reads a file |

#### Metadata Properties

| Property Name               | Default                | Meaning                                                      |
| --------------------------- | ---------------------- | ------------------------------------------------------------ |
| meta.storage.mode           | intern                 | Metadata storage mode. intern: read the metadata stored in the embeded sqlite database. extern: read the metadata stored in the external database |
| meta.intern.schema.dir      | ../metastore/schema.db | The path of embeded database file                            |
| meta.extern.schema.driver   | （none）               | The driver of external database                              |
| meta.extern.schema.url      | （none）               | The connection url of external database                      |
| meta.extern.schema.user     | （none）               | The user name of external database                           |
| meta.extern.schema.password | （none）               | The password of external database                            |

## Metadata Management

### Tables

#### DBS 

| Fields  | Note                 | Sample           |
| ------- | -------------------- | ---------------- |
| DB_ID   | Database ID          | 1                |
| DESC    | Database Description | es index         |
| NAME    | Database Name        | es_profile_index |
| DB_TYPE | Database Type        | es, Hive, MySQL  |

#### DATABASE_PARAMS

| Fields      | Note        | Sample   |
| ----------- | ----------- | -------- |
| DB_ID       | Database ID | 1        |
| PARAM_KEY   | Param Key   | UserName |
| PARAM_VALUE | Param Value | root     |

#### TBLS

| Fields       | Note          | Sample              |
| ------------ | ------------- | ------------------- |
| TBL_ID       | Table ID      | 101                 |
| CREATED_TIME | Creation Time | 2018-10-22 14:36:10 |
| DB_ID        | Database ID   | 1                   |
| TBL_NAME     | Table Name    | student             |

#### COLUMNS

| Fields      | Note          | Sample       |
| ----------- | ------------- | ------------ |
| CD_ID       | Column ID     | 10101        |
| COMMENT     | Field Comment | Student Name |
| COLUMN_NAME | Field Name    | name         |
| TYPE_NAME   | Field Type    | varchar      |
| INTEGER_IDX | Field Index   | 1            |

### Embedded SQLite Database

In the directory ```$QSQL_HOME/metastore```, included following files：

- sqlite3：SQLite command line tool
- schema.db：SQLite embedded database
- ./linux-x86/sqldiff：A tool that displays the differences between SQLite databases.
- ./linux-x86/sqlite3_analyzer：A command-line utility program that measures and displays how much and how efficiently space is used by individual tables and indexes with an SQLite database file

Connect to the schema.db database via sqlite3 and manipulate the metadata table

```shell
sqlite3 ../schema.db
```

### External MySQL Database

Change the embedded SQLite data to a MySQL database

```shell
vim metadata.properties
```

> meta.storage.mode=extern
> meta.extern.schema.driver    = com.mysql.jdbc.Driver
> meta.extern.schema.url       = jdbc:mysql://ip:port/db?useUnicode=true
> meta.extern.schema.user      = YourName
> meta.extern.schema.password  = YourPassword

Initialize the sample data to the MySQL database

```shell
cd $QSQL_HOME/bin/
./metadata --dbType mysql --action init
```

### Configure Metadata

#### Hive

Sample Configuration：

#### DBS

| DB_ID | DESC         | NAME          | DB_TYPE |
| ----- | ------------ | ------------- | ------- |
| 26    | hive message | hive_database | hive    |

#### DATABASE_PARAMS

| DB_ID | PARAM_KEY | PARAM_VALUE  |
| ----- | --------- | ------------ |
| 26    | cluster   | cluster_name |

#### TBLS

| TBL_ID | CREATE_TIME         | DB_ID | TBL_NAME    |
| ------ | ------------------- | ----- | ----------- |
| 60     | 2018-11-06 10:44:51 | 26    | hive_mobile |

#### COLUMNS

| CD_ID | COMMENT | COLUMN_NAME | TYPE_NAME | INTEGER_IDX |
| ----- | ------- | ----------- | --------- | ----------- |
| 60    |         | retsize     | string    | 1           |
| 60    |         | im          | string    | 2           |
| 60    |         | wto         | string    | 3           |
| 60    |         | pro         | int       | 4           |
| 60    |         | pday        | string    | 5           |

#### Elasticsearch

Sample Configuration：

#### DBS 

| DB_ID | DESC       | NAME     | DB_TYPE |
| ----- | ---------- | -------- | ------- |
| 24    | es message | es_index | es      |

#### DATABASE_PARAMS

| DB_ID | PARAM_KEY   | PARAM_VALUE      |
| ----- | ----------- | ---------------- |
| 24    | esNodes     | localhost        |
| 24    | esPort      | 9025             |
| 24    | esUser      | es_user          |
| 24    | esPass      | es_password      |
| 24    | esIndex     | es_index/es_type |
| 24    | esScrollNum | 156              |

#### TBLS

| TBL_ID | CREATE_TIME         | DB_ID | TBL_NAME |
| ------ | ------------------- | ----- | -------- |
| 57     | 2018-11-06 10:44:51 | 24    | profile  |

#### COLUMNS

| CD_ID | COMMENT | COLUMN_NAME | TYPE_NAME | INTEGER_IDX |
| ----- | ------- | ----------- | --------- | ----------- |
| 57    | comment | id          | int       | 1           |
| 57    | comment | name        | string    | 2           |
| 57    | comment | country     | string    | 3           |
| 57    | comment | gender      | string    | 4           |
| 57    | comment | operator    | string    | 5           |

#### MySQL

Sample Configuration：

#### DBS 

| DB_ID | DESC             | NAME           | DB_TYPE |
| ----- | ---------------- | -------------- | ------- |
| 25    | mysql db message | mysql_database | mysql   |

#### DATABASE_PARAMS

| DB_ID | PARAM_KEY    | PARAM_VALUE                                |
| ----- | ------------ | ------------------------------------------ |
| 25    | jdbcDriver   | com.mysql.jdbc.Driver                      |
| 25    | jdbcUrl      | jdbc:mysql://localhost:3306/mysql_database |
| 25    | jdbcUser     | root                                       |
| 25    | jdbcPassword | root                                       |

#### TBLS

| TBL_ID | CREATE_TIME         | DB_ID | TBL_NAME  |
| ------ | ------------------- | ----- | --------- |
| 58     | 2018-11-06 10:44:51 | 25    | test_date |

#### COLUMNS

| CD_ID | COMMENT | COLUMN_NAME | TYPE_NAME | INTEGER_IDX |
| ----- | ------- | ----------- | --------- | ----------- |
| 58    | comment | id          | int       | 1           |
| 58    | comment | name        | string    | 2           |