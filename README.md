![200_200](./doc/picture/logo.jpeg)   



[![license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](./LICENSE)[![Release Version](https://img.shields.io/badge/release-0.5-red.svg)]()[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

QSQL is a SQL query product which can be used for specific datastore queries or multiple datastores correlated queries.  It supports relational databases, non-relational databases and even datastore which does not support SQL (such as Elasticsearch, Druid) . In addition, a SQL query can join or union data from multiple datastores in QSQL. For example, you can perform unified SQL query on one situation that a part of data stored on Elasticsearch, but the other part of data stored on Hive. The most important is that QSQL is not dependent on any intermediate compute engine, users only need to focus on data and unified SQL grammar to finished statistics and analysis.

[English](./doc/README_doc.md)|[中文](./doc/README文档.md)

![1540973404791](./doc/picture/p1.png)

QSQL architecture consists of three layers：

- Parsing  Layer: Used for parsing, validation, optimization of SQL  statements, splitting of mixed SQL and finally generating Query Plan;

- Computing Layer: For routing query plan to a  specific execution plan, then interpreted to executable code for given  storage or engine(such as Elasticsearch JSON query or Hive HQL);

- Storage layer: For data prepared extraction and storage;

## Build

### Requirements

- CentOS 6.2
- java >= 1.8
- scala >= 2.11
- spark >= 2.2
- [Options] MySQL, Elasticsearch, Hive, Druid

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

## Examples

### QSQL Shell

```
./bin/qsql -e "select 1"
```

Detailed：[English](./doc/CLI_doc.md)|[中文](./doc/CLI文档.md)

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

Detailed：[English](./doc/API_doc.md)|[中文](./doc/API文档.md)

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

| DB_ID | DESC         | NAME          | DB_TYPE |
| ----- | ------------ | ------------- | ------- |
| 26    | hive message | hive_database | hive    |

| DB_ID | PARAM_KEY | PARAM_VALUE  |
| ----- | --------- | ------------ |
| 26    | cluster   | cluster_name |

| TBL_ID | CREATE_TIME         | DB_ID | TBL_NAME    |
| ------ | ------------------- | ----- | ----------- |
| 60     | 2018-11-06 10:44:51 | 26    | hive_mobile |

| CD_ID | COMMENT | COLUMN_NAME | TYPE_NAME | INTEGER_IDX |
| ----- | ------- | ----------- | --------- | ----------- |
| 60    |         | retsize     | string    | 1           |
| 60    |         | im          | string    | 2           |
| 60    |         | wto         | string    | 3           |
| 60    |         | pro         | int       | 4           |
| 60    |         | pday        | string    | 5           |

#### Elasticsearch

Sample Configuration：

| DB_ID | DESC       | NAME     | DB_TYPE |
| ----- | ---------- | -------- | ------- |
| 24    | es message | es_index | es      |

| DB_ID | PARAM_KEY   | PARAM_VALUE      |
| ----- | ----------- | ---------------- |
| 24    | esNodes     | localhost        |
| 24    | esPort      | 9025             |
| 24    | esUser      | es_user          |
| 24    | esPass      | es_password      |
| 24    | esIndex     | es_index/es_type |
| 24    | esScrollNum | 156              |

| TBL_ID | CREATE_TIME         | DB_ID | TBL_NAME |
| ------ | ------------------- | ----- | -------- |
| 57     | 2018-11-06 10:44:51 | 24    | profile  |

| CD_ID | COMMENT | COLUMN_NAME | TYPE_NAME | INTEGER_IDX |
| ----- | ------- | ----------- | --------- | ----------- |
| 57    | comment | id          | int       | 1           |
| 57    | comment | name        | string    | 2           |
| 57    | comment | country     | string    | 3           |
| 57    | comment | gender      | string    | 4           |
| 57    | comment | operator    | string    | 5           |

#### MySQL

Sample Configuration：

| DB_ID | DESC             | NAME           | DB_TYPE |
| ----- | ---------------- | -------------- | ------- |
| 25    | mysql db message | mysql_database | mysql   |

| DB_ID | PARAM_KEY    | PARAM_VALUE                                |
| ----- | ------------ | ------------------------------------------ |
| 25    | jdbcDriver   | com.mysql.jdbc.Driver                      |
| 25    | jdbcUrl      | jdbc:mysql://localhost:3006/mysql_database |
| 25    | jdbcUser     | root                                       |
| 25    | jdbcPassword | root                                       |

| TBL_ID | CREATE_TIME         | DB_ID | TBL_NAME  |
| ------ | ------------------- | ----- | --------- |
| 58     | 2018-11-06 10:44:51 | 25    | test_date |

| CD_ID | COMMENT | COLUMN_NAME | TYPE_NAME | INTEGER_IDX |
| ----- | ------- | ----------- | --------- | ----------- |
| 58    | comment | id          | int       | 1           |
| 58    | comment | name        | string    | 2           |

## Contributing

We welcome contributions.

If you are intered in QSQL, you can download the source code from GitHub and execute the following maven comman at the project root directory：

```shell
mvn -DskipTests clean package
```

If you are planning to make a large contribution, talk to us first! It helps to agree on the general approach. Log a Issures on GitHub for your proposed feature.

Fork the GitHub repository, and create a branch for your feature.

Develop your feature and test cases, and make sure that `mvn install` succeeds. (Run extra tests if your change warrants it.)

Commit your change to your branch.

If your change had multiple commits, use `git rebase -i master` to squash them into a single commit, and to bring your code up to date with the latest on the main line.

Then push your commit(s) to GitHub, and create a pull request from your branch to the QSQL master branch. Update the JIRA case to reference your pull request, and a committer will review your changes.

The pull request may need to be updated (after its submission) for two main reasons:

1. you identified a problem after the submission of the pull request;
2. the reviewer requested further changes;

In order to update the pull request, you need to commit the changes in your branch and then push the commit(s) to GitHub. You are encouraged to use regular (non-rebased) commits on top of previously existing ones.

### Talks

**QQ Group: 932439028**

**WeChat Group: Posted in the QQ Group** [ P.S. Incorrect QQ Group number will raise a NPE :) ]
