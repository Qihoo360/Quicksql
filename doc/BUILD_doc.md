## Building Tutorial

[TOC]

### Requirements

- Java >= 1.8
- Spark >= 2.2

### Deployment

1. Download then decompress binary package. Download path: https://github.com/Qihoo360/Quicksql/releases

```shell
tar -zxvf ./qsql-release-bin.tar.gz
```

2. Go to the '/conf' , open `base-env.sh`, and set the environment variables.

- JAVA_HOME (REQUIRED VERSION >= 1.8)
- SPARK_HOME (REQUIRED VERSION >= 2.2)

3. Go to the '/bin', run the `run-example` script to test environment.

```shell
./run-example com.qihoo.qsql.CsvJoinWithEsExample
```

	If you can query the following results, the deployment is successful.

```sql
+------+-------+----------+--------+------+-------+------+
|deptno|   name|      city|province|digest|   type|stu_id|
+------+-------+----------+--------+------+-------+------+
|    40|Scholar|  BROCKTON|      MA| 59498|Scholar|  null|
|    45| Master|   CONCORD|      NH| 34035| Master|  null|
|    40|Scholar|FRAMINGHAM|      MA| 65046|Scholar|  null|
+------+-------+----------+--------+------+-------+------+
```

## Getting Started

Before querying the real data source, you need to put metadata information such as tables and fields into the QSQL metastore.

### Metadata Extraction

QSQL supports extracting metadata from MySQL, Elasticsearch, Hive and Oracle through scripts.

#### Basic Usage

Script Position：$QSQL_HOME/bin/meta-extract

Accepted Parameters：

-p:  data source connection information, connection configuration details see the examples below

-d:  data source type								[oracle, mysql, hive, es]

-r:  Table name filter condition, following LIKE syntax	[%，_，?]

```json
//MySQL Example
{
	"jdbcDriver": "com.mysql.jdbc.Driver",
	"jdbcUrl": "jdbc:mysql://localhost:3306/db",
	"jdbcUser": "user",
	"jdbcPassword": "pass"
}
//Oracle Example
{
	"jdbcDriver": "oracle.jdbc.driver.OracleDriver",
	"jdbcUrl": "jdbc:oracle:thin:@localhost:1521/namespace",
	"jdbcUser": "user",
	"jdbcPassword": "pass" 
}
//Elasticsearch Example
{
	"esNodes": "192.168.1.1",
	"esPort": "9000",
	"esUser": "user",
	"esPass": "pass",
	"esIndex": "index/type"
}
//Hive Example
{
	"jdbcDriver": "com.mysql.jdbc.Driver",
	"jdbcUrl": "jdbc:mysql://localhost:3306/db",
	"jdbcUser": "user",
	"jdbcPassword": "pass",
	"dbName": "hive_db"
}
```

#### Use Example

Note: Double quotes in linux are special characters, which need to be escaped when passing JSON parameters.

Sample 1 (MySQL)：

1. Extract the metadata of the table named my_table table from MySQL and import it into the embedded metabase.

```shell
./meta-extract -p "{\"jdbcDriver\": \"com.mysql.jdbc.Driver\", \"jdbcUrl\": \"jdbc:mysql://localhost:3306/db\", \"jdbcUser\": \"user\",\"jdbcPassword\": \"pass\"}" -d "mysql" -r "my_table"
```

2. After the import is complete, then query.

```shell
./qsql -e "SELECT * FROM my_table LIMIT 10"
```

Sample 2 (Elasticsearch)：

1. Extract all type metadata from Elasticsearch and import it into the embedded metabase

   ```shell
   ./meta-extract -p "{\"esNodes\": \"192.168.1.1\",\"esPort\": \"9090\",\"esUser\": \"user\",\"esPass\": \"pass\",\"esIndex\": \"index/type\"}" -d "es" -r "%"
   ```

2. After the import is complete, then query.

```shell
./qsql -e "SELECT name, age FROM my_type WHERE age < 24 LIMIT 10"
```

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
