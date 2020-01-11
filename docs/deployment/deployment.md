[English](./deployment.md)|[中文](../zh/deployment/deployment.md)


# Deployment guide

**Introduction**
 
   This page will provide the matching version of the data source and engine supported by Quicksql and the dependency package adjustment method. 
   
In addition, the management method of metadata will also be mentioned.

## Data source management

   QuickSql provides the default data source client version. The following is the version relationship table. Users can replace the Jar package by themselves.

| Data storage  | Connection form          | Support engine| Client version | Remarks                                |
| ------------- | ------------------------ | --------------| -------------- | -------------------------------------- |
| Hive          | Read hive-site.xml / JDBC| Spark/Flink   | 1.2.1          | No need to read hive-site.xml          |
| MySQL         | JDBC                     | Spark/Flink   | 5.1.20         |                                        |
| Kylin         | JDBC                     | Spark         | 1.6.0          |                                        |
| Elasticsearch | JDBC                     | Spark         | 6.2.4          | Only ES6 version syntax is supported   |
| CSV           | JDBC                     | Spark         | NULL           |                                        |
| Oracle        | JDBC                     | Spark/Flink   | 11.2.0.3       |                                        |
| MongoDB       | NULL                     | NULL          | NULL           |                                        |
| Druid         | NULL                     | NULL          | NULL           |                                        |

Note: If the application needs to change the client version, please replace the corresponding driver package with the matching version in the /lib, /lib/spark directory. If you are using MySQL 8 services, you can do the following:

``````shell
$ cd /lib
$ rm -f mysql-connector-java-5.1.20.jar
$ cp mysql-connector-java-8.0.18.jar ./
$ ../bin/quicksql -e "SELECT * FROM TABLE_IN_MySQL8"
``````

## Computing Engine Management

The calculation engine parameters  can be customized by the user. The parameters can be modified in ./conf/qsql-runner.properties:

**Spark calculation engine parameters**

| Property Name                     | Default            | Meaning                                            |
| --------------------------------- | ------------------ | -------------------------------------------------- |
| spark.sql.hive.metastore.jars     | builtin            | Required jar packages of SparkSQL linking Hive     |
| spark.sql.hive.metastore.version  | 1.2.1              | Version Information of SparkSQL linking Hive       |
| spark.local.dir                   | /tmp               | Temporary file storage path during Spark execution |
| spark.driver.userClassPathFirst   | true               | User jar package loads first during Spark execution|
| spark.sql.broadcastTimeout        | 300                | Spark Broadcast Timeout                            |
| spark.sql.crossJoin.enabled       | true               | SparkSQL enables cross join                        |
| spark.speculation                 | true               | Spark starts task speculative execution               |
| spark.sql.files.maxPartitionBytes | 134217728（128MB） | Maximum number of bytes in a single partition when Spark reads a file |

**Flink calculation engine parameter**

| Property Name | Default | Meaning |
| ------------- | ------- | ------- |
| To be added   | To be added  | To be added  |

## Metadata Management

QuickSql uses an independent storage to store metadata and configuration information for various data sources. The default metadata storage is SqLite, a document database that can be used in ordinary development environments. If you have higher requirements for concurrency, you can change the metadata storage to MySQL, etc., using the general JDBC standard, you can refer to the following configuration:

### Metadata parameters

| Property Name               | Default                | Meaning                                                      |
| --------------------------- | ---------------------- | ------------------------------------------------------------ |
| meta.storage.mode           | intern                 | Metadata storage mode(Intern:read metadata stored in the built-in SqLite database; Extern:read metadata stored in an external database) |
| meta.intern.schema.dir      | ../metastore/schema.db | Path to the built-in database                                              |
| meta.extern.schema.driver   | （none）               | Driver for external database                                             |
| meta.extern.schema.url      | （none）               | Link to external database                                              |
| meta.extern.schema.user     | （none）               | Username for external database                                          |
| meta.extern.schema.password | （none）               | Password for external database                                             |
