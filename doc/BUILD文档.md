## 部署使用文档

[TOC]

## 集群环境部署

### 1 编译环境依赖

- java >= 1.8
- scala >= 2.11
- maven >= 3.3

### 2 编译步骤

在源码根目录下，执行：

```shell
mvn -DskipTests clean package
```

编译成功后执行：

```shell
ls ./target/
```

在./target/目录下，会生成发布包 qsql-0.5.tar.gz。

### 3 部署环境依赖

- CentOS 6.2
- java >= 1.8
- scala >= 2.11
- spark >= 2.2
- [可选] 目前QSQL支持的存储引擎MySQL、Elasticsearch、Hive、Druid

### 4 客户端部署

在客户端解压缩发布包 qsql-0.5.tar.gz 

```shell
tar -zxvf ./qsql-0.5.tar.gz
```

建立软链

```shell
ln -s qsql-0.5/ qsql
```

该发布包解压后的主要目录结构如下：

- bin：脚本目录
- conf：配置文件
- data：存放测试数据
- lib：依赖jar包
- metastore：元数据管理

在QSQL发布包$QSQL_HOME/conf目录中，分别配置如下文件：

- base-env.sh：设置相关环境变量，如：
  - JAVA_HOME
  - SPARK_HOME
  - QSQL_CLUSTER_URL
  - QSQL_HDFS_TMP
- qsql-runner.properties：设置系统参数
- log4j.properties：设置日志级别

## 开始执行

### QSQL Shell

```
./bin/qsql -e "select 1"
```

### 示例程序

QSQL附带了示例目录中的几个示例程序。要运行其中一个，使用./run-example <class> [params]。例如：

内存表数据：

```
./bin/run-example com.qihoo.qsql.CsvScanExample
```

Hive join MySQL：

```
./bin/run-example com.qihoo.qsql.CsvJoinWithEsExample
```

**注意**

```
./run-example <com.qihoo.qsql.CsvJoinWithEsExample>
```

运行混算，请确保当前客户端存在Spark、Hive、MySQL环境。并且将Hive与MySQL的连接信息添加到元数据管理中。

## 参数配置

### 环境变量

| Property Name              | Meaning                  |
| -------------------------- | ------------------------ |
| JAVA_HOME                  | Java的安装路径           |
| SPARK_HOME                 | Spark的安装路径          |
| QSQL_CLUSTER_URL           | Hadoop集群的路径         |
| QSQL_HDFS_TMP              | 设置临时目录路径         |
| QSQL_DEFAULT_WORKER_NUM    | 设置初始化的Worker数量   |
| QSQL_DEFAULT_WORKER_MEMORY | 设置每个Worker分配的内存 |
| QSQL_DEFAULT_DRIVER_MEMORY | 设置Driver端分配的内存   |
| QSQL_DEFAULT_MASTER        | 设置运行时的集群模式     |
| QSQL_DEFAULT_RUNNER        | 设置运行时的执行计划     |

### 参数配置

#### 应用程序参数

| Property Name                     | Default            | Meaning                             |
| --------------------------------- | ------------------ | ----------------------------------- |
| spark.sql.hive.metastore.jars     | builtin            | Spark Sql链接hive需要的jar包        |
| spark.sql.hive.metastore.version  | 1.2.1              | Spark Sql链接hive的版本信息         |
| spark.local.dir                   | /tmp               | Spark执行过程中的临时文件存放路径   |
| spark.driver.userClassPathFirst   | true               | Spark执行过程中，用户jar包优先加载  |
| spark.sql.broadcastTimeout        | 300                | Spark广播的超时时间                 |
| spark.sql.crossJoin.enabled       | true               | Spark Sql开启cross join             |
| spark.speculation                 | true               | Spark开启任务推测执行               |
| spark.sql.files.maxPartitionBytes | 134217728（128MB） | Spark读取文件时单个分区的最大字节数 |

#### 元数据参数

| Property Name               | Default                | Meaning                                                      |
| --------------------------- | ---------------------- | ------------------------------------------------------------ |
| meta.storage.mode           | intern                 | 元数据存储模式，intern：读取内置sqlite数据库中存储的元数据，extern：读取外部数据库中存储的元数据。 |
| meta.intern.schema.dir      | ../metastore/schema.db | 内置数据库的路径                                             |
| meta.extern.schema.driver   | （none）               | 外部数据库的驱动                                             |
| meta.extern.schema.url      | （none）               | 外部数据库的链接                                             |
| meta.extern.schema.user     | （none）               | 外部数据库的用户名                                           |
| meta.extern.schema.password | （none）               | 外部数据库的密码                                             |

## 元数据管理

### 表结构

#### DBS 

| 表字段  | 说明       | 示例数据         |
| ------- | ---------- | ---------------- |
| DB_ID   | 数据库ID   | 1                |
| DESC    | 数据库描述 | es 索引          |
| NAME    | 数据库名   | es_profile_index |
| DB_TYPE | 数据库类型 | es、hive、mysql  |

#### DATABASE_PARAMS

| 表字段      | 说明     | 示例数据 |
| ----------- | -------- | -------- |
| DB_ID       | 数据库ID | 1        |
| PARAM_KEY   | 参数名   | UserName |
| PARAM_VALUE | 参数值   | root     |

#### TBLS

| 表字段       | 说明     | 示例数据            |
| ------------ | -------- | ------------------- |
| TBL_ID       | 表ID     | 101                 |
| CREATED_TIME | 创建时间 | 2018-10-22 14:36:10 |
| DB_ID        | 数据库ID | 1                   |
| TBL_NAME     | 表名     | student             |

#### COLUMNS

| 表字段      | 说明       | 示例数据 |
| ----------- | ---------- | -------- |
| CD_ID       | 字段信息ID | 10101    |
| COMMENT     | 字段注释   | 学生姓名 |
| COLUMN_NAME | 字段名     | name     |
| TYPE_NAME   | 字段类型   | varchar  |
| INTEGER_IDX | 字段顺序   | 1        |

### 内置SQLite数据库

在QSQL发布包$QSQL_HOME/metastore目录中，存在如下文件：

- sqlite3：SQLite命令行工具
- schema.db：内置元数据数据库
- ./linux-x86/sqldiff：显示SQLite数据库之间的差异的命令行程序
- ./linux-x86/sqlite3_analyzer：用于测量和显示单个表和索引对SQLite数据库文件使用多少空间以及如何有效地使用空间

通过sqlite3连接到schema.db数据库，并操作元数据表

```shell
sqlite3 ../schema.db
```

### 外部MySQL数据库

修改内嵌的SQLite数据为MySQL数据库

```shell
vim metadata.properties
> meta.storage.mode=extern
> meta.extern.schema.driver    = com.mysql.jdbc.Driver
> meta.extern.schema.url       = jdbc:mysql://ip:port/db?useUnicode=true
> meta.extern.schema.user      = YourName
> meta.extern.schema.password  = YourPassword
```

初始化示例数据到MySQL数据库中

```shell
cd $QSQL_HOME/bin/
./metadata --dbType mysql --action init
```

### 配置元数据信息

#### Hive

示例配置：

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

示例配置：

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

示例配置：

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