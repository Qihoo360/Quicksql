[English](../../reference/build.md)|[中文](./build.md)

## 部署使用文档

[TOC]

## 集群环境部署

### 环境依赖

- Java >= 1.8
- Spark >= 2.2

### 部署流程

1. 下载并解压二进制包。下载地址：https://github.com/Qihoo360/Quicksql/releases

```shell
tar -zxvf ./qsql-release-bin.tar.gz
```

2. 进入conf目录，打开base-env.sh，设置环境变量。

- JAVA_HOME (务必保证版本 >= 1.8)
- SPARK_HOME (务必保证版本 >= 2.2)

3. 进入bin目录，执行run-example脚本测试环境。

```shell
./run-example com.qihoo.qsql.CsvJoinWithEsExample
```

如果可以查询出以下结果，则表示部署成功。

```sql
+------+-------+----------+--------+------+-------+------+
|deptno|   name|      city|province|digest|   type|stu_id|
+------+-------+----------+--------+------+-------+------+
|    40|Scholar|  BROCKTON|      MA| 59498|Scholar|  null|
|    45| Master|   CONCORD|      NH| 34035| Master|  null|
|    40|Scholar|FRAMINGHAM|      MA| 65046|Scholar|  null|
+------+-------+----------+--------+------+-------+------+
```

## 开始执行

在查询真实数据源前，需要将数据源相关的表、字段等元数据信息录入QSQL的元数据库。

### 元数据录入

QSQL支持通过脚本录入MySQL, Elasticsearch, Hive, Oracle和Kylin的元数据。

#### 功能介绍

执行脚本：/bin/meta-extract

接收参数：

-p:  数据源连接信息，连接配置详情见下方示例

-d:  数据源类型					[oracle, mysql, hive, es]

-r:  表名过滤条件，遵循LIKE语法	[%：全部匹配，_：占位匹配，?：可选匹配]

```json
//MySQL示例：
{
	"jdbcDriver": "com.mysql.jdbc.Driver",
	"jdbcUrl": "jdbc:mysql://localhost:3306/db",
	"jdbcUser": "user",
	"jdbcPassword": "pass"
}
//Oracle示例：
{
	"jdbcDriver": "oracle.jdbc.driver.OracleDriver",
	"jdbcUrl": "jdbc:oracle:thin:@localhost:1521/namespace",
	"jdbcUser": "user",
	"jdbcPassword": "pass" 
}
//Elasticsearch示例：
{
	"esNodes": "192.168.1.1",
	"esPort": "9000",
	"esUser": "user",
	"esPass": "pass",
	"esIndex": "index/type"
}
//Hive示例(当前支持元数据存在MySQL中的Hive元数据抽取)：
{
	"jdbcDriver": "com.mysql.jdbc.Driver",
	"jdbcUrl": "jdbc:mysql://localhost:3306/db",
	"jdbcUser": "user",
	"jdbcPassword": "pass",
	"dbName": "hive_db"
}
//Kylin示例
{
	"jdbcDriver": "org.apache.kylin.jdbc.Driver",
	"jdbcUrl": "jdbc:kylin://localhost:7070/learn_kylin",
	"jdbcUser": "ADMIN",
	"jdbcPassword": "KYLIN",
	"dbName": "default"
}
```

#### 使用示例

注意：linux中双引号"是特殊字符，传JSON参数时需要做转义。

**示例场景一 (MySQL)：**

1. 从MySQL中取出表名为my_table表的元数据并导入内嵌元数据库

``````shell
./meta-extract -p "{\"jdbcDriver\": \"com.mysql.jdbc.Driver\", \"jdbcUrl\": \"jdbc:mysql://localhost:3306/db\", \"jdbcUser\": \"user\",\"jdbcPassword\": \"pass\"}" -d "mysql" -r "my_table"
``````

2. 导入完成后，进行查询

``````shell
./qsql -e "SELECT * FROM my_table LIMIT 10"
``````

**示例场景二 (Elasticsearch)：**

1. 从Elasticsearch取出所有的type元数据并导入内嵌元数据库

`````shell
./meta-extract -p "{\"esNodes\": \"192.168.1.1\",\"esPort\": \"9090\",\"esUser\": \"user\",\"esPass\": \"pass\",\"esIndex\": \"index/type\"}" -d "es" -r "%"
`````

2. 导入完成后，进行查询

``````shell
./qsql -e "SELECT name, age FROM my_type WHERE age < 24 LIMIT 10"
``````

## 其他参数配置

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
