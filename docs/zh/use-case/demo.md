# QuickSQL文档
​
## QuickSQL概述\Home
​
Quicksql是一款跨计算引擎的统一联邦查询中间件，用户可以使用标准SQL语法对各类数据源进行联合分析查询。其目标是构建实时\离线全数据源统一的数据处理范式，屏蔽底层物理存储和计算层，最大化业务处理数据的效率。同时能够提供给开发人员可插拔接口，由开发人员自行对接新数据源。
​
目前Quicksql已经接入的数据源有**Hive, MySQL, Kylin, Elasticsearch, Oracle, MongoDB, CSV**；
​
## 应用场景\Use Cases
​
(引言)包含Quicksql目前成功实践的工程案例和正在尝试的方向
​
​
​
Quicksql主要面向业务应用呈现多元化，数据处理分析方式多样的场景。用户原本需要熟悉各种数据源的操作方式及内部细节，Quicksql可以屏蔽这些数据源差异，利用这个特性，Quicksql可以帮助我们构建统一数据查询和处理应用，典型场景有：
​
#### SQL交互式分析
​
面对互相隔离的数据源，用户使用标准SQL进行数据查询，由Quicksql进行查询逻辑的解析、优化及运行时引擎的选取，缩减数据人员的开发周期，提升数据计算的性能。
​
[图1]
​
#### 结构化数据ETL
​
替代传统的代码开发式或图元配置化ETL方式，通过一条SQL完成各类数据源的数据抽取、转换和加载，调试工作围绕SQL进行，大幅提升ETL工程师的开发效率。
​
[图2]
​
#### 流批计算多维关联(进行中)
​
打破实时流和离线数据源的屏障，使用标准SQL关联流式数据和维度表，进行复杂场景数据分析；
​
[图3]
​
（那些场景可以使用，具备什么样的优势，如何支持业务的；典型应用是什么，给出适合的应用场景，最好能有动态图展示demo）
​
## Getting Started
​
(引言)Quicksql提供两种方式让用户接入并使用项目，包括命令行形式和JDBC形式
​
### Tutorials
​
包含启动项目和运行范例的流程引导
​
#### Setup on Linux\MacOS
​
在Linux\MacOS上运行Quicksql非常简单，但需要确保环境预置完整，依赖的环境有：
​
· Java >= 1.8
​
· Spark >= 2.2 (必选，未来作为可选)
​
· Flink >= 1.9 (可选)
​
1. 下载并解压二进制安装包，下载地址：<https://github.com/Qihoo360/Quicksql/releases>；
2. 进入conf目录，在quicksql-env.sh中配置环境变量；
​
``````shell
$ tar -zxvf ./quicksql-release-bin.tar.gz
$ cd quicksql-realease-0.7.0
$ vim ./conf/quicksql-env.sh #Set Your Basic Environment.
``````
​
##### 运行样例查询
​
进入bin目录，执行quicksql-example脚本。（这里使用了内嵌Elasticsearch Server与Csv数据源作一个关联过滤）
​
``````shell
$ ./bin/quicksql-example com.qihoo.qsql.CsvJoinWithEsExample #换成选项型，并能打印SQL语句
``````
​
如果能够显示以下结果，说明环境构建完毕，可以尝试新的操作。
​
```sql
+------+-------+----------+--------+------+-------+------+
|deptno|   name|      city|province|digest|   type|stu_id|
+------+-------+----------+--------+------+-------+------+
|    40|Scholar|  BROCKTON|      MA| 59498|Scholar|  null|
|    45| Master|   CONCORD|      NH| 34035| Master|  null|
|    40|Scholar|FRAMINGHAM|      MA| 65046|Scholar|  null|
+------+-------+----------+--------+------+-------+------+
```
​
##### 运行真实查询
​
在Quicksql上运行查询前需要将连接信息以及表、字段信息采集入库。
​
默认元数据库使用Sqlite，切换元数据库的方式参考部署指南，Sqlite可以使用以下方式访问：
​
``````shell
$ cd ./metastore/linux-x86/
$ sqlite3 ../schema.db
SQLite version 3.6.20
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> .tables
COLUMNS    DATABASE_PARAMS    DBS    TBLS  
sqlite> SELECT TBLS.DB_ID, TBL_NAME, NAME  FROM TBLS INNER JOIN DBS ON TBLS.DB_ID = DBS.DB_ID;
+------+---------------+-----------+
| DB_ID|  	   TBL_NAME|    DB_NAME|
+------+---------------+-----------+
|     1|    call_center|   BROCKTON|
|     2|   catalog_page|    CONCORD|
|     3|  catalog_sales| FRAMINGHAM|
+------+---------------+-----------+
``````
​
当然，我们并不需要手工去插入元数据！
​
Quicksql提供了众多标准数据源的采集脚本，通过脚本批量拉取元数据。
​
目前支持通过脚本录入元数据的数据源有**Hive, MySQL, Kylin, Elasticsearch, Oracle, MongoDB**。
​
执行方式如下（注意：-r 参数可以使用LIKE语法，['%': 全部匹配，'_': 占位匹配，'?': 可选匹配]）
​
``````shell
$ ./bin/metadata-extract -p "<SCHEMA-JSON>" -d "<DATA-SOURCE>" -r "<TABLE-NAME-REGEX>"
``````
​
（详细的SCHEMA-JSON格式参考页末）
​
**使用示例**
​
从**MySQL**数据库中采集元数据
​
``````shell
$ ./meta-extract -p "{\"jdbcDriver\": \"com.mysql.jdbc.Driver\", \"jdbcUrl\": \"jdbc:mysql://localhost:3306/db\", \"jdbcUser\": \"user\",\"jdbcPassword\": \"pass\"}" -d "mysql" -r "my_table"
``````
​
从**Elasticsearch**存储中采集元数据
​
``````shell
$ ./meta-extract -p "{\"esNodes\": \"192.168.1.1\",\"esPort\": \"9090\",\"esUser\": \"user\",\"esPass\": \"pass\",\"esIndex\": \"index/type\"}" -d "es" -r "%"
``````
​
采集成功后将返回
​
```shell
1970-01-01 15:09:43,119 [main] INFO  - Connecting server.....
1970-01-01 15:09:44,000 [main] INFO  - Connected successfully!!
1970-01-01 15:09:44,121 [main] INFO  - Successfully collected metadata for 2 tables!!
1970-01-01 15:09:45,622 [main] INFO  - [my_table, my_type]!!
```
​
**连接信息**
​
常见数据源采集的JSON结构如下
​
``````shell
##MySQL
{
	"jdbcDriver": "com.mysql.jdbc.Driver",
	"jdbcUrl": "jdbc:mysql://localhost:3306/db",
	"jdbcUser": "USER",
	"jdbcPassword": "PASSWORD"
}
##Oracle
{
	"jdbcDriver": "oracle.jdbc.driver.OracleDriver",
	"jdbcUrl": "jdbc:oracle:thin:@localhost:1521/namespace",
	"jdbcUser": "USER",
	"jdbcPassword": "PASSWORD" 
}
##Elasticsearch
{
	"esNodes": "192.168.1.1",
	"esPort": "9000",
	"esUser": "USER",
	"esPass": "PASSWORD",
	"esIndex": "index/type"
}
##Hive(Hive元数据存在MySQL中)
{
	"jdbcDriver": "com.mysql.jdbc.Driver",
	"jdbcUrl": "jdbc:mysql://localhost:3306/db",
	"jdbcUser": "USER",
	"jdbcPassword": "PASSWORD",
	"dbName": "hive_db"
}
##Hive-Jdbc(Hive元数据通过Jdbc访问 )
{
	"jdbcDriver": "org.apache.hive.jdbc.HiveDriver",
	"jdbcUrl": "jdbc:hive2://localhost:7070/learn_kylin",
	"jdbcUser": "USER",
	"jdbcPassword": "PASSWORD",
	"dbName": "default"
}
##Kylin
{
	"jdbcDriver": "org.apache.kylin.jdbc.Driver",
	"jdbcUrl": "jdbc:kylin://localhost:7070/learn_kylin",
	"jdbcUser": "ADMIN",
	"jdbcPassword": "KYLIN",
	"dbName": "default"
}
``````
​
注意：Shell中双引号是特殊字符，传JSON参数时需要做转义！！
​
##### 第二页 从命令行提交查询
​
从命令行查询是Quicksql提供的最基本的查询方式之一。
​
像Hive和MySQL一样，使用`quicksql.sh -e "YOUR SQL"`就可以完成查询，结果集将打印在终端上。
​
**使用示例**
​
1. 一个简单的查询，将在Quicksql内核中被执行；
​
``````shell
$ ./bin/quicksql.sh -e "SELECT 1"
``````
​
想让它跑在Spark或Flink计算引擎上？可以使用runner参数；
​
``````shell
$ ./bin/quicksql.sh -e "SELECT 1" --runner spark|flink
``````
​
2. 一个Elasticsearch数据源查询，将由Quicksql建立RestClient连接执行；
​
``````shell
$ ./bin/quicksql.sh -e "SELECT approx_count_distinct(city), state FROM geo_mapping GROUP BY state LIMIT 10"
``````
​
想让计算结果落地到存储？可以尝试INSERT INTO语法：
​
``````shell
$ ./bin/quicksql.sh -e  "INSERT INTO \`hdfs://cluster:9000/hello/world\` IN HDFS SELECT approx_count_distinct(city), state FROM geo_mapping GROUP BY state LIMIT 10"
``````
​
**其他参数**
​
以上实例提供了基本的查询方式，如果对计算引擎需要指定其他参数可以参考下表：
​
| Property Name   | Default     | Meaning                                         |
| --------------- | ----------- | ----------------------------------------------- |
| -e              | --          | 配置查询的SQL语句，查询时必填。                 |
| -h\|--help      | --          | 命令参数的详细描述                              |
| --runner        | dynamic     | 设置执行器类型,包括 dynamic, jdbc, spark, flink |
| --master        | yarn-client | 设置引擎执行模式                                |
| --worker_memory | 1G          | 执行器的内存大小配置                            |
| --driver_memory | 3G          | 控制器的内存大小配置                            |
| --worker_num    | 20          | 执行器的并行度                                  |
​
注意：
​
​	(1) 在quicksql-env.sh 中可以设置runner、master、worker_memory等参数的默认值；
​
​	(2) 在非分布式执行中，即使设置了master、worker_memory等参数也不会生效；
​
##### 第三页 从应用提交查询
​
Quicksql支持使用Client/Server模式的JDBC连接进行查询，用户的应用可以通过引入Driver包与Server建立连接进行联邦查询。
​
**启动Server**
​
``````shell
$ ./bin/quicksql-server.sh start 
``````
​
**应用接入**
​
### Examples(Siyuan liu、Alexander Tan)
​
各类应用入级别的例子（语法、函数、混合查询）
​
### Quicksql on docker(Francis)
​
在docker下的使用指导
​
## 部署环境
​
(引言)本页面将提供Quicksql支持的数据源和引擎的配套版本和依赖包调整方式
​
​
​
| 数据存储      | 连接形式                 | 支持引擎    | 客户端版本 |
| ------------- | ------------------------ | ----------- | ---------- |
| Hive          | 读取hive-site.xml / JDBC | Spark/Flink | 1.2.1      |
| MySQL         | JDBC                     | Spark/Flink |            |
| Kylin         | JDBC                     | Spark       |            |
| Elasticsearch | JDBC                     | Spark       |            |
| CSV           | JDBC                     | Spark       |            |
| Oracle        | JDBC                     | Spark/Flink |            |
| MongoDB       |                          | NULL        |            |
​
JDBC
​
​	MySQL
​
​	Kylin
​
​	Oracle
​
​	Elasticsearch
​
​	MongoDB
​
​	CSV
​
Hive,
​
​
​
计算引擎
​
Spark
​
​
​
Flink
​
（这里面应该给出配套的版本号，兼容的版本，例如：mysql 版本、es版本、docker版本等）
​
考虑linux、windows、macos、docker等下的安装部署，附带简单实例。
​
 每种引擎支持的数据源
​
## QuickSQL开发(Alexander Tan)
​
（主要包含用户怎么样导入源码，怎样构建项目并开发）
​
语法文档
​
## OTHERS
​
### FAQ(Siyuan Liu)
​
General 
​
(1)…?
​
 
​
(2) …?
​
### Blog（Alexander Tan）
​
（主要记录发版本历史，重点功能介绍）
​
### Getting Help (Alexander Tan)
​
（邮件或者stack overflow，列出可以提供帮助的负责人邮箱地址）
​
### How to Contribute（Siyuan liu、Alexander Tan）
​
(主要讲述别人该怎么贡献代码及其他贡献方式，怎么样能成为contributor等)
​
### QuickSQL on GitHub
​
出一个jdbc demo