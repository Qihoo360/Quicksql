[English](./getting-started.md)|[中文](../zh/reference/getting-started.md)

# Tutorials

包含启动项目和运行范例的流程引导

## Setup on Linux\MacOS

在Linux\MacOS上 运行Quicksql非常简单，但需要确保环境预置完整，依赖的环境有：

· Java >= 1.8

· Spark >= 2.2 (必选，未来作为可选)

· Flink >= 1.9 (可选)

1. 下载并解压二进制安装包，下载地址：<https://github.com/Qihoo360/Quicksql/releases>；
2. 进入conf目录，在quicksql-env.sh中配置环境变量；

``````shell
$ tar -zxvf ./quicksql-release-bin.tar.gz
$ cd quicksql-realease-0.7.0
$ vim ./conf/quicksql-env.sh #Set Your Basic Environment.
``````

## 运行样例查询

进入bin目录，执行quicksql-example脚本。（这里使用了内嵌Elasticsearch Server与Csv数据源作一个关联过滤）

``````shell
$ ./bin/quicksql-example com.qihoo.qsql.CsvJoinWithEsExample #换成选项型，并能打印SQL语句
``````

如果能够显示以下结果，说明环境构建完毕，可以尝试新的操作。

```sql
+------+-------+----------+--------+------+-------+------+
|deptno|   name|      city|province|digest|   type|stu_id|
+------+-------+----------+--------+------+-------+------+
|    40|Scholar|  BROCKTON|      MA| 59498|Scholar|  null|
|    45| Master|   CONCORD|      NH| 34035| Master|  null|
|    40|Scholar|FRAMINGHAM|      MA| 65046|Scholar|  null|
+------+-------+----------+--------+------+-------+------+
```

## 运行真实查询

在Quicksql上运行查询前需要将连接信息以及表、字段信息采集入库。

默认元数据库使用Sqlite，切换元数据库的方式参考部署指南，Sqlite可以使用以下方式访问：

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

当然，我们并不需要手工去插入元数据！

Quicksql提供了众多标准数据源的采集脚本，通过脚本批量拉取元数据。

目前支持通过脚本录入元数据的数据源有**Hive, MySQL, Kylin, Elasticsearch, Oracle, MongoDB**。

执行方式如下（注意：-r 参数可以使用LIKE语法，['%': 全部匹配，'_': 占位匹配，'?': 可选匹配]）

``````shell
$ ./bin/metadata-extract -p "<SCHEMA-JSON>" -d "<DATA-SOURCE>" -r "<TABLE-NAME-REGEX>"
``````

（详细的SCHEMA-JSON格式参考页末）

**使用示例**

从**MySQL**数据库中采集元数据

``````shell
$ ./meta-extract -p "{\"jdbcDriver\": \"com.mysql.jdbc.Driver\", \"jdbcUrl\": \"jdbc:mysql://localhost:3306/db\", \"jdbcUser\": \"user\",\"jdbcPassword\": \"pass\"}" -d "mysql" -r "my_table"
``````

从**Elasticsearch**存储中采集元数据

``````shell
$ ./meta-extract -p "{\"esNodes\": \"192.168.1.1\",\"esPort\": \"9090\",\"esUser\": \"user\",\"esPass\": \"pass\",\"esIndex\": \"index/type\"}" -d "es" -r "%"
``````

从**Mongodb**存储中采集元数据

``````shell
$ ./meta-extract -p "{\"host\": \"192.168.1.1\", \"port\": \"27017\", \"authMechanism\": \"SCRAM-SHA-1\",\"userName\": \"admin\",\"password\": \"admin\",\"dataBaseName\": \"test\",\"collectionName\":\"products\"}" -d "mongo" -r "products"
``````

采集成功后将返回

```shell
1970-01-01 15:09:43,119 [main] INFO  - Connecting server.....
1970-01-01 15:09:44,000 [main] INFO  - Connected successfully!!
1970-01-01 15:09:44,121 [main] INFO  - Successfully collected metadata for 2 tables!!
1970-01-01 15:09:45,622 [main] INFO  - [my_table, my_type]!!
```

**连接信息**

常见数据源采集的JSON结构如下

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
##Mongodb
{
	"host": "192.168.1.1",
	"port": "27017",
	"dataBaseName": "test",
	"authMechanism": "SCRAM-SHA-1",
	"userName": "admin",
	"password": "admin",
	"collectionName": "products"
}
``````

注意：Shell中双引号是特殊字符，传JSON参数时需要做转义！！

### 从命令行提交查询

从命令行查询是Quicksql提供的最基本的查询方式之一。

像Hive和MySQL一样，使用`quicksql.sh -e "YOUR SQL"`就可以完成查询，结果集将打印在终端上。

**使用示例**

1. 一个简单的查询，将在Quicksql内核中被执行；

``````shell
$ ./bin/quicksql.sh -e "SELECT 1"
``````

想让它跑在Spark或Flink计算引擎上？可以使用runner参数；

``````shell
$ ./bin/quicksql.sh -e "SELECT 1" --runner spark|flink
``````

2. 一个Elasticsearch数据源查询，将由Quicksql建立RestClient连接执行；

``````shell
$ ./bin/quicksql.sh -e "SELECT approx_count_distinct(city), state FROM geo_mapping GROUP BY state LIMIT 10"
``````

想让计算结果落地到存储？可以尝试INSERT INTO语法：

``````shell
$ ./bin/quicksql.sh -e  "INSERT INTO \`hdfs://cluster:9000/hello/world\` IN HDFS SELECT approx_count_distinct(city), state FROM geo_mapping GROUP BY state LIMIT 10"
``````

**其他参数**

以上实例提供了基本的查询方式，如果对计算引擎需要指定其他参数可以参考下表：

| Property Name   | Default     | Meaning                                         |
| --------------- | ----------- | ----------------------------------------------- |
| -e              | --          | 配置查询的SQL语句，查询时必填。                 |
| -h\|--help      | --          | 命令参数的详细描述                              |
| --runner        | dynamic     | 设置执行器类型,包括 dynamic, jdbc, spark, flink |
| --master        | yarn-client | 设置引擎执行模式                                |
| --worker_memory | 1G          | 执行器的内存大小配置                            |
| --driver_memory | 3G          | 控制器的内存大小配置                            |
| --worker_num    | 20          | 执行器的并行度                                  |

注意：

​	(1) 在quicksql-env.sh 中可以设置runner、master、worker_memory等参数的默认值；

​	(2) 在非分布式执行中，即使设置了master、worker_memory等参数也不会生效；

### 从应用提交查询

Quicksql支持使用Client/Server模式的JDBC连接进行查询，用户的应用可以通过引入Driver包与Server建立连接进行联邦查询。

## Server端

**启动Server**

``````shell
$ ./bin/quicksql-server.sh start -P 5888 -R spark -M yarn-client
``````

启动参数包括start|stop|restart|status，-P/-R/-M为可选项，分别对应端口号，执行引擎和任务调度方式，

-P：指定server端口号，默认为5888

-R：指定执行引擎，支持Spark/Flink

-M：指定spark任务资源调度方式，yarn-client或yarn-cluster等，默认为local[1]

## Client端

**应用接入**

项目poml文件引入quicksql-client和 avatica 依赖包

```
<dependency>
    <groupId>com.qihoo.qsql</groupId>
    <artifactId>qsql</artifactId>
    <version>0.6</version>
</dependency>
<dependency>
    <groupId>org.apache.calcite.avatica</groupId>
    <artifactId>avatica-server</artifactId>
    <version>1.12.0</version>
</dependency>
```

Java代码示例：

```java
 public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("com.qihoo.qsql.client.Driver"); //注入Drvier

        Properties properties = new Properties();
        properties.setProperty("runner","jdbc");
        String url = "jdbc:quicksql:url=http://localhost:5888";
        Connection connection = DriverManager.getConnection(url,properties);
        Statement pS = connection.createStatement();
        String sql = "select * from (values ('a', 1), ('b', 2))";
        ResultSet rs =  pS.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
        }
        rs.close();
        pS.close();
}
```

1. 注入quicksql Driver ：com.qihoo.qsql.client.Driver

2. properties 配置项包含参数

   ​    runner：指定执行引擎， 包括 dynamic, jdbc, spark, flink

   ​	acceptedResultsNum ： 执行查询返回数据的最大条数   

   ​    appName：启动的spark/flink实例名

3. 连接server的url :  jdbc:quicksql:url=http://  +  server服务器域名或ip地址 + server启动端口号（在server的日志文件 里有url信息）

4. 其他操作与普通jdbc查询相同，包括Connection， Statement，ResultSet，ResultSetMetaData等类的操作，以及结果的遍历。
