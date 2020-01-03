[English](../../deployment/deployment.md)|[中文](./deployment.md)


## 部署指南

(引言)本页面将提供Quicksql支持的数据源和引擎的配套版本和依赖包调整方式，此外，元数据的管理方式也将被提及。

### 数据源管理

Quicksql提供默认的数据源客户端版本，以下是版本关系表，用户可以自行替换Jar包更替。

| 数据存储      | 连接形式                 | 支持引擎    | 客户端版本 | 备注                    |
| ------------- | ------------------------ | ----------- | ---------- | ----------------------- |
| Hive          | 读取hive-site.xml / JDBC | Spark/Flink | 1.2.1      | 读取hive-site.xml不需要 |
| MySQL         | JDBC                     | Spark/Flink | 5.1.20     |                         |
| Kylin         | JDBC                     | Spark       | 1.6.0      |                         |
| Elasticsearch | JDBC                     | Spark       | 6.2.4      | 仅支持ES6版本语法       |
| CSV           | JDBC                     | Spark       | 无         |                         |
| Oracle        | JDBC                     | Spark/Flink | 11.2.0.3   |                         |
| MongoDB       | NULL                     | NULL        | NULL       |                         |
| Druid         | NULL                     | NULL        | NULL       |                         |

注：应用如需更换客户端版本请在/lib, /lib/spark目录下将相应的driver包替换为匹配的版本，如默认MySQL的Driver使用mysql-connector-java-5.1.20.jar，应用如使用的时MySQL 8的服务，可执行以下操作：

``````shell
$ cd /lib
$ rm -f mysql-connector-java-5.1.20.jar
$ cp mysql-connector-java-8.0.18.jar ./
$ ../bin/quicksql -e "SELECT * FROM TABLE_IN_MySQL8"
``````

### 计算引擎管理

Quicksql使用的计算引擎可以由用户自定义参数，参数可以在./conf/qsql-runner.properties中修改：

**Spark计算引擎参数**

| Property Name                     | Default            | Meaning                             |
| --------------------------------- | ------------------ | ----------------------------------- |
| spark.sql.hive.metastore.jars     | builtin            | SparkSQL链接Hive需要的jar包         |
| spark.sql.hive.metastore.version  | 1.2.1              | SparkSQL链接Hive的版本信息          |
| spark.local.dir                   | /tmp               | Spark执行过程中的临时文件存放路径   |
| spark.driver.userClassPathFirst   | true               | Spark执行过程中，用户jar包优先加载  |
| spark.sql.broadcastTimeout        | 300                | Spark广播的超时时间                 |
| spark.sql.crossJoin.enabled       | true               | SparkSQL开启cross join              |
| spark.speculation                 | true               | Spark开启任务推测执行               |
| spark.sql.files.maxPartitionBytes | 134217728（128MB） | Spark读取文件时单个分区的最大字节数 |

**Flink计算引擎参数**

| Property Name | Default | Meaning |
| ------------- | ------- | ------- |
| 待补充        | 待补充  | 待补充  |

### 元数据管理

Quicksql使用一个独立的存储来存放各类数据源的元数据及配置信息，默认的元数据存储为Sqlite，一个文档数据库，在普通开发环境下可以使用。如果对并发能力有更高的要求，可以更换元数据存储为MySQL等，采用通用的JDBC标准，可参考以下配置：

#### 元数据参数

| Property Name               | Default                | Meaning                                                      |
| --------------------------- | ---------------------- | ------------------------------------------------------------ |
| meta.storage.mode           | intern                 | 元数据存储模式，intern：读取内置sqlite数据库中存储的元数据，extern：读取外部数据库中存储的元数据。 |
| meta.intern.schema.dir      | ../metastore/schema.db | 内置数据库的路径                                             |
| meta.extern.schema.driver   | （none）               | 外部数据库的驱动                                             |
| meta.extern.schema.url      | （none）               | 外部数据库的链接                                             |
| meta.extern.schema.user     | （none）               | 外部数据库的用户名                                           |
| meta.extern.schema.password | （none）               | 外部数据库的密码                                             |
