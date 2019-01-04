## 命令行

[TOC]

### 前言

命令行是使用Quicksql进行查询的方法之一。像Hive和MySql一样，您可以使用“hive / mysql -e 'select 1' ”之类的命令进行查询。要注意的是，我们暂时只实现了脚本命令的方式，像REPL这样的交互方式接下来逐步推出。

### 开始执行一个SQL查询

当你将Quicksql部署完成后，就可以开始使用qsql的命令啦~

例如，在Quicksql安装目录下，你可以使用如下命令进行查询：

```
./bin/qsql -e "SELECT 1"
```

`SELECT 1`是最简单的SQL，你会发现它可以运行在Hive、MySql等各个数据存储引擎上，所以我们用它来做Quicksql命令行执行的例子。下一步，你可以尝试指定一个执行引擎：

```
./bin/qsql -e "SELECT 1" --runner spark
```

执行上面的命令，你会发现自己其实启动了一个Spark程序执行SQL查询。`--runner` 可以用于选择不同的执行引擎， 例如JDBC。 如果想看到全部的配置及其含义，可以看这篇文章下面的[命令参数](#命令参数)

举个例子吧，下面的命令可以配置计算引擎的相关参数：

```
./bin/qsql -e "SELECT 1" --runner spark --driver_memory 2G --worker_memory 2G
```

这个例子的结果就是，你启动了一个拥有2G worker memory的和2G driver memory的Spark程序来执行你的查询。

除了上面的示例参数，你也可以使用`-h`或者`--help`来查看全部参数的解释：

```
./bin/qsql -h
```



当然，需要注意的是，你最好在使用命令查询数据前先将相关表的信息写入元数据，不然会报错哦。

### 命令参数

这里有一些命令行可配置的参数：

| Property Name   | Default     | Meaning                                       |
| --------------- | ----------- | --------------------------------------------- |
| -e              | --          | 配置查询的SQL语句，查询时必填。               |
| -h\|--help      | --          | 命令参数的详细描述                            |
| --runner        | dynamic     | 设置执行器类型,包括 dynamic, jdbc 或者 spark  |
| --master        | yarn-client | 设置引擎执行模式                              |
| --worker_memory | 1G          | 每一个worker的内存大小, 类似于Spark的executor |
| --driver_memory | 3G          | Driver端的内存大小, 类似于Spark的driver       |
| --worker_num    | 20          | worker的个数, 类似于Spark的executor           |

注意：

1. 你可以在/conf/base-env.sh 中设置runner、master、worker_memory、driver_memory 和 worker_num的默认值
2. 在非分布式执行中，即使你设置了master、worker_memory、driver_memory 和 worker_num，它们也不会生效。

### 元数据相关

Quicksql的默认元数据是存在Sqlite中的，你可以在Quicksql安装目录下的/metastore文件夹下看到它的数据库：**Schema.db**。当然，Quicksql支持你使用别的数据存储引擎作为元数据存储介质，我们也提供了工具来方便你这样做。

首先，你需要在Quicksql安装目录下的/conf/metadata.properties配置其他存储介质的连接信息，不然我们可不能连接上。

然后，执行下面的操作命令，这个命令可以帮助你使用MySql作为元数据存储。

```
./bin/metadata --dbType mysql --action init
```

你也可以使用下面的命令删除MySql中的元数据表。

```
./bin/metadata --dbType mysql --action delete
```



#### 命令参数

从上面的命令可以看出，元数据相关的命令参数只有两个：

| PropetyName | Default | Meaning                                    |
| ----------- | ------- | ------------------------------------------ |
| --dbType    |         | 元数据存储引擎的类型. 我们现在仅支持MySql. |
| --action    |         | 元数据表相关的操作, 包括 init 和 delete.   |