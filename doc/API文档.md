## API文档

### 前言

QSQL支持在其他应用中通过API执行查询，如果你有类似场景，请在项目中导入QSQL依赖。

``` java
<dependency>
		<groupId>com.qihoo.qsql</groupId>
        <artifactId>qsql-core</artifactId>
        <version>${project.version}</version>
</dependency>
```

### 示例用法

QSQL为用户提供了很简洁的SQL查询API，用户不需要关心SQL中FROM子句对应的查询引擎类型，只需要关注数据本身即可完成查询。

示例 1:  一个不需要任何配置的简单查询，直接将结果打印在终端

```java
//Create a dynamic SQL runner to execute SQL query.
SqlRunner runner = SqlRunner.builder().ok();
//Execute and run.
runner.sql("SELECT age, stu_name, dep_name "
     + "FROM student stu INNER JOIN department dep "
     + "on stu.dep_id = dep.id")
    .show()
    .run();
```

示例 2:  其他自定义配置的查询，查询结果存储在本地

```java
SqlRunner runner = SqlRunner.builder()
    .setTransformRunner(RunnerType.SPARK) //Choose Spark to execute query. 
    .setAppName("A Simple Example") //Set application name. default is current ts.
    .setAcceptedResultsNum(1000) //Set maxinum result set size, default is 1000.
    .setMaster("local[*]")	//Set execute mode, default is local[*].
    .setSchemaPath()  //Set path of schema in the form of file.
    .setProperties()  //Set properties of concrete engine.
    .ok();

runner.sql("SELECT * FROM student GROUP BY age")
    .asTextFile("/tmp/part") //Save results on hdfs or local filesystem.
    .run();
```

QSQL目前只支持以上API，后续随着功能迭代将开发更多可用的API。

### 启动程序

在项目中写完代码后，你应当将相关类打包，并将Jar包放在Linux服务器上，然后使用在程序中指定的Runner对应的任务提交方式提交QSQL的Jar包。例如，如果你使用Spark Runner，你可以像这样提交：

```
spark-submit 
```

注意：如果你没有设置Runner，请使用java -jar提交你的Jar包。（这句话有问题）