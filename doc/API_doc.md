## API Document

### Overview 

QSQL supports called API directly by other applications to execute queries. If you want to do this, import  QSQL dependency into your project first.

``` java
<dependency>
		<groupId>com.qihoo.qsql</groupId>
        <artifactId>qsql-core</artifactId>
        <version>${project.version}</version>
</dependency>
```

### Examples

QSQL provides a simple API for users to query by SQL, users don't need to care about the storage which is defined in SQL from clause, only focus on data itself.

Example 1:  A sample without any configuration, only query and show results .

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

Example 2:  A little more custom configurations.

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

QSQL only supports these API currently, more API will be developed gradually.

### Submit Job

The following script templates are available when submitting API applications using spark-submit.

``````shell
#!/bin/bash

export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

. "${QSQL_HOME}/bin/load-qsql-env"
. "${QSQL_HOME}/bin/qsql-env"

for jar in `find ${QSQL_HOME}/lib -maxdepth 1 -name "*.jar"`
do
    if [ ! -n "${JARS}" ]
    then
        export JARS="${jar}"
    elif [[ ! ${jar} =~ "elasticsearch-spark" ]]
    then
        export JARS="${JARS},${jar}"
    fi
done

/spark2.2/bin/spark-submit --class com.qihoo.qsql.CsvScanExample --conf "spark.driver.userClassPathFirst=true" --conf
"spark.executor.extraClassPath=${QSQL_HOME}/lib/qsql-core-0.5.jar" --jars ${JARS} ${QSQL_HOME}/lib/qsql-core-0.5.jar
``````